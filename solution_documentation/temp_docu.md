```sql
CREATE TABLE staffingdata_raw (
    cfn_person_id INTEGER,
    week_begin DATE,
    employee_name STRING,
    email STRING,
    direct_supervisor_name STRING,
    employee_location STRING,
    job_title STRING,
    employee_status STRING,
    contractor STRING,
    financial_business_unit STRING,
    financial_department STRING
);

CREATE TABLE staffingdata (
    -- Keys
    employee_key INTEGER,
    cfn_person_id INTEGER,
    week_begin DATE,
    date_end DATE,
    -- Employee info
    employee_name STRING,
    email STRING,
    -- Supervisor info
    direct_supervisor_name STRING,
    direct_supervisor_id INTEGER,
    -- Role info
    job_title STRING,
    employee_status STRING,
    contractor STRING,
    -- Organization
    employee_location STRING,
    financial_business_unit STRING,
    financial_department STRING,
    -- Hierarchy
    employee_path STRING,
    -- Audit
    insert_date DATETIME
);

-- Sequence for generating employee_key
CREATE SEQUENCE IF NOT EXISTS seq_employee_key START = 1 INCREMENT = 1;
```

---

## Load Procedure

```sql
CREATE OR REPLACE PROCEDURE usp_load_staffingdata()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    validation_result VARIANT;
BEGIN

    -- =============================================================
    -- Step 0: Validate raw data before loading
    -- =============================================================
    CALL sp_validate_staffingdata_raw() INTO validation_result;

    IF (validation_result:error_count > 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'Load aborted',
            'reason', 'Data quality issues found',
            'error_count', validation_result:error_count,
            'validation_details', validation_result
        )::VARCHAR;
    END IF;

    BEGIN TRANSACTION;

    -- =============================================================
    -- Step 1: MERGE - Upsert from raw into destination
    -- Key: (week_begin, cfn_person_id)
    -- Note: date_end set to placeholder, will be recalculated in Step 2
    -- =============================================================

    MERGE INTO staffingdata AS tgt
    USING (
        -- Resolve direct_supervisor_name to direct_supervisor_id via self-join
        WITH RECURSIVE raw_with_supervisor_id AS (
            SELECT
                r.cfn_person_id,
                r.week_begin,
                r.employee_name,
                r.email,
                r.direct_supervisor_name,
                sup.cfn_person_id AS direct_supervisor_id,
                r.job_title,
                r.employee_status,
                r.contractor,
                r.employee_location,
                r.financial_business_unit,
                r.financial_department
            FROM staffingdata_raw r
            LEFT JOIN staffingdata_raw sup
                ON LOWER(TRIM(r.direct_supervisor_name)) = LOWER(TRIM(sup.employee_name))
                AND r.week_begin = sup.week_begin
        ),

        -- Build employee_path using recursive CTE (format: own_id|supervisor_id|supervisor's_supervisor_id|...)
        supervisor_chain AS (
            -- Anchor: Start with each employee's own ID
            SELECT
                cfn_person_id,
                week_begin,
                direct_supervisor_id AS current_supervisor_id,
                CAST(cfn_person_id AS VARCHAR) AS employee_path,
                1 AS lvl
            FROM raw_with_supervisor_id

            UNION ALL

            -- Recursive: Walk up the chain - append supervisor IDs
            SELECT
                sc.cfn_person_id,
                sc.week_begin,
                r.direct_supervisor_id AS current_supervisor_id,
                sc.employee_path || '|' || CAST(r.cfn_person_id AS VARCHAR) AS employee_path,
                sc.lvl + 1
            FROM supervisor_chain sc
            INNER JOIN raw_with_supervisor_id r
                ON sc.current_supervisor_id = r.cfn_person_id
                AND sc.week_begin = r.week_begin
            WHERE sc.current_supervisor_id IS NOT NULL
              AND sc.lvl < 20  -- Safety limit to prevent infinite loops
        ),

        -- Get the deepest path per employee/week_begin (final complete path)
        final_paths AS (
            SELECT
                cfn_person_id,
                week_begin,
                employee_path
            FROM supervisor_chain
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cfn_person_id, week_begin
                ORDER BY lvl DESC
            ) = 1
        ),

        -- Join raw data with computed paths
        source_data AS (
            SELECT
                r.cfn_person_id,
                r.week_begin,
                r.employee_name,
                r.email,
                r.direct_supervisor_name,
                r.direct_supervisor_id,
                r.job_title,
                r.employee_status,
                r.contractor,
                r.employee_location,
                r.financial_business_unit,
                r.financial_department,
                COALESCE(fp.employee_path, '') AS employee_path,
                CURRENT_TIMESTAMP() AS insert_date
            FROM raw_with_supervisor_id r
            LEFT JOIN final_paths fp
                ON r.cfn_person_id = fp.cfn_person_id
                AND r.week_begin = fp.week_begin
        )

        SELECT * FROM source_data

    ) AS src

    ON tgt.cfn_person_id = src.cfn_person_id
       AND tgt.week_begin = src.week_begin

    -- UPDATE existing records (metadata may have changed)
    WHEN MATCHED THEN UPDATE SET
        tgt.employee_name = src.employee_name,
        tgt.email = src.email,
        tgt.direct_supervisor_name = src.direct_supervisor_name,
        tgt.direct_supervisor_id = src.direct_supervisor_id,
        tgt.job_title = src.job_title,
        tgt.employee_status = src.employee_status,
        tgt.contractor = src.contractor,
        tgt.employee_location = src.employee_location,
        tgt.financial_business_unit = src.financial_business_unit,
        tgt.financial_department = src.financial_department,
        tgt.employee_path = src.employee_path,
        tgt.insert_date = src.insert_date

    -- INSERT new records (date_end placeholder, recalculated in Step 2)
    WHEN NOT MATCHED THEN INSERT (
        employee_key,
        cfn_person_id,
        week_begin,
        date_end,
        employee_name,
        email,
        direct_supervisor_name,
        direct_supervisor_id,
        job_title,
        employee_status,
        contractor,
        employee_location,
        financial_business_unit,
        financial_department,
        employee_path,
        insert_date
    ) VALUES (
        seq_employee_key.NEXTVAL,
        src.cfn_person_id,
        src.week_begin,
        '9999-12-31'::DATE,  -- Placeholder, recalculated below
        src.employee_name,
        src.email,
        src.direct_supervisor_name,
        src.direct_supervisor_id,
        src.job_title,
        src.employee_status,
        src.contractor,
        src.employee_location,
        src.financial_business_unit,
        src.financial_department,
        src.employee_path,
        src.insert_date
    );

    -- =============================================================
    -- Step 2: Recalculate date_end for all affected persons
    -- This ensures previously loaded records get correct date_end
    -- when new weeks are added for the same person
    -- =============================================================

    UPDATE staffingdata tgt
    SET date_end = src.calculated_date_end
    FROM (
        SELECT
            employee_key,
            COALESCE(
                DATEADD(DAY, -1, LEAD(week_begin) OVER (
                    PARTITION BY cfn_person_id
                    ORDER BY week_begin
                )),
                '9999-12-31'::DATE
            ) AS calculated_date_end
        FROM staffingdata
        WHERE cfn_person_id IN (SELECT DISTINCT cfn_person_id FROM staffingdata_raw)
    ) src
    WHERE tgt.employee_key = src.employee_key
      AND tgt.date_end != src.calculated_date_end;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'status', 'Load completed successfully',
        'validation_result', validation_result
    )::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;

END;
$$;
```

---

## Usage

```sql
-- 1. Load CSV into staffingdata_raw via Snowsight UI (truncate first if needed)
TRUNCATE TABLE staffingdata_raw;
-- [Upload CSV via UI]

-- 2. Run the load procedure
CALL usp_load_staffingdata();

-- 3. Verify results
SELECT * FROM staffingdata ORDER BY week_begin DESC, cfn_person_id;
```

---

# Swipes PTO Data

## Tables

```sql
-- =============================================================
-- RAW TABLE: swipesptoraw
-- =============================================================
CREATE TABLE swipesptoraw (
    cfn_person_id INTEGER,
    week_begin DATE,
    date DATE,
    time TIME,
    single_cinci_credit NUMERIC,
    pto_in_office_credit NUMERIC
);

-- =============================================================
-- DESTINATION TABLE: swipespto
-- =============================================================
CREATE TABLE swipespto (
    record_key INTEGER,
    employee_key INTEGER,
    swipe_date DATE,
    swipe_time TIME,
    single_cinci_credit NUMERIC,
    pto_in_office_credit NUMERIC,
    insert_date DATETIME
);

-- Sequence for generating record_key
CREATE SEQUENCE IF NOT EXISTS seq_swipe_record_key START = 1 INCREMENT = 1;
```

---

## Load Procedure

```sql
-- =============================================================
-- STORED PROCEDURE: sp_swipespto_insert
-- =============================================================
CREATE OR REPLACE PROCEDURE sp_swipespto_insert()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    -- =============================================================
    -- Step 1: Delete existing swipes for all employee/day
    -- combinations found in the source table
    -- =============================================================
    DELETE FROM swipespto
    WHERE (employee_key, swipe_date) IN (
        SELECT DISTINCT
            s.employee_key,
            r.date
        FROM swipesptoraw r
        LEFT JOIN staffingdata s
            ON r.cfn_person_id = s.cfn_person_id
            AND r.date BETWEEN s.week_begin AND s.date_end
    );

    -- =============================================================
    -- Step 2: Insert all swipes from source with employee_key lookup
    -- Lookup employee_key from staffingdata using date range join
    -- (swipe date between week_begin and date_end)
    -- =============================================================
    INSERT INTO swipespto (
        record_key,
        employee_key,
        swipe_date,
        swipe_time,
        single_cinci_credit,
        pto_in_office_credit,
        insert_date
    )
    SELECT
        seq_swipe_record_key.NEXTVAL,
        s.employee_key,
        r.date AS swipe_date,
        COALESCE(r.time, '08:00:00'::TIME) AS swipe_time,
        r.single_cinci_credit,
        r.pto_in_office_credit,
        CURRENT_TIMESTAMP()
    FROM swipesptoraw r
    LEFT JOIN staffingdata s
        ON r.cfn_person_id = s.cfn_person_id
        AND r.date BETWEEN s.week_begin AND s.date_end;

    RETURN 'Swipes PTO load completed successfully';

END;
$$;
```

---

## Usage

```sql
-- 1. Load CSV into swipesptoraw via Snowsight UI
TRUNCATE TABLE swipesptoraw;
-- [Upload CSV via UI]

-- 2. Run the load procedure
CALL sp_swipespto_insert();

-- 3. Verify results
SELECT * FROM swipespto ORDER BY swipe_date DESC, swipe_time DESC;
```

---

# Swipes PTO Weekly View

## View Definition

```sql
-- =============================================================
-- VIEW: swipes_pto_weekly
-- Aggregates swipe data at week level, including employees
-- who did not swipe (zero swipes counted)
-- =============================================================
CREATE OR REPLACE VIEW swipes_pto_weekly AS
WITH date_range AS (
    -- Get the date range from swipespto
    SELECT
        MIN(swipe_date) AS min_date,
        MAX(swipe_date) AS max_date
    FROM swipespto
),

-- Get all weekdays in the date range (exclude weekends)
calendar_days AS (
    SELECT
        d.date,
        d.week_start
    FROM dim_date d
    CROSS JOIN date_range dr
    WHERE d.date BETWEEN dr.min_date AND dr.max_date
      AND d.day_of_week NOT IN (5, 6)  -- Exclude weekends
),

-- Cross join calendar days with valid employees for each day
-- An employee is valid if the date falls between their week_begin and date_end
employee_day_base AS (
    SELECT
        cd.date,
        cd.week_start,
        s.employee_key
    FROM calendar_days cd
    INNER JOIN staffingdata s
        ON cd.date BETWEEN s.week_begin AND s.date_end
),

-- Enhance with swipe counts per employee per day
enhanced_data AS (
    SELECT
        edb.employee_key,
        edb.date,
        edb.week_start,
        COUNT(sp.record_key) AS swipes,
        CASE WHEN COUNT(sp.record_key) >= 1 THEN 1 ELSE 0 END AS day_in_office
    FROM employee_day_base edb
    LEFT JOIN swipespto sp
        ON edb.employee_key = sp.employee_key
        AND edb.date = sp.swipe_date
    GROUP BY edb.employee_key, edb.date, edb.week_start
)

-- Final aggregation at week level
SELECT
    employee_key,
    week_start,
    SUM(swipes) AS swipes,
    SUM(day_in_office) AS days_in_office
FROM enhanced_data
GROUP BY employee_key, week_start;
```

---

## Usage

```sql
-- Query weekly swipe summary for all employees
SELECT * FROM swipes_pto_weekly ORDER BY week_start DESC, employee_key;

-- Join with staffingdata for employee details
SELECT
    v.week_start,
    s.employee_name,
    s.financial_department,
    v.swipes,
    v.days_in_office
FROM swipes_pto_weekly v
JOIN staffingdata s
    ON v.employee_key = s.employee_key
    AND v.week_start = s.week_begin
ORDER BY v.week_start DESC, s.employee_name;
```
