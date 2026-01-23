```sql
CREATE TABLE staffingdata_raw (
    cfn_person_id INTEGER,
    week DATE,
    employee_name STRING,
    email STRING,
    direct_supervisor_name STRING,
    location STRING,
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
    week DATE,
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
    location STRING,
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
BEGIN

    -- =============================================================
    -- MERGE: Upsert from raw into destination with employee_path
    -- Key: (week, cfn_person_id)
    -- =============================================================

    MERGE INTO staffingdata AS tgt
    USING (
        -- Resolve direct_supervisor_name to direct_supervisor_id via self-join
        WITH RECURSIVE raw_with_supervisor_id AS (
            SELECT
                r.cfn_person_id,
                r.week,
                r.employee_name,
                r.email,
                r.direct_supervisor_name,
                sup.cfn_person_id AS direct_supervisor_id,
                r.job_title,
                r.employee_status,
                r.contractor,
                r.location,
                r.financial_business_unit,
                r.financial_department
            FROM staffingdata_raw r
            LEFT JOIN staffingdata_raw sup
                ON LOWER(TRIM(r.direct_supervisor_name)) = LOWER(TRIM(sup.employee_name))
                AND r.week = sup.week
        ),

        -- Build employee_path using recursive CTE
        supervisor_chain AS (
            -- Anchor: Start with each employee, first level is their direct supervisor
            SELECT
                cfn_person_id,
                week,
                direct_supervisor_id AS current_supervisor_id,
                COALESCE(CAST(direct_supervisor_id AS VARCHAR), '') AS employee_path,
                1 AS lvl
            FROM raw_with_supervisor_id

            UNION ALL

            -- Recursive: Walk up the chain - find supervisor's supervisor
            SELECT
                sc.cfn_person_id,
                sc.week,
                r.direct_supervisor_id AS current_supervisor_id,
                sc.employee_path || ' | ' || CAST(r.direct_supervisor_id AS VARCHAR) AS employee_path,
                sc.lvl + 1
            FROM supervisor_chain sc
            INNER JOIN raw_with_supervisor_id r
                ON sc.current_supervisor_id = r.cfn_person_id
                AND sc.week = r.week
            WHERE sc.current_supervisor_id IS NOT NULL
              AND r.direct_supervisor_id IS NOT NULL
              AND sc.lvl < 20  -- Safety limit to prevent infinite loops
        ),

        -- Get the deepest path per employee/week (final complete path)
        final_paths AS (
            SELECT
                cfn_person_id,
                week,
                employee_path
            FROM supervisor_chain
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cfn_person_id, week
                ORDER BY lvl DESC
            ) = 1
        ),

        -- Join raw data with computed paths
        source_data AS (
            SELECT
                r.cfn_person_id,
                r.week,
                r.employee_name,
                r.email,
                r.direct_supervisor_name,
                r.direct_supervisor_id,
                r.job_title,
                r.employee_status,
                r.contractor,
                r.location,
                r.financial_business_unit,
                r.financial_department,
                COALESCE(fp.employee_path, '') AS employee_path,
                CURRENT_TIMESTAMP() AS insert_date
            FROM raw_with_supervisor_id r
            LEFT JOIN final_paths fp
                ON r.cfn_person_id = fp.cfn_person_id
                AND r.week = fp.week
        )

        SELECT * FROM source_data

    ) AS src

    ON tgt.cfn_person_id = src.cfn_person_id
       AND tgt.week = src.week

    -- UPDATE existing records (metadata may have changed)
    WHEN MATCHED THEN UPDATE SET
        tgt.employee_name = src.employee_name,
        tgt.email = src.email,
        tgt.direct_supervisor_name = src.direct_supervisor_name,
        tgt.direct_supervisor_id = src.direct_supervisor_id,
        tgt.job_title = src.job_title,
        tgt.employee_status = src.employee_status,
        tgt.contractor = src.contractor,
        tgt.location = src.location,
        tgt.financial_business_unit = src.financial_business_unit,
        tgt.financial_department = src.financial_department,
        tgt.employee_path = src.employee_path,
        tgt.insert_date = src.insert_date

    -- INSERT new records
    WHEN NOT MATCHED THEN INSERT (
        employee_key,
        cfn_person_id,
        week,
        employee_name,
        email,
        direct_supervisor_name,
        direct_supervisor_id,
        job_title,
        employee_status,
        contractor,
        location,
        financial_business_unit,
        financial_department,
        employee_path,
        insert_date
    ) VALUES (
        seq_employee_key.NEXTVAL,
        src.cfn_person_id,
        src.week,
        src.employee_name,
        src.email,
        src.direct_supervisor_name,
        src.direct_supervisor_id,
        src.job_title,
        src.employee_status,
        src.contractor,
        src.location,
        src.financial_business_unit,
        src.financial_department,
        src.employee_path,
        src.insert_date
    );

    RETURN 'Load completed successfully';

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
SELECT * FROM staffingdata ORDER BY week DESC, cfn_person_id;
```
