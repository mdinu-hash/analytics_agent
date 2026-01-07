-- DATA INTEGRITY CHECKS SQL
-- Run these queries individually after loading each table to validate data integrity
-- Based on q_data_integrity_checks.py

-- ================================================================
-- DATE DIMENSION TABLE CHECKS  
-- ================================================================

-- Check 0.1: No duplicated date_key values
SELECT 'Check 0.1: No duplicated date_key values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('date_key ' || date_key || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT date_key, COUNT(*) as count
    FROM date_dimension
    GROUP BY date_key
    HAVING COUNT(*) > 1
) duplicates;

-- Check 0.2: Target count: Exactly 365 date records (one year)
SELECT 'Check 0.2: Target count: Exactly 365 date records (one year)' as check_name,
       CASE WHEN COUNT(*) = 365 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(*) = 365 
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(*) || ' date records' 
       END as details
FROM date_dimension;

-- Check 0.3: Date range validation: September 30, 2024 to September 30, 2025
SELECT 'Check 0.3: Date range validation: September 30, 2024 to September 30, 2025' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('date_key ' || date_key || ': ' || full_date, '; ') as details
FROM date_dimension
WHERE full_date < '2024-09-30' OR full_date > '2025-09-30';

-- ================================================================
-- BUSINESS LINE TABLE CHECKS
-- ================================================================

-- Check 5.1: No duplicated business_line_key values
SELECT 'Check 5.1: No duplicated business_line_key values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('business_line_key ' || business_line_key || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT business_line_key, COUNT(*) as count
    FROM business_line
    GROUP BY business_line_key
    HAVING COUNT(*) > 1
) duplicates;

-- Check 5.2: business_line_name is not null and not empty
SELECT 'Check 5.2: business_line_name is not null and not empty' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('business_line_key ' || business_line_key, '; ') as details
FROM business_line
WHERE business_line_name IS NULL OR business_line_name = '' OR TRIM(business_line_name) = '';

-- ================================================================
-- TIER FEE TABLE CHECKS
-- ================================================================

-- Check 12.1: No duplicated tier_fee_id values
SELECT 'Check 12.1: No duplicated tier_fee_id values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('tier_fee_id ' || tier_fee_id || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT tier_fee_id, COUNT(*) as count
    FROM tier_fee
    GROUP BY tier_fee_id
    HAVING COUNT(*) > 1
) duplicates;

-- Check 12.2: tier_min_aum < tier_max_aum for all records
SELECT 'Check 12.2: tier_min_aum < tier_max_aum for all records' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('tier_fee_id ' || tier_fee_id || ': min $' || TO_CHAR(tier_min_aum, 'FM999,999,999') || ' >= max $' || TO_CHAR(tier_max_aum, 'FM999,999,999'), '; ') as details
FROM tier_fee
WHERE tier_min_aum >= tier_max_aum;

-- Check 12.3: fee_rate is between 0 and 3%
SELECT 'Check 12.3: fee_rate is between 0 and 3%' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('tier_fee_id ' || tier_fee_id || ': ' || tier_fee_pct || '%', '; ') as details
FROM tier_fee
WHERE tier_fee_pct < 0 OR tier_fee_pct > 3;

-- ================================================================
-- HOUSEHOLD TABLE CHECKS
-- ================================================================

-- Check 1.1: No duplicated household_key values
SELECT 'Check 1.1: No duplicated household_key values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_key ' || household_key || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT household_key, COUNT(*) as count
    FROM household
    GROUP BY household_key
    HAVING COUNT(*) > 1
) duplicates;

-- Check 1.2: Exactly one record per household_id where to_date = '9999-12-31'
SELECT 'Check 1.2: Exactly one record per household_id where to_date = 9999-12-31' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_id ' || household_id || ' has ' || count || ' current records', '; ') as details
FROM (
    SELECT household_id, COUNT(*) as count
    FROM household
    WHERE to_date = '9999-12-31'
    GROUP BY household_id
    HAVING COUNT(*) != 1
) current_records;

-- Check 1.3: Target count: Exactly 5,000 distinct household_id values
SELECT 'Check 1.3: Target count: Exactly 5,000 distinct household_id values' as check_name,
       CASE WHEN COUNT(DISTINCT household_id) = 5000 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(DISTINCT household_id) = 5000 
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(DISTINCT household_id) || ' distinct household_id values' 
       END as details
FROM household;

-- Check 1.4: No conflicting dates: from_date < to_date for all records
SELECT 'Check 1.4: No conflicting dates: from_date < to_date for all records' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_key ' || household_key || ' (id ' || household_id || '): from_date ' || from_date || ' >= to_date ' || to_date, '; ') as details
FROM household
WHERE from_date >= to_date;

-- Check 1.5: No gaps in SCD2 history (next record should start day after previous ends)
WITH next_records AS (
    SELECT household_id, to_date,
           LEAD(from_date) OVER (PARTITION BY household_id ORDER BY from_date) as next_from_date
    FROM household
)
SELECT 'Check 1.5: No gaps in SCD2 history (next record should start day after previous ends)' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_id ' || household_id || ': gap between to_date ' || to_date || ' and next from_date ' || next_from_date, '; ') as details
FROM next_records
WHERE next_from_date IS NOT NULL AND next_from_date != to_date + INTERVAL '1 day';

-- Check 1.6: household_registration_date alignment with tenure (±180 days)
SELECT 'Check 1.6: household_registration_date alignment with tenure (±180 days)' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_key ' || household_key || ' (id ' || household_id || '): ' || ROUND(days_diff) || ' days difference', '; ') as details
FROM (
    SELECT household_key, household_id, household_registration_date, household_tenure,
           DATE '2025-09-30' - INTERVAL '1 day' * (household_tenure * 365) as calculated_base,
           ABS(EXTRACT(DAYS FROM (household_registration_date - (DATE '2025-09-30' - INTERVAL '1 day' * (household_tenure * 365))))) as days_diff
    FROM household
    WHERE to_date = '9999-12-31'
) tenure_check
WHERE days_diff > 180;

-- Check 1.7: All household_advisor_id exist in advisors.advisor_id
SELECT 'Check 1.7: All household_advisor_id exist in advisors.advisor_id' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_id ' || household_advisor_id, '; ') as details
FROM (
    SELECT DISTINCT h.household_advisor_id
    FROM household h
    LEFT JOIN advisors a ON h.household_advisor_id = a.advisor_id
    WHERE a.advisor_id IS NULL
) missing_advisors;

-- Check 1.9: household_tenure is between 1 and 40 years
SELECT 'Check 1.9: household_tenure is between 1 and 40 years' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_key ' || household_key || ' (id ' || household_id || '): tenure ' || household_tenure, '; ') as details
FROM household
WHERE household_tenure < 1 OR household_tenure > 40;

-- ================================================================
-- ADVISORS TABLE CHECKS
-- ================================================================

-- Check 2.1: No duplicated advisor_key values
SELECT 'Check 2.1: No duplicated advisor_key values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_key ' || advisor_key || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT advisor_key, COUNT(*) as count
    FROM advisors
    GROUP BY advisor_key
    HAVING COUNT(*) > 1
) duplicates;

-- Check 2.2: Exactly one record per advisor_id where to_date = '9999-12-31'
SELECT 'Check 2.2: Exactly one record per advisor_id where to_date = 9999-12-31' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_id ' || advisor_id || ' has ' || count || ' current records', '; ') as details
FROM (
    SELECT advisor_id, COUNT(*) as count
    FROM advisors
    WHERE to_date = '9999-12-31'
    GROUP BY advisor_id
    HAVING COUNT(*) != 1
) current_records;

-- Check 2.3: Target count: Exactly 50 distinct advisor_id values
SELECT 'Check 2.3: Target count: Exactly 500 distinct advisor_id values' as check_name,
       CASE WHEN COUNT(DISTINCT advisor_id) = 50 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(DISTINCT advisor_id) = 50
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(DISTINCT advisor_id) || ' distinct advisor_id values' 
       END as details
FROM advisors;

-- Check 2.4: No conflicting dates: from_date < to_date for all records
SELECT 'Check 2.4: No conflicting dates: from_date < to_date for all records' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_key ' || advisor_key || ' (id ' || advisor_id || '): from_date ' || from_date || ' >= to_date ' || to_date, '; ') as details
FROM advisors
WHERE from_date >= to_date;

-- Check 2.5: No gaps in SCD2 history (next record should start day after previous ends)
WITH next_records AS (
    SELECT advisor_id, to_date,
           LEAD(from_date) OVER (PARTITION BY advisor_id ORDER BY from_date) as next_from_date
    FROM advisors
)
SELECT 'Check 2.5: No gaps in SCD2 history (next record should start day after previous ends)' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_id ' || advisor_id || ': gap between to_date ' || to_date || ' and next from_date ' || next_from_date, '; ') as details
FROM next_records
WHERE next_from_date IS NOT NULL AND next_from_date != to_date + INTERVAL '1 day';

-- Check 2.6: advisor_tenure is between 1 and 40 years
SELECT 'Check 2.6: advisor_tenure is between 1 and 40 years' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_key ' || advisor_key || ' (id ' || advisor_id || '): tenure ' || advisor_tenure, '; ') as details
FROM advisors
WHERE advisor_tenure < 1 OR advisor_tenure > 40;

-- ================================================================
-- ACCOUNT TABLE CHECKS
-- ================================================================

-- Check 3.1: No duplicated account_key values
SELECT 'Check 3.1: No duplicated account_key values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT account_key, COUNT(*) as count
    FROM account
    GROUP BY account_key
    HAVING COUNT(*) > 1
) duplicates;

-- Check 3.2: Exactly one record per account_id where to_date = '9999-12-31'
SELECT 'Check 3.2: Exactly one record per account_id where to_date = 9999-12-31' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_id ' || account_id || ' has ' || count || ' current records', '; ') as details
FROM (
    SELECT account_id, COUNT(*) as count
    FROM account
    WHERE to_date = '9999-12-31'
    GROUP BY account_id
    HAVING COUNT(*) != 1
) current_records;

-- Check 3.3: Target count: Exactly 800 distinct account_id values
SELECT 'Check 3.3: Target count: Exactly 800 distinct account_id values' as check_name,
       CASE WHEN COUNT(DISTINCT account_id) = 800 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(DISTINCT account_id) = 800 
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(DISTINCT account_id) || ' distinct account_id values' 
       END as details
FROM account;

-- Check 3.4: No conflicting dates: from_date < to_date for all records
SELECT 'Check 3.4: No conflicting dates: from_date < to_date for all records' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' (id ' || account_id || '): from_date ' || from_date || ' >= to_date ' || to_date, '; ') as details
FROM account
WHERE from_date >= to_date;

-- Check 3.7: All closed accounts have closed_date > opened_date
SELECT 'Check 3.7: All closed accounts have closed_date > opened_date' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' (id ' || account_id || '): opened ' || opened_date || ', closed ' || closed_date, '; ') as details
FROM account
WHERE account_status = 'Closed' AND closed_date <= opened_date;

-- Check 3.8: Open accounts have closed_date IS NULL
SELECT 'Check 3.8: Open accounts have closed_date IS NULL' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' (id ' || account_id || '): closed_date ' || closed_date, '; ') as details
FROM account
WHERE account_status = 'Open' AND closed_date IS NOT NULL;

-- Check 3.9: Closed accounts have closed_date IS NOT NULL
SELECT 'Check 3.9: Closed accounts have closed_date IS NOT NULL' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' (id ' || account_id || ')', '; ') as details
FROM account
WHERE account_status = 'Closed' AND closed_date IS NULL;

-- Check 3.15: Accounts per advisor: min 2, max 40
SELECT 'Check 3.15: Accounts per advisor: min 2, max 40' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('advisor_key ' || a.advisor_key || ': ' || a.account_count || ' accounts', '; ') as details
FROM (
    SELECT advisor_key, COUNT(*) as account_count
    FROM account
    WHERE to_date = '9999-12-31'
    GROUP BY advisor_key
    HAVING COUNT(*) < 2 OR COUNT(*) > 40
) a;

-- ================================================================
-- PRODUCT TABLE CHECKS
-- ================================================================

-- Check 4.1: No duplicated product_id values
SELECT 'Check 4.1: No duplicated product_id values' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('product_id ' || product_id || ' appears ' || count || ' times', '; ') as details
FROM (
    SELECT product_id, COUNT(*) as count
    FROM product
    GROUP BY product_id
    HAVING COUNT(*) > 1
) duplicates;

-- Check 4.2: Target count: Exactly 350 distinct product_id values
SELECT 'Check 4.2: Target count: Exactly 350 distinct product_id values' as check_name,
       CASE WHEN COUNT(DISTINCT product_id) = 350 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(DISTINCT product_id) = 350 
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(DISTINCT product_id) || ' distinct product_id values' 
       END as details
FROM product;

-- Check 4.6: product_name is not null and not empty
SELECT 'Check 4.6: product_name is not null and not empty' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('product_id ' || product_id, '; ') as details
FROM product
WHERE product_name IS NULL OR product_name = '' OR TRIM(product_name) = '';

-- ================================================================
-- ADVISOR PAYOUT RATE TABLE CHECKS
-- ================================================================

-- Check 6.1: All firm_affiliation_model values from advisors table exist
SELECT 'Check 6.1: All firm_affiliation_model values from advisors table exist' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('firm_affiliation_model ''' || firm_affiliation_model || '''', '; ') as details
FROM (
    SELECT DISTINCT a.firm_affiliation_model
    FROM advisors a
    LEFT JOIN advisor_payout_rate apr ON a.firm_affiliation_model = apr.firm_affiliation_model
    WHERE apr.firm_affiliation_model IS NULL
) missing_payout_rates;

-- ================================================================
-- FACT ACCOUNT INITIAL ASSETS TABLE CHECKS
-- ================================================================

-- Check 7.3: account_initial_assets are positive (> 0)
SELECT 'Check 7.3: account_initial_assets are positive (> 0)' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key, '; ') as details
FROM fact_account_initial_assets
WHERE account_initial_assets <= 0;

-- Check 7.4: No account should exceed $20M in initial assets
SELECT 'Check 7.4: No account should exceed $20M in initial assets' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ': $' || TO_CHAR(account_initial_assets, 'FM999,999,999'), '; ') as details
FROM fact_account_initial_assets
WHERE account_initial_assets > 20000000;

-- ================================================================
-- FACT ACCOUNT MONTHLY TABLE CHECKS
-- ================================================================

-- Check 8.1: Exactly 12 distinct snapshot_date values (end-of-month dates)
SELECT 'Check 8.1: Exactly 13 distinct snapshot_date values (end-of-month dates)' as check_name,
       CASE WHEN COUNT(DISTINCT snapshot_date) = 13 THEN 0 ELSE 1 END as issues_found,
       CASE WHEN COUNT(DISTINCT snapshot_date) = 13 
            THEN 'PASS' 
            ELSE 'Found ' || COUNT(DISTINCT snapshot_date) || ' distinct snapshot_date values' 
       END as details
FROM fact_account_monthly;

-- Check 8.10: account_monthly_return is between -12% and +12%
SELECT 'Check 8.10: account_monthly_return is between -12% and +12%' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': ' || ROUND(account_monthly_return * 100, 2) || '%', '; ') as details
FROM fact_account_monthly
WHERE account_monthly_return < -0.12 OR account_monthly_return > 0.12;

-- Check 8.11: For snapshot_date = snapshot_start_date, account_monthly_return = 0%
SELECT 'Check 8.11: For snapshot_date = snapshot_start_date, account_monthly_return = 0%' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ': ' || ROUND(account_monthly_return * 100, 2) || '%', '; ') as details
FROM fact_account_monthly
WHERE snapshot_date = '2024-09-30' AND account_monthly_return != 0;

-- Check 8.13: account_assets is positive and ≤ $20M
SELECT 'Check 8.13: account_assets is positive and ≤ $20M' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': $' || TO_CHAR(account_assets, 'FM999,999,999'), '; ') as details
FROM fact_account_monthly
WHERE account_assets <= 0 OR account_assets > 20000000;

-- ================================================================
-- FACT ACCOUNT PRODUCT MONTHLY TABLE CHECKS
-- ================================================================

-- Check 9.3: Sum of product_allocation_pct per (snapshot_date, account_key) equals 100%
SELECT 'Check 9.3: Sum of product_allocation_pct per (snapshot_date, account_key) equals 100%' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': ' || ROUND(total_allocation, 2) || '%', '; ') as details
FROM (
    SELECT snapshot_date, account_key, SUM(product_allocation_pct) as total_allocation
    FROM fact_account_product_monthly
    GROUP BY snapshot_date, account_key
    HAVING ABS(SUM(product_allocation_pct) - 100.0) > 0.01
) allocation_issues;

-- Check 9.4: Each account has between 2 and 5 products
SELECT 'Check 9.4: Each account has between 2 and 5 products' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': ' || product_count || ' products', '; ') as details
FROM (
    SELECT snapshot_date, account_key, COUNT(*) as product_count
    FROM fact_account_product_monthly
    GROUP BY snapshot_date, account_key
    HAVING COUNT(*) < 2 OR COUNT(*) > 5
) product_count_issues;

-- Check 9.5: product_allocation_pct is between 0 and 100
SELECT 'Check 9.5: product_allocation_pct is between 0 and 100' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ', product_id ' || product_id || ' on ' || snapshot_date || ': ' || ROUND(product_allocation_pct, 2) || '%', '; ') as details
FROM fact_account_product_monthly
WHERE product_allocation_pct < 0 OR product_allocation_pct > 100;

-- ================================================================
-- FACT HOUSEHOLD MONTHLY TABLE CHECKS
-- ================================================================

-- Check 10.5: high_net_worth_flag is TRUE if and only if household_assets >= $1M
SELECT 'Check 10.5: high_net_worth_flag is TRUE if and only if household_assets >= $1M' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('household_key ' || household_key || ' on ' || snapshot_date || ': $' || TO_CHAR(household_assets, 'FM999,999,999') || ', flag=' || high_net_worth_flag, '; ') as details
FROM fact_household_monthly
WHERE (household_assets >= 1000000 AND high_net_worth_flag = false)
   OR (household_assets < 1000000 AND high_net_worth_flag = true);

-- ================================================================
-- FACT REVENUE MONTHLY TABLE CHECKS
-- ================================================================

-- Check 11.6: All monetary amounts are non-negative
SELECT 'Check 11.6: All monetary amounts are non-negative' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': ' || field || ' = $' || TO_CHAR(amount, 'FM999,999,999.00'), '; ') as details
FROM (
    SELECT snapshot_date, account_key, 'gross_fee_amount' as field, gross_fee_amount as amount
    FROM fact_revenue_monthly
    WHERE gross_fee_amount < 0
    UNION ALL
    SELECT snapshot_date, account_key, 'third_party_fee', third_party_fee
    FROM fact_revenue_monthly
    WHERE third_party_fee < 0
    UNION ALL
    SELECT snapshot_date, account_key, 'advisor_payout_amount', advisor_payout_amount
    FROM fact_revenue_monthly
    WHERE advisor_payout_amount < 0
) negative_amounts;

-- Check 11.7: net_revenue should be positive
SELECT 'Check 11.7: net_revenue should be positive' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('account_key ' || account_key || ' on ' || snapshot_date || ': $' || TO_CHAR(net_revenue, 'FM999,999,999.00'), '; ') as details
FROM fact_revenue_monthly
WHERE net_revenue <= 0;

-- ================================================================
-- FACT CUSTOMER FEEDBACK TABLE CHECKS
-- ================================================================

-- Check 13.5: feedback_date is between January 1st of previous year and current_date
SELECT 'Check 13.5: feedback_date is between January 1st of previous year and current_date' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('feedback_id ' || feedback_id || ': ' || feedback_date, '; ') as details
FROM fact_customer_feedback
WHERE feedback_date < '2024-01-01' OR feedback_date > '2025-09-30';

-- Check 13.6: satisfaction_score is between 0 and 100
SELECT 'Check 13.6: satisfaction_score is between 0 and 100' as check_name,
       COUNT(*) as issues_found,
       STRING_AGG('feedback_id ' || feedback_id || ': ' || satisfaction_score, '; ') as details
FROM fact_customer_feedback
WHERE satisfaction_score < 0 OR satisfaction_score > 100;

-- ================================================================
-- SUMMARY QUERY - Run this to see all checks at once
-- ================================================================

/*
-- To run all checks and see a summary, uncomment and run this query:

WITH all_checks AS (
    -- Add all individual check queries here with UNION ALL
    -- This will give you a comprehensive view of all integrity issues
)
SELECT check_name, issues_found, 
       CASE WHEN issues_found = 0 THEN 'PASS' ELSE details END as result
FROM all_checks
ORDER BY issues_found DESC, check_name;
*/