
-- Date dimension table
DROP TABLE IF EXISTS date CASCADE;
CREATE TABLE date (
    calendar_day DATE PRIMARY KEY,
    month_name TEXT NOT NULL,
    month INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    quarter INTEGER NOT NULL,
    quarter_name TEXT NOT NULL,
    quarter_start_date DATE NOT NULL,
    quarter_end_date DATE NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Business line lookup table
DROP TABLE IF EXISTS business_line CASCADE;
CREATE TABLE business_line (
    business_line_key SERIAL PRIMARY KEY,
    business_line_name TEXT NOT NULL CHECK (business_line_name IN (
        'Managed Portfolio', 
        'Separately Managed Account', 
        'Mutual Fund Wrap', 
        'Annuity', 
        'Cash'
    ))
);

-- Advisors table (SCD Type 2)
DROP TABLE IF EXISTS advisors CASCADE;
CREATE TABLE advisors (
    advisor_key SERIAL PRIMARY KEY,
    advisor_id INTEGER NOT NULL,
    advisor_tenure INTEGER NOT NULL CHECK (advisor_tenure BETWEEN 1 AND 40),
    firm_name TEXT NOT NULL,
    firm_affiliation_model TEXT NOT NULL CHECK (firm_affiliation_model IN (
        'RIA', 'Hybrid RIA', 'Broker-Dealer W-2', 'Independent BD', 
        'Bank/Trust', 'Insurance BD', 'Wirehouse'
    )),
    advisor_role TEXT NOT NULL CHECK (advisor_role IN (
        'Lead Advisor', 'Associate Advisor', 'Relationship Manager', 
        'Portfolio Manager', 'Client Service Associate'
    )),
    advisor_status TEXT NOT NULL CHECK (advisor_status IN ('Active', 'Terminated')),
    practice_segment TEXT NOT NULL CHECK (practice_segment IN (
        'Solo Practice', 'Small Team', 'Ensemble', 'Enterprise'
    )),
    from_date DATE NOT NULL,
    to_date DATE NOT NULL DEFAULT '9999-12-31'
);

-- Households table (SCD Type 2)
DROP TABLE IF EXISTS household CASCADE;
CREATE TABLE household (
    household_key SERIAL PRIMARY KEY,
    household_id INTEGER NOT NULL,
    household_tenure INTEGER NOT NULL CHECK (household_tenure BETWEEN 1 AND 40),
    household_registration_type TEXT NOT NULL CHECK (household_registration_type IN (
        'Individual', 'Joint', 'Trust', 'Institutional'
    )),
    household_registration_date DATE NOT NULL,
    household_segment TEXT NOT NULL CHECK (household_segment IN (
        'Self-Directed', 'Advice-Seeking', 'Discretionary Managed', 
        'Retirement Income', 'Business/Institutional', 'Active Trader'
    )),
    household_status TEXT NOT NULL CHECK (household_status IN ('Active', 'Terminated')),
    household_advisor_id INTEGER NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL DEFAULT '9999-12-31'
);

-- Accounts table (SCD Type 2)
DROP TABLE IF EXISTS account CASCADE;
CREATE TABLE account (
    account_key SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    advisor_key INTEGER NOT NULL REFERENCES advisors(advisor_key),
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    business_line_key INTEGER NOT NULL REFERENCES business_line(business_line_key),
    account_type TEXT NOT NULL CHECK (account_type IN (
        'Taxable', 'IRA', '401k', 'Trust', 'Custody'
    )),
    account_custodian TEXT NOT NULL CHECK (account_custodian IN (
        'Schwab', 'Fidelity', 'Pershing', 'In-House', 'BankTrust'
    )),
    opened_date DATE NOT NULL,
    account_status TEXT NOT NULL CHECK (account_status IN ('Open', 'Closed')),
    closed_date DATE,
    account_risk_profile TEXT NOT NULL CHECK (account_risk_profile IN (
        'Conservative', 'Moderate', 'Aggressive'
    )),
    from_date DATE NOT NULL,
    to_date DATE NOT NULL DEFAULT '9999-12-31',
    
    -- Business rule constraints
    CONSTRAINT chk_closed_date CHECK (
        (account_status = 'Open' AND closed_date IS NULL) OR
        (account_status = 'Closed' AND closed_date IS NOT NULL AND closed_date > opened_date)
    )
);

-- Product table
DROP TABLE IF EXISTS product CASCADE;
CREATE TABLE product (
    product_id SERIAL PRIMARY KEY,
    asset_category TEXT NOT NULL CHECK (asset_category IN (
        'Equity', 'Fixed Income', 'Multi-Asset', 'Cash'
    )),
    asset_subcategory TEXT NOT NULL CHECK (asset_subcategory IN (
        'Common Stock', 'Preferred Stock', 'Equity Mutual Fund', 
        'Balanced Fund (60/40)', 'Target-Date Fund', 'U.S. Treasury Bill', 
        'U.S. Treasury Note', 'Investment-Grade Corporate Bond', 
        'Municipal Bond', 'Money Market Fund'
    )),
    product_line TEXT NOT NULL CHECK (product_line IN (
        'Mutual Fund', 'ETF', 'Separately Managed Account Strategy', 
        'Annuity Contract', 'Money Market'
    )),
    product_name TEXT NOT NULL
);

-- Tier fee structure table
DROP TABLE IF EXISTS tier_fee CASCADE;
CREATE TABLE tier_fee (
    tier_fee_id SERIAL PRIMARY KEY,
    business_line_key INTEGER NOT NULL REFERENCES business_line(business_line_key),
    tier_min_aum DECIMAL(15,2) NOT NULL,
    tier_max_aum DECIMAL(15,2),
    tier_fee_bps INTEGER NOT NULL -- stored as basis points (e.g., 90 for 0.90%)
);

-- Advisor payout rates table
DROP TABLE IF EXISTS advisor_payout_rate CASCADE;
CREATE TABLE advisor_payout_rate (
    firm_affiliation_model TEXT PRIMARY KEY CHECK (firm_affiliation_model IN (
        'RIA', 'Hybrid RIA', 'Broker-Dealer W-2', 'Independent BD', 
        'Bank/Trust', 'Insurance BD', 'Wirehouse'
    )),
    advisor_payout_rate DECIMAL(5,4) NOT NULL -- stored as decimal (e.g., 0.7800 for 78%)
);

-- Fact table: Initial account assets
DROP TABLE IF EXISTS fact_account_initial_assets CASCADE;
CREATE TABLE fact_account_initial_assets (
    account_key INTEGER PRIMARY KEY REFERENCES account(account_key),
    account_initial_assets DECIMAL(15,2) NOT NULL CHECK (account_initial_assets > 0)
);

-- Fact table: Monthly account data
DROP TABLE IF EXISTS fact_account_monthly CASCADE;
CREATE TABLE fact_account_monthly (
    snapshot_date DATE NOT NULL,
    account_key INTEGER NOT NULL REFERENCES account(account_key),
    account_monthly_return DECIMAL(8,6) NOT NULL CHECK (account_monthly_return BETWEEN -0.12 AND 0.12),
    account_net_flow DECIMAL(15,2) NOT NULL,
    account_assets_previous_month DECIMAL(15,2) NOT NULL,
    account_assets DECIMAL(15,2) NOT NULL,
    advisor_key INTEGER NOT NULL REFERENCES advisors(advisor_key),
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    business_line_key INTEGER NOT NULL REFERENCES business_line(business_line_key),
    
    PRIMARY KEY (snapshot_date, account_key)
);

-- Fact table: Account product allocations monthly
DROP TABLE IF EXISTS fact_account_product_monthly CASCADE;
CREATE TABLE fact_account_product_monthly (
    snapshot_date DATE NOT NULL,
    account_key INTEGER NOT NULL REFERENCES account(account_key),
    product_id INTEGER NOT NULL REFERENCES product(product_id),
    product_allocation_pct DECIMAL(5,2) NOT NULL CHECK (product_allocation_pct BETWEEN 0 AND 100),
    
    PRIMARY KEY (snapshot_date, account_key, product_id)
);

-- Fact table: Monthly household aggregations
DROP TABLE IF EXISTS fact_household_monthly CASCADE;
CREATE TABLE fact_household_monthly (
    snapshot_date DATE NOT NULL,
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    household_assets DECIMAL(15,2) NOT NULL,
    asset_range_bucket TEXT NOT NULL CHECK (asset_range_bucket IN (
        '$0 – $100k', '$100k – $250k', '$250k – $500k', 
        '$500k – $1M', '$1M – $5M', '$5M – $10M', '$10M+'
    )),
    high_net_worth_flag BOOLEAN NOT NULL,
    household_net_flow DECIMAL(15,2) NOT NULL,
    
    PRIMARY KEY (snapshot_date, household_key)
);

-- Fact table: Monthly revenue calculations
DROP TABLE IF EXISTS fact_revenue_monthly CASCADE;
CREATE TABLE fact_revenue_monthly (
    snapshot_date DATE NOT NULL,
    account_key INTEGER NOT NULL REFERENCES account(account_key),
    advisor_key INTEGER NOT NULL REFERENCES advisors(advisor_key),
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    business_line_key INTEGER NOT NULL REFERENCES business_line(business_line_key),
    account_assets DECIMAL(15,2) NOT NULL,
    fee_percentage DECIMAL(8,6) NOT NULL,
    gross_fee_amount DECIMAL(15,2) NOT NULL,
    third_party_fee DECIMAL(15,2) NOT NULL,
    advisor_payout_rate DECIMAL(5,4) NOT NULL,
    advisor_payout_amount DECIMAL(15,2) NOT NULL,
    net_revenue DECIMAL(15,2) NOT NULL,
    
    PRIMARY KEY (snapshot_date, account_key)
);

-- Transactions table (high volume)
DROP TABLE IF EXISTS transactions CASCADE;
CREATE TABLE transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    advisor_key INTEGER NOT NULL REFERENCES advisors(advisor_key),
    account_key INTEGER NOT NULL REFERENCES account(account_key),
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    business_line_key INTEGER NOT NULL REFERENCES business_line(business_line_key),
    product_id INTEGER NOT NULL REFERENCES product(product_id),
    transaction_date DATE NOT NULL,
    gross_revenue DECIMAL(15,2),
    revenue_fee DECIMAL(15,2),
    third_party_fee DECIMAL(15,2),
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('deposit', 'withdrawal', 'fee'))
);

-- Customer feedback fact table
DROP TABLE IF EXISTS fact_customer_feedback CASCADE;
CREATE TABLE fact_customer_feedback (
    feedback_id SERIAL PRIMARY KEY,
    feedback_date DATE NOT NULL,
    household_key INTEGER NOT NULL REFERENCES household(household_key),
    advisor_key INTEGER NOT NULL REFERENCES advisors(advisor_key),
    feedback_text TEXT CHECK (LENGTH(feedback_text) <= 200),
    satisfaction_score INTEGER NOT NULL CHECK (satisfaction_score BETWEEN 0 AND 100)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Household indexes
CREATE UNIQUE INDEX ux_household_current ON household(household_id)
WHERE to_date = DATE '9999-12-31';

CREATE INDEX ix_household_id_window ON household(household_id, from_date, to_date);

-- Advisor indexes
CREATE UNIQUE INDEX ux_advisors_current ON advisors(advisor_id)
WHERE to_date = DATE '9999-12-31';

CREATE INDEX ix_advisor_id_window ON advisors(advisor_id, from_date, to_date);

-- Account indexes
CREATE UNIQUE INDEX ux_accounts_current ON account(account_id)
WHERE to_date = DATE '9999-12-31';

CREATE INDEX ix_account_id_window ON account(account_id, from_date, to_date);
CREATE INDEX ix_accounts_household ON account(household_key);
CREATE INDEX ix_accounts_advisor ON account(advisor_key);

-- Fact table indexes for performance
CREATE INDEX ix_fact_account_monthly_date ON fact_account_monthly(snapshot_date);
CREATE INDEX ix_fact_account_monthly_advisor ON fact_account_monthly(advisor_key);
CREATE INDEX ix_fact_account_monthly_household ON fact_account_monthly(household_key);

CREATE INDEX ix_fact_revenue_monthly_date ON fact_revenue_monthly(snapshot_date);
CREATE INDEX ix_fact_revenue_monthly_advisor ON fact_revenue_monthly(advisor_key);

-- Customer feedback indexes
CREATE INDEX ix_fb_house_date ON fact_customer_feedback(household_key, feedback_date);
CREATE INDEX ix_fb_adv_date ON fact_customer_feedback(advisor_key, feedback_date);

-- Transaction table indexes (for high volume queries)
CREATE INDEX ix_transactions_date ON transactions(transaction_date);
CREATE INDEX ix_transactions_advisor ON transactions(advisor_key);
CREATE INDEX ix_transactions_account ON transactions(account_key);
CREATE INDEX ix_transactions_household ON transactions(household_key);

-- Date dimension index
CREATE INDEX ix_date_year_quarter ON date(year, quarter);
CREATE INDEX ix_date_month ON date(month_start_date, month_end_date);