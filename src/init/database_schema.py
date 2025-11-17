# Database Schema Glossary
database_schema = [
    {
        'table_name': 'public.date',
        'table_description': 'Calendar dimension table for time-based analysis',
        'columns': {
            'calendar_day': {
                'description': 'Specific calendar date (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'month_name': {
                'description': 'Full month name (January, February, etc.)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'month': {
                'description': 'Month number (1-12)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'day_of_month': {
                'description': 'Day within the month (1-31)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'month_start_date': {
                'description': 'First day of the month',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'month_end_date': {
                'description': 'Last day of the month',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'quarter': {
                'description': 'Quarter number (1-4)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'quarter_name': {
                'description': 'Quarter label (Q1, Q2, Q3, Q4)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'quarter_start_date': {
                'description': 'First day of the quarter',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'quarter_end_date': {
                'description': 'Last day of the quarter',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'year': {
                'description': 'Four-digit year',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'is_weekend': {
                'description': 'Boolean flag indicating weekend days',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.business_line',
        'table_description': 'Investment product service lines offered by Capital Partners to advisors',
        'columns': {
            'business_line_key': {
                'description': 'Unique identifier for Capital Partners business line (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'business_line_name': {
                'description': 'Capital Partners product service category (Managed Portfolio, SMA, Mutual Fund Wrap, Annuity, Cash)',
                'query_to_get_column_values': 'SELECT DISTINCT business_line_name FROM business_line ORDER BY business_line_name',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.advisors',
        'table_description': 'Financial advisors with historical tracking (SCD Type 2)',
        'columns': {
            'advisor_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_id': {
                'description': 'Natural business identifier for the advisor',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_tenure': {
                'description': 'Years of experience as an advisor (1-40 years)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'firm_name': {
                'description': 'Name of the firm the advisor is part of (NOT Capital Partners)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'firm_affiliation_model': {
                'description': 'Business model of the advisor firm (RIA, Hybrid RIA, Broker-Dealer, etc.)',
                'query_to_get_column_values': 'SELECT DISTINCT firm_affiliation_model FROM advisors ORDER BY firm_affiliation_model',
                'query_to_get_date_range': ''
            },
            'advisor_role': {
                'description': 'Position within the firm (Lead Advisor, Associate, etc.)',
                'query_to_get_column_values': 'SELECT DISTINCT advisor_role FROM advisors ORDER BY advisor_role',
                'query_to_get_date_range': ''
            },
            'advisor_status': {
                'description': 'Current employment status (Active/Terminated)',
                'query_to_get_column_values': 'SELECT DISTINCT advisor_status FROM advisors ORDER BY advisor_status',
                'query_to_get_date_range': ''
            },
            'practice_segment': {
                'description': 'Size classification of advisory practice',
                'query_to_get_column_values': 'SELECT DISTINCT practice_segment FROM advisors ORDER BY practice_segment',
                'query_to_get_date_range': ''
            },
            'from_date': {
                'description': 'Effective start date for this record version',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'to_date': {
                'description': 'Effective end date for this record version',
                'values': "SELECT distinct concat('filter on to_date = ','''',to_date,'''',' to identify the most recent record of the advisor_id' ) as value_description FROM public.advisors where to_date = '9999-12-31'",
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.household',
        'table_description': 'Client households with historical tracking (SCD Type 2)',
        'columns': {
            'household_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_id': {
                'description': 'Natural business identifier for the household',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_tenure': {
                'description': 'Years as a client (1-40 years)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_registration_type': {
                'description': 'Legal registration structure (Individual, Joint, Trust, Institutional)',
                'query_to_get_column_values': 'SELECT DISTINCT household_registration_type FROM household ORDER BY household_registration_type',
                'query_to_get_date_range': ''
            },
            'household_registration_date': {
                'description': 'Date when household became a client',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_segment': {
                'description': 'Client service model classification',
                'query_to_get_column_values': 'SELECT DISTINCT household_segment FROM household ORDER BY household_segment',
                'query_to_get_date_range': ''
            },
            'household_status': {
                'description': 'Current relationship status (Active/Terminated)',
                'query_to_get_column_values': 'SELECT DISTINCT household_status FROM household ORDER BY household_status',
                'query_to_get_date_range': ''
            },
            'household_advisor_id': {
                'description': 'ID of the primary advisor serving this household',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'from_date': {
                'description': 'Effective start date for this record version',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'to_date': {
                'description': 'Effective end date for this record version',
                'values': "SELECT distinct concat('filter on to_date = ','''',to_date,'''',' to identify the most recent record of the household_id' ) as value_description FROM public.household where to_date = '9999-12-31'",
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.account',
        'table_description': 'Individual investment accounts with historical tracking (SCD Type 2)',
        'columns': {
            'account_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_id': {
                'description': 'Natural business identifier for the account',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_key': {
                'description': 'Foreign key to household table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_type': {
                'description': 'Tax classification (Taxable, IRA, 401k, Trust, Custody)',
                'query_to_get_column_values': 'SELECT DISTINCT account_type FROM account ORDER BY account_type',
                'query_to_get_date_range': ''
            },
            'account_custodian': {
                'description': 'Firm holding the assets (Schwab, Fidelity, Pershing, etc.)',
                'query_to_get_column_values': 'SELECT DISTINCT account_custodian FROM account ORDER BY account_custodian',
                'query_to_get_date_range': ''
            },
            'opened_date': {
                'description': 'Date the account was opened',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_status': {
                'description': 'Current status (Open/Closed)',
                'query_to_get_column_values': 'SELECT DISTINCT account_status FROM account ORDER BY account_status',
                'query_to_get_date_range': ''
            },
            'closed_date': {
                'description': 'Date account was closed (if applicable)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_risk_profile': {
                'description': 'Investment risk tolerance (Conservative, Moderate, Aggressive)',
                'query_to_get_column_values': 'SELECT DISTINCT account_risk_profile FROM account ORDER BY account_risk_profile',
                'query_to_get_date_range': ''
            },
            'from_date': {
                'description': 'Effective start date for this record version',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'to_date': {
                'description': 'Effective end date for this record version',
                'values': "SELECT distinct concat('filter on to_date = ','''',to_date,'''',' to identify the most recent record of the account_id' ) as value_description FROM public.account where to_date = '9999-12-31'",
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.product',
        'table_description': 'Investment products and securities',
        'columns': {
            'product_id': {
                'description': 'Unique identifier for investment product (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'asset_category': {
                'description': 'High-level asset classification (Equity, Fixed Income, Multi-Asset, Cash)',
                'query_to_get_column_values': 'SELECT DISTINCT asset_category FROM product ORDER BY asset_category',
                'query_to_get_date_range': ''
            },
            'asset_subcategory': {
                'description': 'Detailed product classification within asset category',
                'query_to_get_column_values': 'SELECT DISTINCT asset_subcategory FROM product ORDER BY asset_subcategory',
                'query_to_get_date_range': ''
            },
            'product_line': {
                'description': 'Distribution channel or wrapper type',
                'query_to_get_column_values': 'SELECT DISTINCT product_line FROM product ORDER BY product_line',
                'query_to_get_date_range': ''
            },
            'product_name': {
                'description': 'Full name of the investment product',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.tier_fee',
        'table_description': 'Fee schedule based on asset levels by business line',
        'columns': {
            'tier_fee_id': {
                'description': 'Unique identifier (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'tier_min_aum': {
                'description': 'Minimum assets under management for this fee tier',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'tier_max_aum': {
                'description': 'Maximum assets under management for this fee tier',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'tier_fee_bps': {
                'description': 'Fee rate in basis points (e.g., 90 = 0.90%). Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.advisor_payout_rate',
        'table_description': 'Commission/payout rates by firm affiliation model',
        'columns': {
            'firm_affiliation_model': {
                'description': 'Type of firm affiliation (PRIMARY KEY). Source is table advisors, column firm_affiliation_model.',
                'query_to_get_column_values': 'SELECT DISTINCT firm_affiliation_model FROM advisor_payout_rate ORDER BY firm_affiliation_model',
                'query_to_get_date_range': ''
            },
            'advisor_payout_rate': {
                'description': 'Percentage of revenue paid to advisor (e.g., 0.7800 = 78%). Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.fact_account_monthly',
        'table_description': 'Monthly account performance and asset data',
        'columns': {
            'snapshot_date': {
                'description': 'End-of-month date for the data snapshot',
                'query_to_get_column_values': '',
                'query_to_get_date_range': "SELECT distinct concat ('account dates between ',min(snapshot_date),' and ', max(snapshot_date) ) FROM fact_account_monthly"
            },
            'account_key': {
                'description': 'Foreign key to account table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_monthly_return': {
                'description': 'Monthly investment return percentage. Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_net_flow': {
                'description': 'Net deposits/withdrawals during the month. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_assets_previous_month': {
                'description': 'Asset value at start of month. Semi-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_assets': {
                'description': 'Asset value at end of month. Semi-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table (current advisor)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_key': {
                'description': 'Foreign key to household table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.fact_account_product_monthly',
        'table_description': 'Monthly asset allocation by product for each account',
        'columns': {
            'snapshot_date': {
                'description': 'End-of-month date for the allocation snapshot',
                'query_to_get_column_values': '',
                'query_to_get_date_range': "SELECT distinct concat ('product account dates between ',min(snapshot_date),' and ', max(snapshot_date) ) FROM fact_account_product_monthly"
            },
            'account_key': {
                'description': 'Foreign key to account table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'product_id': {
                'description': 'Foreign key to product table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'product_allocation_pct': {
                'description': 'Percentage of account allocated to this product. Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.fact_household_monthly',
        'table_description': 'Monthly aggregated household data. This table aggregates fact_account_monthly at household level.',
        'columns': {
            'snapshot_date': {
                'description': 'End-of-month date for the data snapshot',
                'query_to_get_column_values': '',
                'query_to_get_date_range': "SELECT distinct concat ('household dates between ',min(snapshot_date),' and ', max(snapshot_date) ) FROM fact_household_monthly"
            },
            'household_key': {
                'description': 'Foreign key to household table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_assets': {
                'description': 'Total assets across all household accounts. Aggregation over table fact_account_monthly -> column account_assets. Semi-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'asset_range_bucket': {
                'description': 'Categorized asset range for segmentation',
                'query_to_get_column_values': 'SELECT DISTINCT asset_range_bucket FROM fact_household_monthly ORDER BY asset_range_bucket',
                'query_to_get_date_range': ''
            },
            'high_net_worth_flag': {
                'description': 'Boolean indicator for high-net-worth status',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_net_flow': {
                'description': 'Net deposits/withdrawals across all accounts. Aggregation of table fact_account_monthly -> column account_net_flow. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.fact_revenue_monthly',
        'table_description': 'Monthly fee and revenue calculations',
        'columns': {
            'snapshot_date': {
                'description': 'End-of-month date for revenue calculation',
                'query_to_get_column_values': '',
                'query_to_get_date_range': "SELECT distinct concat ('revenue dates between ',min(snapshot_date),' and ', max(snapshot_date) ) FROM fact_revenue_monthly"
            },
            'account_key': {
                'description': 'Foreign key to account table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'household_key': {
                'description': 'Foreign key to household table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'account_assets': {
                'description': 'Asset value used for fee calculation. Semi-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'fee_percentage': {
                'description': 'Annual fee rate applied to assets. Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'gross_fee_amount': {
                'description': 'Total fee charged before deductions. Equals to account_assets x fee_percentage. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'third_party_fee': {
                'description': 'Fees paid to external parties. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_payout_rate': {
                'description': 'Percentage of net revenue paid to advisor. Source is table advisor_payout_rate -> column advisor_payout_rate. Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_payout_amount': {
                'description': 'Dollar amount paid to advisor. Equals to (gross_fee_amount - third_party_fee) x advisor_payout_rate. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'net_revenue': {
                'description': 'Revenue retained by Capital Partners. Equals to gross_fee_amount - third_party_fee - advisor_payout_amount. Additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    },

    {
        'table_name': 'public.fact_customer_feedback',
        'table_description': 'Client satisfaction and feedback data',
        'columns': {
            'feedback_id': {
                'description': 'Unique feedback record identifier (PRIMARY KEY)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'feedback_date': {
                'description': 'Date feedback was collected',
                'query_to_get_column_values': '',
                'query_to_get_date_range': "SELECT distinct concat ('feedback dates between ',min(feedback_date),' and ', max(feedback_date) ) FROM fact_customer_feedback"
            },
            'household_key': {
                'description': 'Foreign key to household table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'feedback_text': {
                'description': 'Customer comments (max 200 characters)',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            },
            'satisfaction_score': {
                'description': 'Numeric satisfaction rating (0-100). Non-additive measure.',
                'query_to_get_column_values': '',
                'query_to_get_date_range': ''
            }
        }
    }
]

# Table relationships: respect the naming convention of schema_name.table_name.column_name. this structure is used for parsing
table_relationships = [
    {
        'key1': 'public.account.advisor_key',
        'key2': 'public.advisors.advisor_key'
    },
    {
        'key1': 'public.account.household_key',
        'key2': 'public.household.household_key'
    },
    {
        'key1': 'public.account.business_line_key',
        'key2': 'public.business_line.business_line_key'
    },
    {
        'key1': 'public.fact_account_monthly.account_key',
        'key2': 'public.account.account_key'
    },
    {
        'key1': 'public.fact_revenue_monthly.account_key',
        'key2': 'public.account.account_key'
    },
    {
        'key1': 'public.transactions.product_id',
        'key2': 'public.product.product_id'
    },
    {
        'key1': 'public.fact_account_monthly.snapshot_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.fact_account_product_monthly.snapshot_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.fact_household_monthly.snapshot_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.fact_revenue_monthly.snapshot_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.transactions.transaction_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.fact_customer_feedback.feedback_date',
        'key2': 'public.date.calendar_day'
    }
]
