# Database Schema Glossary
database_schema = [
    {
        'table_name': 'public.date',
        'table_description': 'Calendar dimension table for time-based analysis',
        'columns': {
            'calendar_day': {
                'description': 'Specific calendar date (PRIMARY KEY)'
            },
            'month_name': {
                'description': 'Full month name (January, February, etc.)'
            },
            'month': {
                'description': 'Month number (1-12)'
            },
            'day_of_month': {
                'description': 'Day within the month (1-31)'
            },
            'month_start_date': {
                'description': 'First day of the month'
            },
            'month_end_date': {
                'description': 'Last day of the month'
            },
            'quarter': {
                'description': 'Quarter number (1-4)'
            },
            'quarter_name': {
                'description': 'Quarter label (Q1, Q2, Q3, Q4)'
            },
            'quarter_start_date': {
                'description': 'First day of the quarter'
            },
            'quarter_end_date': {
                'description': 'Last day of the quarter'
            },
            'year': {
                'description': 'Four-digit year'
            },
            'is_weekend': {
                'description': 'Boolean flag indicating weekend days'
            }
        }
    },

    {
        'table_name': 'public.business_line',
        'table_description': 'Investment product service lines offered by Capital Partners to advisors',
        'columns': {
            'business_line_key': {
                'description': 'Unique identifier for Capital Partners business line (PRIMARY KEY)'
            },
            'business_line_name': {
                'description': 'Capital Partners product service category (Managed Portfolio, SMA, Mutual Fund Wrap, Annuity, Cash)',
                'column_values': 'Annuity | Cash | Managed Portfolio | Mutual Fund Wrap | Separately Managed Account'
            }
        }
    },

    {
        'table_name': 'public.advisors',
        'table_description': 'Financial advisors with historical tracking (SCD Type 2)',
        'columns': {
            'advisor_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)'
            },
            'advisor_id': {
                'description': 'Natural business identifier for the advisor'
            },
            'advisor_tenure': {
                'description': 'Years of experience as an advisor (1-40 years)'
            },
            'firm_name': {
                'description': 'Name of the firm the advisor is part of (NOT Capital Partners)'
            },
            'firm_affiliation_model': {
                'description': 'Business model of the advisor firm (RIA, Hybrid RIA, Broker-Dealer, etc.)',
                'column_values': 'Bank/Trust | Broker-Dealer W-2 | Bank/Trust | Broker-Dealer W-2 | Hybrid RIA | Independent BD | Insurance BD | RIA | Wirehouse'
            },
            'advisor_role': {
                'description': 'Position within the firm (Lead Advisor, Associate, etc.)',
                'column_values': 'Associate Advisor | Client Service Associate | Lead Advisor | Portfolio Manager | Relationship Manager'
            },
            'advisor_status': {
                'description': 'Current employment status (Active/Terminated)',
                'column_values': 'Active | Terminated'
            },
            'practice_segment': {
                'description': 'Size classification of advisory practice',
                'column_values': 'Ensemble | Enterprise | Small Team | Solo Practice'
            },
            'from_date': {
                'description': 'Effective start date for this record version'
            },
            'to_date': {
                'description': 'Effective end date for this record version'
            }
        }
    },

    {
        'table_name': 'public.household',
        'table_description': 'Client households with historical tracking (SCD Type 2)',
        'columns': {
            'household_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)'
            },
            'household_id': {
                'description': 'Natural business identifier for the household'
            },
            'household_tenure': {
                'description': 'Years as a client (1-40 years)'
            },
            'household_registration_type': {
                'description': 'Legal registration structure (Individual, Joint, Trust, Institutional)',
                'column_values': 'Individual | Institutional | Joint | Trust'
            },
            'household_registration_date': {
                'description': 'Date when household became a client'
            },
            'household_segment': {
                'description': 'Client service model classification',
                'column_values': 'Active Trader | Advice-Seeking | Business/Institutional | Discretionary Managed | Retirement Income'
            },
            'household_status': {
                'description': 'Current relationship status (Active/Terminated)',
                'column_values': 'Active | Terminated'
            },
            'household_advisor_id': {
                'description': 'ID of the primary advisor serving this household'
            },
            'from_date': {
                'description': 'Effective start date for this record version'
            },
            'to_date': {
                'description': 'Effective end date for this record version'
            }
        }
    },

    {
        'table_name': 'public.account',
        'table_description': 'Individual investment accounts with historical tracking (SCD Type 2)',
        'columns': {
            'account_key': {
                'description': 'Unique surrogate key (PRIMARY KEY)'
            },
            'account_id': {
                'description': 'Natural business identifier for the account'
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table'
            },
            'household_key': {
                'description': 'Foreign key to household table'
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table'
            },
            'account_type': {
                'description': 'Tax classification (Taxable, IRA, 401k, Trust, Custody)',
                'column_values': '401k | Custody | IRA | Taxable | Trust'
            },
            'account_custodian': {
                'description': 'Firm holding the assets (Schwab, Fidelity, Pershing, etc.)',
                'column_values': 'BankTrust | Fidelity | In-House | Pershing | Schwab'
            },
            'opened_date': {
                'description': 'Date the account was opened'
            },
            'account_status': {
                'description': 'Current status (Open/Closed)',
                'column_values': 'Closed | Open'
            },
            'closed_date': {
                'description': 'Date account was closed (if applicable)'
            },
            'account_risk_profile': {
                'description': 'Investment risk tolerance (Conservative, Moderate, Aggressive)',
                'column_values': 'Aggressive | Conservative | Moderate'
            },
            'from_date': {
                'description': 'Effective start date for this record version'
            },
            'to_date': {
                'description': 'Effective end date for this record version'
            }
        }
    },

    {
        'table_name': 'public.product',
        'table_description': 'Investment products and securities',
        'columns': {
            'product_id': {
                'description': 'Unique identifier for investment product (PRIMARY KEY)'
            },
            'asset_category': {
                'description': 'High-level asset classification (Equity, Fixed Income, Multi-Asset, Cash)',
                'column_values': 'Cash | Equity | Fixed Income | Multi-Asset'
            },
            'asset_subcategory': {
                'description': 'Detailed product classification within asset category',
                'column_values': 'Balanced Fund (60/40) | Common Stock | Equity Mutual Fund| Investment-Grade Corporate Bond | Money Market Fund | Municipal Bond | Preferred Stock | Target-Date Fund | U.S. Treasury Bill | U.S. Treasury Note'
            },
            'product_line': {
                'description': 'Distribution channel or wrapper type',
                'column_values': 'Annuity Contract | ETF | Money Market | Mutual Fund | Separately Managed Account Strategy'
            },
            'product_name': {
                'description': 'Full name of the investment product'
            }
        }
    },

    {
        'table_name': 'public.tier_fee',
        'table_description': 'Fee schedule based on asset levels by business line',
        'columns': {
            'tier_fee_id': {
                'description': 'Unique identifier (PRIMARY KEY)'
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table'
            },
            'tier_min_aum': {
                'description': 'Minimum assets under management for this fee tier'
            },
            'tier_max_aum': {
                'description': 'Maximum assets under management for this fee tier'
            },
            'tier_fee_bps': {
                'description': 'Fee rate in basis points (e.g., 90 = 0.90%). Non-additive measure.'
            }
        }
    },

    {
        'table_name': 'public.advisor_payout_rate',
        'table_description': 'Commission/payout rates by firm affiliation model',
        'columns': {
            'firm_affiliation_model': {
                'description': 'Type of firm affiliation (PRIMARY KEY). Source is table advisors, column firm_affiliation_model.',
                'column_values': 'Bank/Trust | Broker-Dealer W-2 | Hybrid RIA | Independent BD | Insurance BD | RIA | Wirehouse'
            },
            'advisor_payout_rate': {
                'description': 'Percentage of revenue paid to advisor (e.g., 0.7800 = 78%). Non-additive measure.'
            }
        }
    },

    {
        'table_name': 'public.fact_account_monthly',
        'table_description': 'Monthly account performance and asset data',
        'columns': {
            'snapshot_date': {
                'description': 'End-of-month date for the data snapshot'
            },
            'account_key': {
                'description': 'Foreign key to account table'
            },
            'account_monthly_return': {
                'description': 'Monthly investment return percentage. Non-additive measure.'
            },
            'account_net_flow': {
                'description': 'Net deposits/withdrawals during the month. Additive measure.'
            },
            'account_assets_previous_month': {
                'description': 'Asset value at start of month. Semi-additive measure.'
            },
            'account_assets': {
                'description': 'Asset value at end of month. Semi-additive measure.'
            },
            'advisor_key': {
                'description': 'Foreign key to advisors table (current advisor)'
            },
            'household_key': {
                'description': 'Foreign key to household table'
            },
            'business_line_key': {
                'description': 'Foreign key to business_line table'
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
        'key1': 'public.transactions.product_id',
        'key2': 'public.product.product_id'
    },
    {
        'key1': 'public.fact_account_monthly.snapshot_date',
        'key2': 'public.date.calendar_day'
    },
    {
        'key1': 'public.transactions.transaction_date',
        'key2': 'public.date.calendar_day'
    }
]
