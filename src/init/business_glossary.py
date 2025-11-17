# Business Terms Glossary

key_terms = [
     {
        'name': 'Assets Under Management',
        'definition': 'Total market value of all client investments',
        'query_instructions': "Use account_assets from fact_account_monthly table. Filter to_date = '9999-12-31' to get current records. Aggregate at advisor, household, or business line level as needed.",
        'exists_in_database': True
    },

     {
        'name': 'Household',
        'definition': '',
        'query_instructions': "to get recent records, filter for household.household_status = 'Active' and household.to_date = '9999-12-31'",
        'exists_in_database': True
    } ,

         {
        'name': 'Advisor',
        'definition': '',
        'query_instructions': "to get recent records, filter for advisors.advisor_status = 'Active' and advisors.to_date = '9999-12-31'",
        'exists_in_database': True
    } ,

    {
        'name': 'Account',
        'definition': '',
        'query_instructions': "to get recent records, filter for account.account_status = 'Active' and account.to_date = '9999-12-31'",
        'exists_in_database': True
    } ,

     {
        'name': 'payout',
        'definition': 'Dollar amount paid to advisor.',
        'query_instructions': 'open accounts and to_date = 9999-12-31 to get most recent records only. for historical point in time, ',
        'exists_in_database': False
    } ,

     {
        'name': 'net revenue',
        'definition': 'Revenue retained by Capital Partners.',
        'query_instructions': '',
        'exists_in_database': True
    },

     {
        'name': 'compensation',
        'definition': 'placeholder.',
        'query_instructions': '',
        'exists_in_database': False
    },

     {
        'name': 'high net worth',
        'definition': 'If household_assets >= $1M.',
        'query_instructions': 'query fact_household_monthly for high_net_worth_flag = True',
        'exists_in_database': False
    }
]

# Synonyms - maps user terms to key_terms. Add the synonym to the left and the key term (defined) in the right
synonyms = {
    'aum': 'assets under management',
    'total assets': 'assets under management',
    'client': 'household',
    'hnw': 'high net worth',
    'production': 'payout',
}

# Related terms - terms that are conceptually related
related_terms = [
    ['compensation' ,'net revenue', 'payout']
]


def check_glossary_consistency():
    """
    Checks if all terms referenced in synonyms and related_terms exist in key_terms.
    Prints messages for missing terms.
    """
    # Get all term names from key_terms (normalized to lowercase for comparison)
    key_term_names = {term['name'].lower() for term in key_terms}

    missing_terms = set()

    # Check synonyms - the values (right side) should exist in key_terms
    for synonym, key_term in synonyms.items():
        key_term_normalized = key_term.replace('_', ' ').lower()
        if key_term_normalized not in key_term_names:
            missing_terms.add(key_term)

    # Check related_terms - all terms in the lists should exist in key_terms
    for term_group in related_terms:
        for term in term_group:
            term_normalized = term.replace('_', ' ').lower()
            if term_normalized not in key_term_names:
                missing_terms.add(term)

    # Print messages for missing terms
    if missing_terms:
        print("⚠️  Missing terms found in business_glossary:")
        print("="*80)
        for term in sorted(missing_terms):
            print(f"\nPlease add the following term to key_terms:")
            print(f"{{")
            print(f"    'name': '{term}',")
            print(f"    'definition': '<ADD DEFINITION HERE>',")
            print(f"    'query_instructions': '<ADD QUERY INSTRUCTIONS OR LEAVE BLANK IF NOT IN DATABASE>',")
            print(f"    'exists_in_database': <True or False>")
            print(f"}},")
        print("\n" + "="*80)
    else:
        print("✅ All terms in synonyms and related_terms exist in key_terms")

