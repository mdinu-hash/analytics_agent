# Business Terms Glossary
from difflib import get_close_matches

key_terms = [
    # ========================= ASSETS =========================
     {
        'name': 'Assets Under Management',
        'definition': 'Total market value of all client investments',
        'query_instructions': '''Use account_assets from fact_account_monthly table. 
         Don't aggregate over time as this measure is semi-additive. 
         You can aggregate this measure at advisor, household, or business line level.''',
        'exists_in_database': True
    },
     {
        'name': 'advisory assets',
        'definition': 'Assets in Managed Portfolio and SMA business lines',
        'query_instructions': ''' Aggregate fact_account_monthly.account_assets filtered for business_line_name in ('Separately Managed Account','Managed Portfolio').
                                  Don't aggregate over time as this measure is semi-additive.''',
        'exists_in_database': True
    },
     {
        'name': 'liquid assets',
        'definition': 'assets easily converted to cash',
        'query_instructions': '',
        'exists_in_database': False
    },
     {
        'name': 'Household',
        'definition': 'business or individual with a contractual agreement with advisors',
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
        'name': 'high net worth',
        'definition': 'If household_assets >= $1M.',
        'query_instructions': 'query fact_household_monthly for high_net_worth_flag = True',
        'exists_in_database': False
    },
# ========================= REVENUE STATEMENT =========================

     {
        'name': 'payout',
        'definition': 'Dollar amount paid to advisor',
        'query_instructions': 'Sum of fact_revenue_monthly.advisor_payout_amount',
        'exists_in_database': True
    } ,

     {
        'name': 'net revenue',
        'definition': 'Revenue retained by Capital Partners',
        'query_instructions': 'Sum of fact_revenue_monthly.net_revenue',
        'exists_in_database': True
    },
     {
        'name': 'distribution',
        'definition': 'advisor payout after tech fees are deducted',
        'query_instructions': '',
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
    ['payment','net revenue', 'payout','distribution'],
    ['advisory assets','producing assets','liquid assets']
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
        print("✅ All terms in synonyms exist in key_terms")


def search_terms(user_question, key_terms, synonyms, related_terms):
    """
    Searches for key terms, synonyms, and related terms in a user question.
    Uses keyword lookup and fuzzy matching.

    Returns dict with:
        - key_terms: list of dicts - key_terms that exist in database (exists_in_database=True)
        - synonym: dict or None
          If 1 synonym found: {
            'searched_for': str - word/phrase from user question,
            'maps_to': dict - the key term dict it maps to,
            'definition': str - definition of searched_for from key_terms (empty string if not found)
          }
          If multiple synonyms found: {
            'matches': list of dicts - each dict has 'searched_for', 'maps_to', 'definition'
          }
        - related_terms: dict or None - {
            'searched_for': str - word/phrase from user question,
            'matches': list of dicts - related term dicts (1 or more),
            'definition': str - definition of searched_for from key_terms (empty string if not found)
          }
        - documentation: str - combined documentation string
          Format: "{synonym_word} is synonym with {key_term_name}" (one line per synonym) and/or
                  "{term_from_q} is related (similar but different) with: {rel1}, {rel2}"
          (multi-line if multiple terms, empty string if none)
        - term_substitutions: list - initialized as empty list, populated later by LLM
    """
    user_question_lower = user_question.lower()

    # Create lookup dictionaries for fast access
    key_terms_lookup = {term['name'].lower(): term for term in key_terms}

    # Initialize all return values
    key_terms_found = []
    synonym_docu = None
    related_term_searched_for = None  # Will be dict: {'name': str, 'exists_in_db': bool}
    related_term_exists_in_db = False
    related_terms_found = None
    related_terms_exists_in_db = False
    related_terms_docu = None

    # Track all synonym matches (can be multiple)
    all_synonym_matches = []
    # Track all related terms matches (can be multiple)
    all_related_matches = []

    # 1. Check for direct key terms (keyword and fuzzy match)
    for term in key_terms:
        term_name = term['name'].lower()

        # Keyword lookup (exact substring match)
        if term_name in user_question_lower:
            if term.get('exists_in_database', False):
                key_terms_found.append(term)
            continue

        # Fuzzy match on the complete phrase
        term_words = term_name.split()
        if len(term_words) > 1:
            # For multi-word terms, try to find the complete phrase with fuzzy matching
            words = user_question_lower.split()
            for i in range(len(words) - len(term_words) + 1):
                phrase = ' '.join(words[i:i+len(term_words)])
                if get_close_matches(term_name, [phrase], n=1, cutoff=0.85):
                    if term.get('exists_in_database', False):
                        key_terms_found.append(term)
                    break
        else:
            # Single word term - fuzzy match directly
            words = user_question_lower.split()
            if get_close_matches(term_name, words, n=1, cutoff=0.85):
                if term.get('exists_in_database', False):
                    key_terms_found.append(term)

    # 2. Check for synonyms (process ALL matches, not just first)
    for syn_word, key_term_ref in synonyms.items():
        syn_word_lower = syn_word.lower()
        found_synonym = False

        # Keyword lookup
        if syn_word_lower in user_question_lower:
            found_synonym = True
        else:
            # Fuzzy match for synonym (complete phrase)
            syn_words = syn_word_lower.split()
            if len(syn_words) > 1:
                # Multi-word synonym
                words = user_question_lower.split()
                for i in range(len(words) - len(syn_words) + 1):
                    phrase = ' '.join(words[i:i+len(syn_words)])
                    if get_close_matches(syn_word_lower, [phrase], n=1, cutoff=0.85):
                        found_synonym = True
                        break
            else:
                # Single word synonym
                words = user_question_lower.split()
                if get_close_matches(syn_word_lower, words, n=1, cutoff=0.85):
                    found_synonym = True

        if found_synonym:
            # Look up the actual key term
            key_term_normalized = key_term_ref.replace('_', ' ').lower()
            if key_term_normalized in key_terms_lookup:
                actual_term = key_terms_lookup[key_term_normalized]

                # Check if synonym exists in database
                if actual_term.get('exists_in_database', False):
                    # Check if the synonym word itself exists in DB as a key term and get definition
                    syn_word_exists_in_db = False
                    syn_word_definition = ''
                    if syn_word.lower() in key_terms_lookup:
                        syn_word_exists_in_db = key_terms_lookup[syn_word.lower()].get('exists_in_database', False)
                        syn_word_definition = key_terms_lookup[syn_word.lower()].get('definition', '')

                    # Store this synonym match
                    all_synonym_matches.append({
                        'searched_for': syn_word,
                        'maps_to': actual_term,
                        'definition': syn_word_definition
                    })

                    # Add to key_terms_found
                    if actual_term not in key_terms_found:
                        key_terms_found.append(actual_term)

                    # Continue to check other synonyms (don't break)

    # 3. Check for related terms (process ALL matches, not just first)
    for term_group in related_terms:
        found_term_in_group = None
        found_term_obj = None
        found_term_from_question = None  # Track the actual word/phrase from user question

        for related_term in term_group:
            related_term_lower = related_term.replace('_', ' ').lower()

            # Keyword lookup
            if related_term_lower in user_question_lower:
                found_term_in_group = related_term_lower
                found_term_from_question = related_term  # Use original casing from glossary
                if related_term_lower in key_terms_lookup:
                    found_term_obj = key_terms_lookup[related_term_lower]
                break

            # Fuzzy match (complete phrase)
            related_words = related_term_lower.split()
            if len(related_words) > 1:
                # Multi-word related term
                words = user_question_lower.split()
                for i in range(len(words) - len(related_words) + 1):
                    phrase = ' '.join(words[i:i+len(related_words)])
                    if get_close_matches(related_term_lower, [phrase], n=1, cutoff=0.85):
                        found_term_in_group = related_term_lower
                        found_term_from_question = related_term  # Use original casing from glossary
                        if related_term_lower in key_terms_lookup:
                            found_term_obj = key_terms_lookup[related_term_lower]
                        break
                if found_term_in_group:
                    break
            else:
                # Single word related term
                words = user_question_lower.split()
                if get_close_matches(related_term_lower, words, n=1, cutoff=0.85):
                    found_term_in_group = related_term_lower
                    found_term_from_question = related_term  # Use original casing from glossary
                    if related_term_lower in key_terms_lookup:
                        found_term_obj = key_terms_lookup[related_term_lower]
                    break

        # If we found a term in this group, collect all OTHER related terms that exist in DB
        if found_term_in_group:
            related_terms_in_db = []
            for group_term in term_group:
                group_term_normalized = group_term.replace('_', ' ').lower()
                # Only add if it's NOT the term we found
                if group_term_normalized != found_term_in_group:
                    if group_term_normalized in key_terms_lookup:
                        term_data = key_terms_lookup[group_term_normalized]
                        # Only include if exists in database
                        if term_data.get('exists_in_database', False):
                            related_terms_in_db.append(term_data)
                            # Add to key_terms_found
                            if term_data not in key_terms_found:
                                key_terms_found.append(term_data)

            # Store this match
            if related_terms_in_db:
                all_related_matches.append({
                    'term_from_question': found_term_from_question,
                    'related_terms_in_db': related_terms_in_db
                })

            # Continue to check other term groups (don't break)

    # Now process all related term matches
    if all_related_matches:
        # Combine counts from all matches
        total_related_count = sum(len(match['related_terms_in_db']) for match in all_related_matches)

        if total_related_count == 1:
            # Exactly 1 related term found across all matches
            related_term_exists_in_db = True
            term_from_q = all_related_matches[0]['term_from_question']
            related_terms_found = all_related_matches[0]['related_terms_in_db'][0]  # Single dict

            # Check if the related term word itself exists in DB as a key term and get definition
            term_exists_in_db = False
            term_definition = ''
            if term_from_q.lower() in key_terms_lookup:
                term_exists_in_db = key_terms_lookup[term_from_q.lower()].get('exists_in_database', False)
                term_definition = key_terms_lookup[term_from_q.lower()].get('definition', '')

            related_term_searched_for = {'name': term_from_q, 'exists_in_db': term_exists_in_db, 'definition': term_definition}

            # Create related_terms_docu
            rel_name = all_related_matches[0]['related_terms_in_db'][0].get('name', '')
            related_terms_docu = f"{term_from_q} is related (similar but different) with: {rel_name}"

        elif total_related_count > 1:
            # Multiple related terms found
            related_terms_exists_in_db = True

            # Collect all related terms
            all_related_terms = []
            for match in all_related_matches:
                all_related_terms.extend(match['related_terms_in_db'])
            related_terms_found = all_related_terms  # List of dicts

            # For related_term_searched_for, use first match
            term_from_q = all_related_matches[0]['term_from_question']

            # Check if the related term word itself exists in DB as a key term and get definition
            term_exists_in_db = False
            term_definition = ''
            if term_from_q.lower() in key_terms_lookup:
                term_exists_in_db = key_terms_lookup[term_from_q.lower()].get('exists_in_database', False)
                term_definition = key_terms_lookup[term_from_q.lower()].get('definition', '')

            related_term_searched_for = {'name': term_from_q, 'exists_in_db': term_exists_in_db, 'definition': term_definition}

            # Create related_terms_docu with multiple lines if multiple terms from question
            docu_lines = []
            for match in all_related_matches:
                term_from_q = match['term_from_question']
                rel_names = [t.get('name', '') for t in match['related_terms_in_db']]
                docu_lines.append(f"{term_from_q} is related (similar but different) with: {', '.join(rel_names)}")
            related_terms_docu = '\n'.join(docu_lines)

    # Build synonym documentation from all matches
    synonym_docu_parts = []
    for syn_match in all_synonym_matches:
        searched_for = syn_match['searched_for']
        maps_to_name = syn_match['maps_to'].get('name', '')
        synonym_docu_parts.append(f"{searched_for} is synonym with {maps_to_name}")
    synonym_docu = '\n'.join(synonym_docu_parts) if synonym_docu_parts else ''

    # Build documentation by combining synonym_docu and related_terms_docu
    docu_parts = []
    if synonym_docu:
        docu_parts.append(synonym_docu)
    if related_terms_docu:
        docu_parts.append(related_terms_docu)

    documentation = '\n'.join(docu_parts) if docu_parts else ''

    # Build simplified synonym structure
    synonym_result = None
    if all_synonym_matches:
        if len(all_synonym_matches) == 1:
            # Single synonym - return as single dict for backward compatibility
            synonym_result = all_synonym_matches[0]
        else:
            # Multiple synonyms - return as dict with matches list
            synonym_result = {
                'matches': all_synonym_matches
            }

    # Build simplified related_terms structure
    related_terms_result = None
    if related_terms_found and related_term_searched_for:
        # related_terms_found can be a single dict or a list of dicts
        matches = related_terms_found if isinstance(related_terms_found, list) else [related_terms_found]
        related_terms_result = {
            'searched_for': related_term_searched_for['name'],
            'matches': matches,
            'definition': related_term_searched_for.get('definition', '')
        }

    return {
        'key_terms': key_terms_found,
        'synonym': synonym_result,
        'related_terms': related_terms_result,
        'documentation': documentation,
        'term_substitutions': []  # Initialized empty, populated later by LLM
    }

