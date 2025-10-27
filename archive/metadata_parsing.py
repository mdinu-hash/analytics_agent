# Archived metadata parsing functions
# These functions were removed from the main codebase as they are no longer used

import sqlglot
from sqlglot import parse_one, exp

def extract_metadata_from_sql_query(sql_query):
   # returns a dictionary with parsed names of tables and columns used in filters, aggregations and groupings

 ast = parse_one(sql_query)

 sql_query_metadata = {
    "tables": [],
    "filters": [],
    "aggregations": [],
    "groupings": []
 }

 # extract tables
 table_generator = ast.find_all(sqlglot.expressions.Table)
 for items in table_generator:
    sql_query_metadata['tables'].append(items.sql())
 # remove dups
 sql_query_metadata['tables'] = list(dict.fromkeys(sql_query_metadata['tables']))

 # extract filters
 where_conditions = ast.find_all(sqlglot.expressions.Where)
 for item in where_conditions:
  sql_query_metadata['filters'].append(item.this.sql())
  # remove dups
 sql_query_metadata['filters'] = list(dict.fromkeys(sql_query_metadata['filters']))

 # extract aggregate functions
 funcs = ast.find_all(sqlglot.expressions.AggFunc)
 for item in funcs:
  sql_query_metadata['aggregations'].append(item.sql())
 # remove dups
 sql_query_metadata['aggregations'] = list(dict.fromkeys(sql_query_metadata['aggregations']))

 # extract groupings
 groupings = ast.find_all(sqlglot.expressions.Group)
 for item in groupings:
  groupings_flattened = item.flatten()
  for item in groupings_flattened:
    sql_query_metadata['groupings'].append(item.sql())
 # remove dups
 sql_query_metadata['groupings'] = list(dict.fromkeys(sql_query_metadata['groupings']))

 return {'tables':sql_query_metadata.get('tables'),
         'filters':sql_query_metadata.get('filters'),
         'aggregations':sql_query_metadata.get('aggregations'),
         'groupings':sql_query_metadata.get('groupings'),
          }

def format_sql_metadata_explanation(tables:list=None, filters:list=None, aggregations:list=None, groupings:list=None,header :str='') -> str:
    # creates a string explanation of the filters, tables, aggregations and groupings used by the query
    explanation = header

    if tables:
        explanation += "\n\n🧊 Tables: • " + " • ".join(tables)
    if filters:
        explanation += "\n\n🔍 Filters applied: • " + " • ".join(filters)
    if aggregations:
        explanation += "\n\n🧮 Aggregations: • " + " • ".join(aggregations)
    if groupings:
        explanation += "\n\n📦 Groupings: • " + " • ".join(groupings)

    return explanation.strip()

def create_query_metadata(sql_query: str):
 """ creates an explanation for one single query """

 metadata = extract_metadata_from_sql_query(sql_query)
 return format_sql_metadata_explanation(metadata['tables'],metadata['filters'],metadata['aggregations'],metadata['groupings'])


def create_queries_metadata(sql_queries: list[dict]):
 """ creates an explanation for multiple queries: used in the generate_answer tool """

 all_tables = []
 all_filters = []

 for q in sql_queries:

  metadata = extract_metadata_from_sql_query(q['query'])
  all_tables.extend(metadata["tables"])
  all_filters.extend(metadata["filters"])

  # include the default min/max feedback filters if feedback table has been used and was not filtered at all
  if all_filters:
     output = format_sql_metadata_explanation(filters = all_filters,header='')
  # if no filters were applied, don't include other metadata for the sake of keeping the message simple
  else:
     output = ''

 return output
