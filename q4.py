import os
import psycopg2
import mf_struct as mf
from dotenv import load_dotenv
from prettytable import PrettyTable
from psycopg2.extras import DictCursor
load_dotenv(), 
conn = psycopg2.connect(
    dbname=os.getenv('DBNAME'),
    user='postgres',
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
)
cur = conn.cursor(cursor_factory=DictCursor)
cur.execute("SELECT * FROM sales")
sales_data = cur.fetchall()
cur.close()

selectAttributes = "prod, 1_sum_quant, 1_avg_quant, 1_max_quant"
groupingVars = "1"
groupingAttributes = "prod"
aggregates = "1_sum_quant, 1_avg_quant, 1_max_quant"
groupingPredicates = "1.year = 2017"
havingCondition = ""
mf_struct = {}
output = PrettyTable()
mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes)
print(output)
parsed_predicates = mf.parse_predicates(groupingPredicates)
mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes)
print(output)
mf.having_condition(mf_struct, havingCondition, selectAttributes, output)
print(output)