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

selectAttributes = "prod, 1_sum_quant, 2_sum_quant, 3_sum_quant"
groupingVars = "3"
groupingAttributes = "prod"
aggregates = "1_sum_quant, 2_sum_quant, 3_sum_quant"
groupingPredicates = "1.month= 1,2.month = 2,3.month = 3"
havingCondition = "year = 2020"
mf_struct = {}
output = PrettyTable()
mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes)
print(output)
parsed_predicates = mf.parse_predicates(groupingPredicates)
mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes)
print(output)
mf.having_condition(mf_struct, havingCondition, selectAttributes, output)
print(output)