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

selectAttributes = "cust, 1_sum_quant, 1_avg_quant, 2_sum_quant, 2_avg_quant, 3_sum_quant, 3_avg_quant"
groupingVars = "3"
groupingAttributes = "cust"
aggregates = "1_sum_quant, 1_avg_quant, 2_sum_quant, 2_avg_quant, 3_sum_quant, 3_avg_quant"
groupingPredicates = "1.state='NY' ,2.state='NJ',3.state='CT'"
havingCondition = "1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant"
mf_struct = {}
output = PrettyTable()
mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes)
print(output)
parsed_predicates = mf.parse_predicates(groupingPredicates)
mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes)
print(output)
mf.having_condition(mf_struct, havingCondition, selectAttributes, output)
print(output)