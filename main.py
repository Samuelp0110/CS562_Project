import os
import psycopg2
from dotenv import load_dotenv


#function to connect to database and populate it
def connectAndPopulate():
    load_dotenv()
    conn = psycopg2.connect(
            dbname=os.getenv('DBNAME'),
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
    )
    cur = conn.cursor()
    sales = open("sales.sql", "r")
    for i in sales:
        cur.execute(i)

def parse_file(file_path):
    #for when the user wants to use a file
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()
    
    
    groupingVars = ""
    selectAttributes = ""
    groupingAttributes = ""
    aggregates = ""
    groupingPredicates = ""
    havingCondition = ""
    #  iterate the lines of the input file, should be in the same order all the time 
    # since prof said we don't need to do error checking
    i = 0
    while i < len(lines):
        if lines[i] == "SELECT ATTRIBUTE(S):\n":
            selectAttributes = lines[i+1].replace("\n", "")
            i += 2
        elif lines[i] == "NUMBER OF GROUPING VARIABLES(n):\n":
            groupingVars = lines[i+1].replace("\n", "")
            i += 2
        elif lines[i] == "GROUPING ATTRIBUTES(V):\n":
            groupingAttributes = lines[i+1].replace("\n", "")
            i += 2
        elif lines[i] == "F-VECT([F]):\n":
            aggregates = lines[i+1].replace("\n", "")
            i +=2
        elif lines[i] == "SELECT CONDITION-VECT([σ]):\n":
            groupingPredicates = lines[i+1].replace("\n", "")
            i += 2
        elif lines[i] == "HAVING_CONDITION(G):\n":
            havingCondition = lines[i+1].replace("\n", "")
            break
        else:
            groupingPredicates += "," + lines[i].replace("\n", "")
            i += 1
        
    
    return selectAttributes, groupingVars, groupingAttributes, aggregates, groupingPredicates, havingCondition

#call the function to connect to the database and populate it
connectAndPopulate()
        
userInput = input("Press enter for inline commands or write the name of the file you want to be read: ")
if userInput == "":
    #for when the user wants to use the command line
    selectAttributes  = input("Please input the select attributes seperated by a comma. For example: product, sum_x_quant: ").replace(" ", "")
    groupingVars = input("Please input the number of grouping variables. For example: 2: ").replace(" ", "")
    groupingAttributes = input("Please input the grouping attribute seperated with commas. Fpr example: product, place: ").replace(" ", "")
    aggregates = input("Please input the list of aggregate functions for the query seperated by a comma. For example: sum_x_quant, sum_y_quant: ").replace(" ", "")
    groupingPredicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. For example: x.month = 1, y.month = 2: ").replace(" ", "")
    havingCondition = input("Please input the having condition with each element seperated by spaces. When using a AND or OR write it in lowercase. For example: state1 = state2 and state3 = state4: ").replace(" ", "")
    output_file = "_generated.py"  # Default name
else:
    selectAttributes, groupingVars, groupingAttributes, aggregates, groupingPredicates, havingCondition = parse_file(userInput)
    output_file = os.path.splitext(userInput)[0] + ".py"  # Use input file name
    

with open(output_file, "w") as file:
    # Write imports
    file.write("import os\n")
    file.write("import psycopg2\n")
    file.write("import mf_struct as mf\n")
    file.write("from dotenv import load_dotenv\n")
    file.write("from prettytable import PrettyTable\n")
    file.write("from psycopg2.extras import DictCursor\n")    

    # Write connection setup
    file.write("load_dotenv(), \n")
    file.write("conn = psycopg2.connect(\n")
    file.write("    dbname=os.getenv('DBNAME'),\n")
    file.write("    user='postgres',\n")
    file.write("    password=os.getenv('PASSWORD'),\n")
    file.write("    host=os.getenv('HOST'),\n")
    file.write(")\n")
    file.write("cur = conn.cursor(cursor_factory=DictCursor)\n")  # Use DictCursor here
    file.write("cur.execute(\"SELECT * FROM sales\")\n")
    file.write("sales_data = cur.fetchall()\n")
    file.write("cur.close()\n\n")

    file.write(f"selectAttributes = \"" + selectAttributes + "\"\n")
    file.write(f"groupingVars = \"" + groupingVars + "\"\n")
    file.write(f"groupingAttributes = \"" + groupingAttributes + "\"\n")
    file.write(f"aggregates = \"" + aggregates + "\"\n")
    file.write(f"groupingPredicates = \"" + groupingPredicates + "\"\n")
    file.write(f"havingCondition = \"" + havingCondition + "\"\n")

    file.write("mf_struct = {}\n")
    file.write("output = PrettyTable()\n")

    file.write("mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes)\n")
    file.write("print(output)\n")
    file.write("parsed_predicates = mf.parse_predicates(groupingPredicates)\n")
    file.write("mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes)\n")
    file.write("print(output)\n")
    file.write("mf.having_condition(mf_struct, havingCondition, selectAttributes, output)\n")
    file.write("print(output)")
