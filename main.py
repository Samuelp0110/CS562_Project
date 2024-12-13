import os
import psycopg2
from dotenv import load_dotenv


#connect to database
load_dotenv()
conn = psycopg2.connect(
        dbname=os.getenv('DBNAME'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
)
cur = conn.cursor()

#populate the database
#this needs to be done but simple, copy from canvas

#get user input for the phi operator info
userInput = input("Press enter for inline commands or write the name of the file you want to be read: ")
def parse_file(file_path):
    #for when the user wants to use a file
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()
    
    #iterate the lines
    groupingVars = ""
    selectAttributes = ""
    groupingAttributes = ""
    aggregates = ""
    groupingPredicates = ""
    havingCondition = ""
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

if userInput == "":
    #for when the user wants to use the command line
    selectAttributes  = input("Please input the select attributes seperated by a comma. For example: product, sum_x_quant: ").replace(" ", "")
    groupingVars = input("Please input the number of grouping variables. For example: 2: ").replace(" ", "")
    groupingAttributes = input("Please input the grouping attribute seperated with commas. Fpr example: product, place: ").replace(" ", "")
    aggregates = input("Please input the list of aggregate functions for the query seperated by a comma. For example: sum_x_quant, sum_y_quant: ").replace(" ", "")
    groupingPredicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. For example: x.month = 1, y.month = 2: ").replace(" ", "")
    havingCondition = input("Please input the having condition with each element seperated by spaces. When using a AND or OR write it in lowercase. For example: state1 = state2 and state3 = state4: ").replace(" ", "")

else:
    selectAttributes, groupingVars, groupingAttributes, aggregates, groupingPredicates, havingCondition = parse_file(userInput)
    

with open("_generated.py", "w") as file:
    #write the code to the file to run the query 
    #do the imports and call the generator function in the generator file
    #pretty sure we need to do a select * from sales to do the query
    file.write("import os\n")
    file.write("import psycopg2\n")

# print("Select Attributes: ", selectAttributes)
# print("Grouping Variables: ", groupingVars)
# print("Grouping Attributes: ", groupingAttributes)
# print("Aggerates: ", aggregates)
# print("Predicates: ", groupingPredicates)
# print("Having Condition: ", havingCondition)
