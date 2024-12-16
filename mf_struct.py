def initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes):
    output.field_names = [attr.strip() for attr in selectAttributes.split(',')]
    
    for row in sales_data:
        key = ''
        value = {}
        for attr in groupingAttributes.split(','):
            key += f'{str(row[attr])},'
        key = key[:-1]
        if key not in mf_struct.keys():
            groupingattrib_init(groupingAttributes, row, value)
            fVect_init(aggregates, value)
            # print(f"Initialized key: {key} with value: {value}")
            mf_struct[key] = value

    output.clear_rows()
    for row_key, row_data in mf_struct.items():
        row_info = []
        for attr in selectAttributes.split(','):
            attr = attr.strip()
            if len(attr.split('_')) > 1 and attr.split('_')[1] == 'avg':
                row_info.append(str(row_data.get(attr, {}).get('avg', 0)))
            else:
                row_info.append(str(row_data.get(attr, 0)))
        output.add_row(row_info)


def groupingattrib_init(groupingAttributes, row, value):
    for groupAttrib in groupingAttributes.split(','):
        colVal = row[groupAttrib]
        if colVal:
            value[groupAttrib] = colVal

def fVect_init(aggregates, value):
    for agg in [x.strip() for x in aggregates.split(',')]:
        parts = agg.split('_')
        if len(parts) < 2:
            print(f"Invalid aggregate format: {agg}")
            continue
        agg_type = parts[1]
        if agg_type == 'avg':
            value[agg] = {'sum': 0, 'count': 0, 'avg': 0}
        elif agg_type in ['min', 'max']:
            value[agg] = float('-inf') if agg_type == 'max' else float('inf')
        else:
            value[agg] = 0
        # print(f"Initialized aggregate {agg} with value {value[agg]}")



def update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes):
    for i in range(1, int(groupingVars) + 1):
        print(f"Processing grouping variable {i}")
        for agg in [x.strip() for x in aggregates.split(',')]:
            aggList = agg.split('_')
            if len(aggList) != 3:
                raise ValueError(f"Invalid aggregate format: {agg}")
            groupVar, aggregateType, aggregateAttr = aggList
            if i == int(groupVar):
                for row in sales_data:
                    key = ''
                    for attr in groupingAttributes.split(','):
                        key += f'{str(row[attr])},'
                    key = key[:-1]
                    # print(f"Row: {row}, Key: {key}, Aggregate: {agg}")
                    if aggregateType == 'avg':
                        func_avg(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i)
                    elif aggregateType == 'count':
                        func_count(agg, parsed_predicates, row, mf_struct, key, i)
                    elif aggregateType == 'min':
                        func_min(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i)
                    elif aggregateType == 'max':
                        func_max(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i)
                    elif aggregateType == 'sum':
                        func_sum(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i)
    output.clear_rows()
    for row_key, row_data in mf_struct.items():
        row_info = []
        for attr in selectAttributes.split(','):
            attr = attr.strip()
            if len(attr.split('_')) > 1 and attr.split('_')[1] == 'avg':
                row_info.append(str(row_data.get(attr, {}).get('avg', 0)))
            else:
                row_info.append(str(row_data.get(attr, 0)))
        output.add_row(row_info)

def func_avg(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i):
    for parsed_predicate in parsed_predicates:
        if evaluate_predicate(parsed_predicate, row, i):
            sum = mf_struct[key][agg]['sum']
            count = mf_struct[key][agg]['count']
            sum += int(row[aggregateAttr])
            count += 1
            if count != 0:
                mf_struct[key][agg] = {'sum': sum, 'count': count, 'avg': (sum / count)}


def func_min(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i):
    for parsed_predicate in parsed_predicates:
        if evaluate_predicate(parsed_predicate, row, i):
            current_min = mf_struct[key][agg]
            row_value = int(row[aggregateAttr])
            if row_value < current_min:
                mf_struct[key][agg] = row_value

def func_max(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i):
    for parsed_predicate in parsed_predicates:
        if evaluate_predicate(parsed_predicate, row, i):
            current_max = mf_struct[key][agg]
            row_value = int(row[aggregateAttr])
            if row_value > current_max:
                mf_struct[key][agg] = row_value


def func_sum(agg, parsed_predicates, row, mf_struct, key, aggregateAttr, i):
    for parsed_predicate in parsed_predicates:
        if evaluate_predicate(parsed_predicate, row, i):
            sum = int(row[aggregateAttr])
            mf_struct[key][agg] += sum


def func_count(agg, parsed_predicates, row, mf_struct, key, i):
    for parsed_predicate in parsed_predicates:
        if evaluate_predicate(parsed_predicate, row, i):
            mf_struct[key][agg] += 1


def parse_predicates(groupingPredicates):
    predicates = groupingPredicates.split(',')
    parsed_predicates = []
    for pred in predicates:
        parts = pred.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid predicate format: {pred}")
        group_var, rest = parts
        if '=' in rest:
            column, value = rest.split('=')
            operator = '=='
        elif '!=' in rest:
            column, value = rest.split('!=')
            operator = '!='
        elif '<=' in rest:
            column, value = rest.split('<=')
            operator = '<='
        elif '>=' in rest:
            column, value = rest.split('>=')
            operator = '>='
        elif '<' in rest:
            column, value = rest.split('<')
            operator = '<'
        elif '>' in rest:
            column, value = rest.split('>')
            operator = '>'
        else:
            raise ValueError(f"Unsupported operator in predicate: {rest}")
        # Add quotes only if value is not already quoted
        value = value.strip()
        if not (value.startswith("'") and value.endswith("'")):
            value = f"'{value}'"
        parsed_predicates.append((group_var.strip(), column.strip(), operator, value))
    return parsed_predicates




def evaluate_predicate(parsed_predicate, row, i):
    group_var, column, operator, value = parsed_predicate
    if str(i) != group_var:
        return False
    row_value = row[column]
    # Construct the condition without introducing double quotes
    condition = f"'{row_value}' {operator} {value}"
    # print(f"Evaluating condition: {condition}")
    try:
        result = eval(condition)
        # print(f"Result: {result}")
        return result
    except Exception as e:
        print(f"Error evaluating predicate: {condition}\n{e}")
        return False






def having_condition(mf_struct, havingCondition, selectAttributes, output):
    for row in mf_struct:
        evalString = ''
        if havingCondition != '':
            for string in havingCondition.split(' '):
                if string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
                    try:
                        int(string)
                        evalString += f' {string} '
                    except:
                        string = string.strip()  # Ensure string matches the stripped keys
                        if len(string.split('_')) > 1 and string.split('_')[1] == 'avg':
                            evalString += f' {mf_struct[row][string].get("avg", 0)} '
                        else:
                            evalString += f' {mf_struct[row].get(string, 0)} '
                else:
                    evalString += f' {string} '
            try:
                print(f"Evaluating HAVING condition: {evalString}")
                if eval(evalString.replace('=', '==')):
                    row_info = []
                    for val in selectAttributes.split(','):
                        val = val.strip()
                        if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
                            row_info += [str(mf_struct[row][val].get('avg', 0))]
                        else:
                            row_info += [str(mf_struct[row].get(val, 0))]
                    output.add_row(row_info)
            except Exception as e:
                print(f"Error evaluating HAVING condition for row {row}: {e}")


# userInput = input("Press enter for inline commands or write the name of the file you want to be read: ")
# if userInput == "":
#     #for when the user wants to use the command line
#     selectAttributes  = input("Please input the select attributes seperated by a comma. For example: product, sum_x_quant: ").replace(" ", "")
#     groupingVars = input("Please input the number of grouping variables. For example: 2: ").replace(" ", "")
#     groupingAttributes = input("Please input the grouping attribute seperated with commas. Fpr example: product, place: ").replace(" ", "")
#     aggregates = input("Please input the list of aggregate functions for the query seperated by a comma. For example: sum_x_quant, sum_y_quant: ").replace(" ", "")
#     groupingPredicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. For example: x.month = 1, y.month = 2: ").replace(" ", "")
#     havingCondition = input("Please input the having condition with each element seperated by spaces. When using a AND or OR write it in lowercase. For example: state1 = state2 and state3 = state4: ").replace(" ", "")
#     output_file = "_generated.py"  # Default name
# else:
#     selectAttributes, groupingVars, groupingAttributes, aggregates, groupingPredicates, havingCondition = parse_file(userInput)
#     output_file = os.path.splitext(userInput)[0] + ".py"  # Use input file name
    

# with open(output_file, "w") as file:
#     # Write imports
#     file.write("import os\n")
#     file.write("import psycopg2\n")
#     file.write("import mf_struct as mf\n")
#     file.write("from dotenv import load_dotenv\n")
#     file.write("from prettytable import PrettyTable\n")
#     file.write("from psycopg2.extras import DictCursor\n")    

#     # Write connection setup
#     file.write("load_dotenv(), \n")
#     file.write("conn = psycopg2.connect(\n")
#     file.write("    dbname=os.getenv('DBNAME'),\n")
#     file.write("    user='postgres',\n")
#     file.write("    password=os.getenv('PASSWORD'),\n")
#     file.write("    host=os.getenv('HOST'),\n")
#     file.write(")\n")
#     file.write("cur = conn.cursor(cursor_factory=DictCursor)\n")  # Use DictCursor here
#     file.write("cur.execute(\"SELECT * FROM sales\")\n")
#     file.write("sales_data = cur.fetchall()\n")
#     file.write("cur.close()\n\n")

#     file.write(f"selectAttributes = \"" + selectAttributes + "\"\n")
#     file.write(f"groupingVars = \"" + groupingVars + "\"\n")
#     file.write(f"groupingAttributes = \"" + groupingAttributes + "\"\n")
#     file.write(f"aggregates = \"" + aggregates + "\"\n")
#     file.write(f"groupingPredicates = \"" + groupingPredicates + "\"\n")
#     file.write(f"havingCondition = \"" + havingCondition + "\"\n")

#     file.write("mf_struct = {}\n")
#     file.write("output = PrettyTable()\n")

#     file.write("mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data, output, selectAttributes)\n")
#     file.write("print(output)\n")
#     file.write("parsed_predicates = mf.parse_predicates(groupingPredicates)\n")
#     file.write("mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data, output, selectAttributes)\n")
#     file.write("print(output)\n")
#     file.write("mf.having_condition(mf_struct, havingCondition, selectAttributes, output)\n")
#     file.write("print(output)")