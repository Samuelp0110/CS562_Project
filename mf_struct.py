def initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data):
    for row in sales_data:
        key = ''
        value = {}
        for attr in groupingAttributes.split(','):
            key += f'{str(row[attr])},'
        key = key[:-1]
        if key not in mf_struct.keys():
            groupingattrib_init(groupingAttributes, row, value)
            fVect_init(aggregates, value)
            mf_struct[key] = value

def groupingattrib_init(groupingAttributes, row, value):
    for groupAttrib in groupingAttributes.split(','):
        colVal = row[groupAttrib]
        if colVal:
            value[groupAttrib] = colVal

def fVect_init(aggregates, value):
    for agg in [x.strip() for x in aggregates.split(',')]:  # Trim whitespace
        if (agg.split('_')[1] == 'avg'):
            # Average is saved as an object with the sum, count, and overall average
            value[agg] = {'sum': 0, 'count': 0, 'avg': 0}
        elif (agg.split('_')[1] == 'min'):
            # Initialize min with a high value (or use None if comparing dynamically)
            value[agg] = float('inf')  # Dynamically handle max values later
        elif (agg.split('_')[1] == 'max'):
            # Initialize max with a low value (or use None if comparing dynamically)
            value[agg] = float('-inf')  # Dynamically handle min values later
        else:
            # Initialize other aggregate types (e.g., sum, count) to 0
            value[agg] = 0



def update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data):
    for i in range(1, int(groupingVars) + 1):  # Correctly loop for grouping variable count
        for agg in [x.strip() for x in aggregates.split(',')]:  # Trim whitespace in aggregates
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
        parsed_predicates.append((group_var, column.strip(), operator, value.strip()))
    return parsed_predicates

def evaluate_predicate(parsed_predicate, row, i):
    group_var, column, operator, value = parsed_predicate
    if str(i) != group_var:
        return False
    row_value = row[column]
    # Build and evaluate the condition
    condition = f"'{row_value}' {operator} '{value}'"
    try:
        return eval(condition)
    except Exception as e:
        print(f"Error evaluating predicate: {condition}\n{e}")
        return False

    # file.write(f"selectAttributes = \"" + selectAttributes + "\"\n")
    # file.write(f"groupingVars = \"" + groupingVars + "\"\n")
    # file.write(f"groupingAttributes = \"" + groupingAttributes + "\"\n")
    # file.write(f"aggregates = \"" + aggregates + "\"\n")
    # file.write(f"groupingPredicates = \"" + groupingPredicates + "\"\n")
    # file.write(f"havingCondition = \"" + havingCondition + "\"\n")

    # file.write("mf_struct = {}\n")

    # file.write("mf.initialize_mf(groupingAttributes, aggregates, mf_struct, sales_data)\n")
    # file.write("print(mf_struct)\n")
    # file.write("parsed_predicates = mf.parse_predicates(groupingPredicates)\n")
    # file.write("mf.update_mf(groupingAttributes, groupingVars, aggregates, mf_struct, parsed_predicates, sales_data)\n")
    # file.write("print(mf_struct)\n")