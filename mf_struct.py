# def initialize_mf_structure(sales_data, grouping_attributes, aggregates):
#     """
#     Initialize the MF-Structure based on grouping attributes and aggregate functions.
#     """
#     mf_structure = {}

#     for row in sales_data:
#         # Create a key from grouping attributes
#         key = tuple(row[attr] for attr in grouping_attributes)  # Use keys to access row data

#         if key not in mf_structure:
#             mf_structure[key] = {}
            
#             # Initialize grouping attributes
#             for attr in grouping_attributes:
#                 mf_structure[key][attr] = row[attr]

#             # Initialize aggregates
#             for agg in aggregates:
#                 parts = agg.split('_')
#                 if parts[1] == 'avg':  # Special case for averages
#                     mf_structure[key][agg] = {'sum': 0, 'count': 0, 'avg': 0}
#                 else:
#                     mf_structure[key][agg] = 0

#     return mf_structure


# def update_mf_structure_0th_pass(mf_structure, sales_data, grouping_attributes, aggregates):
#     """
#     Populate the MF-Structure during the 0th pass.
#     """
#     for row in sales_data:
#         # Create a key from grouping attributes
#         key = tuple(row[attr] for attr in grouping_attributes)  # Use keys to access row data

#         # Ensure key exists in the MF-Structure
#         if key not in mf_structure:
#             raise ValueError(f"Key {key} not found in MF-Structure.")

#         # Update aggregates
#         for agg in aggregates:
#             parts = agg.split('_')
#             group_var, func, col = parts[0], parts[1], parts[2]

#             if func == 'sum':
#                 mf_structure[key][agg] += row[col]
#             elif func == 'count':
#                 mf_structure[key][agg] += 1
#             elif func == 'avg':
#                 mf_structure[key][agg]['sum'] += row[col]
#                 mf_structure[key][agg]['count'] += 1
#                 mf_structure[key][agg]['avg'] = (
#                     mf_structure[key][agg]['sum'] / mf_structure[key][agg]['count']
#                 )

# def update_mf_structure_pass(mf_structure, sales_data, grouping_attributes, aggregates, predicates, pass_num):
#     """
#     Update the MF-Structure for subsequent passes in a generalized manner.

#     Args:
#     - mf_structure (dict): The current MF-Structure.
#     - sales_data (list of dict): Sales data rows as dictionaries.
#     - grouping_attributes (list of str): Attributes to group by.
#     - aggregates (list of str): Aggregates to compute.
#     - predicates (list of str): Conditions for each pass (e.g., "1.state=NY").
#     - pass_num (int): The current pass number.
#     """
#     predicate = predicates[pass_num - 1]  # Get the predicate for the current pass

#     for row in sales_data:
#         for key, values in mf_structure.items():
#             eval_string = predicate

#             # Replace group variables in the predicate
#             for attr in grouping_attributes:
#                 eval_string = eval_string.replace(f"{pass_num}.{attr}", f"'{values[attr]}'")

#             # Replace row variables in the predicate
#             for attr, value in row.items():
#                 if isinstance(value, str):
#                     value = f"'{value}'"  # Quote string values dynamically
#                 eval_string = eval_string.replace(f"{pass_num}.{attr}", str(value))

#             try:
#                 # Evaluate the predicate
#                 if eval(eval_string.replace("=", "==")):  # Ensure proper equality comparison
#                     for agg in aggregates:
#                         group_var, func, col = agg.split('_')

#                         if int(group_var) == pass_num:
#                             if func == 'sum':
#                                 values[agg] += row[col]
#                             elif func == 'count':
#                                 values[agg] += 1
#                             elif func == 'avg':
#                                 values[agg]['sum'] += row[col]
#                                 values[agg]['count'] += 1
#                                 values[agg]['avg'] = values[agg]['sum'] / values[agg]['count']
#             except Exception as e:
#                 print(f"Error evaluating predicate: {eval_string}\n{e}")


def initialize_mf_structure(sales_data, grouping_attributes, aggregates):
    """
    Initialize the MF-Structure based on grouping attributes and aggregate functions.
    """
    mf_structure = {}

    for row in sales_data:
        # Create a string key from grouping attributes
        key = "_".join(str(row[attr]) for attr in grouping_attributes)  # Use string keys

        if key not in mf_structure:
            mf_structure[key] = {}
            
            # Initialize grouping attributes
            for attr in grouping_attributes:
                mf_structure[key][attr] = row[attr]

            # Initialize aggregates
            for agg in aggregates:
                parts = agg.split('_')
                if parts[1] == 'avg':  # Special case for averages
                    mf_structure[key][agg] = {'sum': 0, 'count': 0, 'avg': 0}
                else:
                    mf_structure[key][agg] = 0

    return mf_structure


def update_mf_structure_0th_pass(mf_structure, sales_data, grouping_attributes, aggregates):
    """
    Populate the MF-Structure during the 0th pass.
    """
    for row in sales_data:
        # Create a string key from grouping attributes
        key = "_".join(str(row[attr]) for attr in grouping_attributes)  # Use string keys

        # Ensure key exists in the MF-Structure
        if key not in mf_structure:
            raise ValueError(f"Key {key} not found in MF-Structure.")

        # Update aggregates
        for agg in aggregates:
            parts = agg.split('_')
            group_var, func, col = parts[0], parts[1], parts[2]

            if func == 'sum':
                mf_structure[key][agg] += row[col]
            elif func == 'count':
                mf_structure[key][agg] += 1
            elif func == 'avg':
                mf_structure[key][agg]['sum'] += row[col]
                mf_structure[key][agg]['count'] += 1
                mf_structure[key][agg]['avg'] = (
                    mf_structure[key][agg]['sum'] / mf_structure[key][agg]['count']
                )

import re

def quote_unquoted_literals(eval_string):
    """
    Ensure that all unquoted string literals in the eval_string are properly quoted.

    Args:
    - eval_string (str): The string to process.

    Returns:
    - str: The string with proper quoting applied to string literals.
    """
    # Match unquoted words that are not Python operators or keywords
    tokens = re.findall(r"(?<!')\b[A-Za-z_]+\b(?!')", eval_string)
    python_keywords = {'and', 'or', 'not', 'in', 'is', 'True', 'False'}

    for token in tokens:
        if token not in python_keywords:  # Avoid quoting Python keywords
            eval_string = eval_string.replace(token, f"'{token}'")

    return eval_string

def update_mf_structure_pass(mf_structure, sales_data, grouping_attributes, aggregates, predicates, pass_num):
    """
    Update the MF-Structure for subsequent passes in a generalized manner.
    """
    predicate = predicates[pass_num - 1]  # Get the predicate for the current pass

    for row in sales_data:
        for key, values in mf_structure.items():
            eval_string = predicate

            # Replace group variables in the predicate
            for attr in grouping_attributes:
                eval_string = eval_string.replace(f"{pass_num}.{attr}", f"'{values[attr]}'")

            # Replace row variables in the predicate
            for attr, value in row.items():
                if isinstance(value, str) and not (value.startswith("'") and value.endswith("'")):
                    # Quote only if not already quoted
                    value = f"'{value}'"
                eval_string = eval_string.replace(f"{pass_num}.{attr}", str(value))

            try:
                # Evaluate the predicate
                if eval(eval_string.replace("=", "==")):  # Ensure proper equality comparison
                    for agg in aggregates:
                        group_var, func, col = agg.split('_')

                        if int(group_var) == pass_num:
                            if func == 'sum':
                                values[agg] += row[col]
                            elif func == 'count':
                                values[agg] += 1
                            elif func == 'avg':
                                values[agg]['sum'] += row[col]
                                values[agg]['count'] += 1
                                values[agg]['avg'] = values[agg]['sum'] / values[agg]['count']
            except Exception as e:
                print(f"Error evaluating predicate: {eval_string}\n{e}")




def apply_having_condition(mf_structure, having_condition):
    """
    Apply the HAVING condition to filter rows in the MF-Structure.
    Args:
    - mf_structure (dict): The MF-Structure after all passes.
    - having_condition (str): The HAVING condition as a string.

    Returns:
    - result (list of dict): Filtered rows that satisfy the HAVING condition.
    """
    result = []

    for key, values in mf_structure.items():
        eval_string = having_condition

        # Replace attributes in the HAVING condition with their respective values
        for attr, value in values.items():
            if isinstance(value, dict) and 'avg' in value:
                eval_string = eval_string.replace(attr, str(value['avg']))
            else:
                eval_string = eval_string.replace(attr, str(value))

        try:
            # Add spaces between operators and operands for safety
            eval_string = eval_string.replace(">", " > ").replace("<", " < ")
            eval_string = eval_string.replace(">=", " >= ").replace("<=", " <= ")
            eval_string = eval_string.replace("==", " == ").replace("!=", " != ")
            eval_string = eval_string.replace("and", " and ").replace("or", " or ")

            # Evaluate the HAVING condition
            if eval(eval_string):
                result.append(values)
        except Exception as e:
            print(f"Error evaluating HAVING condition: {eval_string}\n{e}")

    return result
