


userInput = input("Press enter for inline commands or write the name of the file you want to be read: ")
def parse_file(file_path):
    #in progress for working with any kind of file
    # I think he said we don't need to worry about error checking though
    with open(file_path, "r") as file:
        lines = file.readlines()
    
    selectAttributes = lines[1].strip()
    groupingVars = lines[3].strip()
    groupingAttributes = lines[5].strip()
    aggregates = lines[7].strip()
    predicates = lines[9].strip()
    havingCondition = lines[11].strip()
    
    return selectAttributes, groupingVars, groupingAttributes, aggregates, predicates, havingCondition

if userInput == "":
    #this works
    selectAttributes  = input("Please input the select attributes seperated by a comma. For example: product, sum_x_quant: ").replace(" ", "")
    groupingVars = input("Please input the number of grouping variables. For example: 2: ").replace(" ", "")
    groupingAttributes = input("Please input the grouping attribute seperated with commas. Fpr example: product, place: ").replace(" ", "")
    aggregates = input("Please input the list of aggregate functions for the query seperated by a comma. For example: sum_x_quant, sum_y_quant: ").replace(" ", "")
    predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. For example: x.month = 1, y.month = 2: ").replace(" ", "")
    havingCondition = input("Please input the having condition with each element seperated by spaces. Make sure the AND and OR written in lowercase. Idk an example for this: ")

else:
    selectAttributes, groupingVars, groupingAttributes, aggregates, predicates, havingCondition = parse_file(userInput)
    


print("Select Attributes: ", selectAttributes)
print("Grouping Variables: ", groupingVars)
print("Grouping Attributes: ", groupingAttributes)
print("Aggerates: ", aggregates)
print("Predicates: ", predicates)
print("Having Condition: ", havingCondition)
