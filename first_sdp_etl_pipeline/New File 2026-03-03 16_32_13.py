from pyspark import pipelines as dp 
import ast 
tables_list = ["a", "b", "c"]

list_var_list = tables_list  # Use directly (not ast.literal_eval)
print(list_var_list)
