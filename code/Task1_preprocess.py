import pandas as pd 

# read csv file 

filename_input = "" # this is file name 
filename_output = ""

csv = pd.read_csv(filename_input)

# remove newline character in each column
csv = csv.replace("\n|\r", "", regex=True)

# write to new file 
csv.to_csv(filename_output,header=False,index=False)
