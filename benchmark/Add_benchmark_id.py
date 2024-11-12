
import csv
import datetime

# Import pandas
import pandas as pd


def outputfile(output_file_name, data,read_mode='a'):
    with open(f'./benchmark/{output_file_name}.csv', read_mode, newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(data)
        
def change_split_add_id(input_file_name,output_file_name,tree_type):

    file = open(f'./benchmark/{input_file_name}.csv', "r") 
    data_str = file.readline()
    data = data_str.strip().split(";")
    data.insert(0,"test_id")
    outputfile(output_file_name,data,read_mode='w')
    
    test_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    
    while True:
        data_str = file.readline()   
        if data_str == "":
            break
        data_str = data_str.replace('"', '')
        data_str = data_str.replace(';;', ';')
        
        
        data = data_str.strip().split(";")
        if len(data) == 1:
            continue
        data.insert(0,test_id)
        data[3] = tree_type
        outputfile(output_file_name,data)
    
    file.close() 
    
def change_split(input_file_name,output_file_name,tree_type):

    file = open(f'./benchmark/{input_file_name}.csv', "r") 
    data_str = file.readline()
    data = data_str.strip().split(";")
    outputfile(output_file_name,data,read_mode='w')
    

    while True:
        data_str = file.readline()   
        if data_str == "":
            break
        data_str = data_str.replace('"', '')
        data_str = data_str.replace(';;', ';')
        
        
        data = data_str.strip().split(";")
        if len(data) == 1:
            continue
        data[3] = tree_type
        outputfile(output_file_name,data)
    
    file.close() 
#change_split("testin","testing1","Deep_tree")

import sys



arg = sys.argv[1]
if arg == "1":

    ############ VVVVVVVV TO RUN
    print("Begin fixing maria db")
    print("Begin balanced_tree")
    change_split("benchmark_balanced_tree_mariadb_stats1RUN","benchmark_balanced_tree_mariadb_stats_1_CLEAN","Balanced_tree")
    change_split("benchmark_balanced_tree_mariadb_stats2RUN","benchmark_balanced_tree_mariadb_stats_2_CLEAN","Balanced_tree")
    print("Begin Deep_tree")
    change_split("benchmark_deep_tree_mariadb_stats","benchmark_deep_tree_mariadb_stats_CLEAN","Deep_tree")
    #print("Begin Wide_tree")
    #change_split("benchmark_wide_tree_mariadb_stats","benchmark_wide_tree_mariadb_stats_clean","Wide_tree")
elif arg == "2":
    print("Begin fixing Snowflake")
    print("Begin balanced_tree")
    change_split_add_id("benchmark_balanced_tree_snowflake_stats","benchmark_balanced_tree_snowflake_stats_CLEAN","Balanced_tree")
    print("Begin Deep_tree")
    change_split_add_id("benchmark_deep_tree_snowflake_stats","benchmark_deep_tree_snowflake_stats_CLEAN","Deep_tree")
    #print("Begin Wide_tree")
    #change_split_add_id("benchmark_wide_tree_snowflake_stats","benchmark_wide_tree_snowflake_stats_CLEAN")
elif arg == "3":
    print("Begin fixing postgresql")
    print("Begin balanced_tree")
    change_split_add_id("benchmark_balanced_tree_postgresql_stats","benchmark_balanced_tree_postgresql_stats_CLEAN","Balanced_tree")
elif arg == "4":
    print("Begin fixing postgresql")
    print("Begin Deep_tree")
    change_split_add_id("benchmark_deep_tree_postgresql_stats","benchmark_deep_tree_postgresql_stats_CLEAN","Deep_tree")
    #print("Begin Wide_tree")
    #change_split_add_id("benchmark_wide_tree_postgresql_stats","benchmark_wide_tree_postgresql_stats_CLEAN")
