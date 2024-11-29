import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import csv

colors = ['blue', 'green', 'red', 'orange', 'purple', '#FF5733', '#33FF57', '#3357FF', 'cyan', 'magenta']
markers = [
        'v',  # Triangle down
        'o',  # Circle
        's',  # Square
        'p',  # Pentagon
        '*',  # Star
        'h',  # Hexagon1
        '^',  # Triangle up
        '<',  # Triangle left
        '>',  # Triangle right
        '1',  # Tri-down (3 points)
        '2',  # Tri-up (3 points)
        '3',  # Tri-left (3 points)
        '4',  # Tri-right (3 points)
        'H',  # Hexagon2
        '+',  # Plus
        'x',  # X
        'D',  # Diamond
        'd',  # Thin diamond
        '|',  # Vertical line
        '_',  # Horizontal line
    ]

def plot_graph(x_value:list,
               y_values: list[list],
               y_value_names:list,
               x_name:str,
               y_name:str,
               title:str,
               filename:str):

    
    
    
    for i in range(len(y_values)):
        plt.plot(x_value, y_values[i], label=y_value_names[i], marker=markers[i],color=colors[i])
        


    # Set labels and title
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.title(title)
    plt.grid(True)
    plt.legend()
    plt.savefig(f"plots/{filename}.png", format='png')
    
    # Show legend
    # Display plot
    plt.show()
    plt.close()  # Close the figure after saving



def plot_snowflake_trees(measurement_points:list,repetitions:int):
    
    data = defaultdict(lambda: defaultdict(dict))

    # Load data from a CSV file wide
    with open('./benchmark_wide_role_tree_stats.csv', 'r') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            # Skip rows that do not have exactly 4 elements
            if len(row) != 4:
                continue
            repetition, measurement_type, elapsed_seconds, elapsed_milli = row
            data["Wide"][measurement_type][repetition] = [elapsed_seconds, elapsed_milli]
    file.close()
    
    # Load data from a CSV file deep
    with open('./benchmark_deep_role_tree_stats.csv', 'r') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
        # Skip rows that do not have exactly 4 elements
            if len(row) != 4:
                continue
            repetition, measurement_type, elapsed_seconds, elapsed_milli = row
            data["Deep"][measurement_type][repetition] = [elapsed_seconds, elapsed_milli]
    file.close()
    
    # Load data from a CSV file balanced
    with open('./benchmark_balanced_role_tree_stats.csv', 'r') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
        # Skip rows that do not have exactly 4 elements
            if len(row) != 4:
                continue
            repetition, measurement_type, elapsed_seconds, elapsed_milli = row
            data["Balanced"][measurement_type][repetition] = [elapsed_seconds, elapsed_milli]
    file.close()
    
    #calc sum and create array for wide data points
    Wide_lst = []
    Deep_lst = []
    Balanced_lst = []
    
    for measurment in measurement_points:
        wide_sum = 0
        deep_sum = 0
        balanced_sum = 0
        for i in range(repetitions):
            wide_sum += float(data["Wide"][f'{measurment}'][f'{i}'][1])
            deep_sum += float(data["Deep"][f'{measurment}'][f'{i}'][1])
            balanced_sum += float(data["Balanced"][f'{measurment}'][f'{i}'][1])
        Wide_lst.append((wide_sum/repetitions))
        Deep_lst.append((deep_sum/repetitions))
        Balanced_lst.append((balanced_sum/repetitions))
        
    plot_graph(measurement_points,
               [Wide_lst,Deep_lst,Balanced_lst],
               ["Wide","Deep","Balanced"],
               "Role creation",
               "Time in ms",
               "Snowflake Role trees",
               "Snowflake_Role_Tree_graphs")
    

import pandas as pd
from matplotlib import pyplot as plt
import plotly.express as px

if __name__ == "__main__":


    df = pd.read_csv('Snowflake_latency.csv').query("TREE_QUERY_TYPE in ['GRANT_balanced_tree', 'CREATE_balanced_tree','GRANT_deep_tree', 'CREATE_deep_tree','GRANT_wide_tree', 'CREATE_wide_tree',]")
    df = df.query("AVG_LATENCY_MS <= 1000")
    #df = df.query("SECOND > 500")
    
    
    fig = px.line(df, x="SECOND", y="AVG_LATENCY_MS", color="TREE_QUERY_TYPE",text="AVG_LATENCY_MS",symbol="TREE_QUERY_TYPE")
    fig.update_traces(textposition="bottom right")
    fig.show()

        
    
    
    
    

        
   

