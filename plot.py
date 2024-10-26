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
    plt.savefig(f"plots/{filename}.png", format='png')
    
    # Show legend
    plt.legend()
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
        deep_sum = 2000000
        balanced_sum = 4000000
        for i in range(repetitions):
            wide_sum += float(data["Wide"][f'{measurment}'][f'{i}'][1])
            deep_sum += float(data["Deep"][f'{measurment}'][f'{i}'][1])
            balanced_sum += float(data["Balanced"][f'{measurment}'][f'{i}'][1])
        Wide_lst.append((wide_sum/repetitions)/1000)
        Deep_lst.append((deep_sum/repetitions)/1000)
        Balanced_lst.append((balanced_sum/repetitions)/1000)
        
    plot_graph(measurement_points,
               [Wide_lst,Deep_lst,Balanced_lst],
               ["Wide","Deep","Balanced"],
               "Role creation",
               "Time in s",
               "Snowflake Role trees",
               "Snowflake_Role_Tree_graphs")
    

if __name__ == "__main__":
    plot_snowflake_trees([500,1_000,2_000,4_000,8_000,16_000,32_000,64_000,100_000],2)
        
    
    
    
    

        
   

