import datetime
import os
import time
import snowflake.connector
import sql.deep_role_sql as deep
import sql.wide_role_sql as wide
import sql.balanced_role_sql as balanced

import utils as util
import psycopg2 
import mariadb
import sys

def wide_tree(cur,db,tree_size):
        role_num = 1
        while True:
            if tree_size < role_num:
                print(f"reached role {role_num}. Exiting loop.")
                break
            
            for query in wide.generate_role_queries(db,f"Role{role_num}"):
                cur.execute(query)
            role_num += 1
  

    
def deep_tree(cur,db,tree_size):
        role_num = 1
        while True:
            if tree_size < role_num:
                print(f"reached role {role_num}. Exiting loop.")
                break
            
            for query in deep.generate_role_queries(db,f"Role{role_num}",f"Role{role_num-1}"):
                cur.execute(query)
            role_num += 1
    
def balanced_tree(cur,db,tree_size):
        current = 0
        front = 1
        while True:
            if tree_size < front:
                print(f"reached role {front}. Exiting loop.")
                break
            
            for query in balanced.generate_role_queries(db,f"Role{current}",f"Role{(front)}"):
                cur.execute(query)
                
            if (front % 4) == 0:
                    current += 1
            front += 1
    
    
    
