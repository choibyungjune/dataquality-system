import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import glob
import re
import mysql.connector


# Connect to the database
conn = pymysql.connect(host="127.0.0.1", user="root", password="0000", db="csv_db4")

# Create a cursor object
cursor = conn.cursor()
# Connect to the database
cnx = mysql.connector.connect(user="root", password="0000",
                              host="127.0.0.1",
                              database="csv_db4")

# Create a cursor object
cursor = cnx.cursor()

# Define the list of reserved words
reserved_words = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'ORDER', 'BY']
column_patterns = ['\\\\n', "N/A", '{', '}']
masq_commands = ['abst_row_num__', 'rec_disim', 'rec_numerical', 'rec_categorical']
space_ = [' ']


# Define the table name
db_name = 'csv_db4'
table_name = 'choi_test2'


print()
print("---------------------------------------------")
print("reserved word exist: ")
def col_reserved_word_check(reserved_words):
    col_lst = []
    for reserved_word in reserved_words:
        # Execute a query to get all column names from the table
        # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
        # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
        # print(column_patern)

        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '{reserved_word}'"
        cursor.execute(query)
        column_names = cursor.fetchall()
        # print(column_names)
        col_lst.append(column_names)
    return col_lst

print(col_reserved_word_check(reserved_words))

print()
print("---------------------------------------------")
print("masq command exist: ")
def col_masq_command_check(masq_commands):
    col_lst = []
    for masq_command in masq_commands:
        # Execute a query to get all column names from the table
        # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
        # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
        # print(column_patern)

        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '{masq_command}'"
        cursor.execute(query)
        column_names = cursor.fetchall()
        # print(column_names)
        col_lst.append(column_names)
    return col_lst

print(col_masq_command_check(masq_commands))


print()
print("---------------------------------------------")
print("special character exist: ")
def col_special_char_check(column_patterns):
    col_lst = []
    for column_patern in column_patterns:
        # Execute a query to get all column names from the table
        # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
        # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
        # print(column_patern)
        if column_patern == '\\\\n':     # \n인 경우만 따로 검사
            query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) = '{column_patern}'"
            cursor.execute(query)
            column_names = cursor.fetchall()
            col_lst.append(column_names)
            # print(column_names)
            continue
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '%{column_patern}%'"
        cursor.execute(query)
        column_names = cursor.fetchall()
        col_lst.append(column_names)
        # print(column_names)
    return col_lst

print(col_special_char_check(column_patterns))


print()
print("---------------------------------------------")
print("special character exist: ")


# 특수문자 바꾸는 함수

# db table column name 가져와서 clear_col_name 이라는 list에 넣는 코드
query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}'"
cursor.execute(query)
column_names = cursor.fetchall()
clear_col_name = []
for col in column_names:
    clear_col_name.append(col[0])


def change_word(lst, patterns):
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    result = [re.sub(pattern, '_', x) if isinstance(x, str) else x for x in lst]
    return result


print(clear_col_name)
print(change_word(clear_col_name, column_patterns))


# print(clear_col_name)
exit()
