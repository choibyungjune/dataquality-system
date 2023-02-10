import pandas as pd
import numpy as np
import re
import mariadb
import pymysql
from sqlalchemy import create_engine
import glob
from multiprocessing import Pool
from multiprocessing import freeze_support
import dask.dataframe as dd
from tqdm import tqdm
# from tqdm.auto import tqdm
from csv_column_name import check_col_type, change_column
from csv_value import check_word, change_word, change_word_null

# The DataFrame of strings
# df = pd.DataFrame([[1, "'2", 'select',1,2,3,4,5,6,7], [4, None, '\n as',1,2,3,4,5,6,7], [7, ' ', 9,1,2,3,4,5,6,7], [None, None, None,1,2,3,4,None,6,7]], columns=['「ex','{','2test','3test','abst_row_num__','test','test','name','name','name'])
# df = pd.DataFrame([])

# df = pd.read_csv('sample.csv')
df = pd.read_csv('HW_LDGS_DAIL_MAX_AVRG_MIN_PRC_INFO_202212-10.csv')
# df = pd.read_csv("국민건강보험공단_건강검진정보_20211229.CSV", encoding="cp949")

# The patterns we want to find 특수문자나 예약어 등록해놓으면
column_patterns = ['"', "'", '{', '}','「','(',')', ' ']
value_patterns = ['"', "'", '{', '}'] #to change '_'
#ratio는 값에서 검사
reserved_words = ['select', 'from', 'where','abst_row_num__','rec_disim','rec_numerical','rec_categorical','ratio:']
null_words = ['\\\\n','\n','\\n','\\\\','!','@','#','$','%','^','&','\\(','\\)','\\*'] # (, ), *은 정규식에 영향을 주므로 escape 시켜줘야함
# null_words = ['\\n', '\\']
col_type = ['string', 'integer', 'float']


def add_record(df):
    if (len(df)>0):
        df.insert(0, 'record_number', range(1, len(df) + 1), True)
    else: pass
    return df

# print("---------------------------------")
# print("original dataframe")
# print(df)

# df = change_column(df, column_patterns)
# print("---------------------------------")
# print("column_change")
# print(df)


# df = change_column(df, reserved_words)
# print("---------------------------------")
# print("column_change : reserved words")
# print(df)

# df = change_word(df, value_patterns)
# print("---------------------------------")
# print("word_change")
# print(df)

# df = change_word(df, reserved_words)
# print("---------------------------------")
# print("word_change : reserved words")
# print(df)

# df = change_word_null(df, null_words)
# # result = tqdm(df, total=len(df), desc='Removing null words')
# print("---------------------------------")
# print("word_change : null words")
# print(df.head(50))

# del [df]
# exit()
# df = add_record(df)
# print("---------------------------------")
# print("add record column")
# print(df)
# Use compression
# df.to_csv('large_file.csv', chunksize=100000, index=False)
# Use chunking and display progress

def chunk_work(df, chunk_size, func):
    for i, chunk in tqdm(enumerate(np.array_split(df, df.shape[0] // chunk_size + 1)), total= df.shape[0] // chunk_size + 1, position=0, leave=True):
        if i == 0:
            df = func(chunk, null_words)
        else:
            df = pd.concat([df, func(chunk, null_words)], ignore_index=True)
    return df
# chunk_size = 100000


# print(chunk_work(df, chunk_size,change_word_null))





# chunk_size = 100000
# for i, chunk in tqdm(enumerate(np.array_split(df, df.shape[0] // chunk_size + 1)), total= df.shape[0] // chunk_size + 1, position=0, leave=True):
#     if i == 0:
#         df = change_word_null(chunk, null_words)
#     else:
#         df = pd.concat([df, change_word_null(chunk, null_words)], ignore_index=True)
# print(df)

# del [df]
# # exit()
# for i, chunk in tqdm(enumerate(np.array_split(df, total_rows // chunk_size + 1)), total=total_rows // chunk_size + 1):
#     chunk.to_csv(f'large_file2.csv', index=False, mode='a')

df = check_col_type(df)
print(df)

del [df] # 들고있는 df 삭제
exit()

# # 5. 속성값이 모두 동일한지 확인
# if df['name'].nunique() == 1:
#     print("There is only one unique value in the column.")
# else:
#     print("There are multiple unique values in the column.")



# # 6. distinct 값의 비율이 80% 이하인지 확인
# distinct_prop = []
# for col_ in df.columns:
#     if df[col_].nunique()/len(df) >= 0.8:
#         distinct_prop.append(True)
#     else :  distinct_prop.append(False)
# print(distinct_prop)

# print(df.dtypes)


#db에 밀어넣기
# if True:
#     host = "127.0.0.1"
#     user = "root"
#     password = "0000"
#     port = "3306"
#     database = "test_db"                  #넣을 db이름

#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

#     df.to_sql(index = False,
#             name = 'df',
#             con = engine,
#             if_exists = 'append',
#             method = 'multi',
#             chunksize = 10000)
