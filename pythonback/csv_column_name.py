import re
def check_col_type(df):
    col_type = ['string', 'integer', 'float']
    for i, col_type_ in enumerate(df.iloc[0]):
        if col_type_ in col_type:
            continue
        else:
            print("---------------------------------")
            print("worng input value, could I recognize this value as string?(Y/N)")
            print('col number:', i, ', column type:' ,col_type_)
            user_input = input()
            if user_input.lower() == 'y' or user_input.lower() == 'yes':
                df.iloc[0][i] = 'string'
            else:
                print('then please let me know what this column type is(float or integer)')
                user_input = input()
                if user_input.lower() == 'float':
                    df.iloc[0][i] = 'float'
                elif user_input.lower() == 'int' or user_input.lower() == 'integer':
                    df.iloc[0][i] = 'integer'
                else:
                    print("please enter again")
                    return df
    return df

def change_column(df, patterns):

    #컬럼명에 특수문자, 공백이 있는지 검사 후 값 변경
    df_col = list(df.columns)
    pattern = ''.join(patterns)
    df_col2 = list(map(lambda x: re.sub(f'[{pattern}]', '_', x) if isinstance(x, str) else x, df_col))
    df.columns = df_col2

    # 컬럼명이 숫자로 시작하는 지 검사 후 값 변경
    df_col2 = list(df.columns)
    num_pattern = r"^\d"
    regex = re.compile(num_pattern)
    # Iterate over the list of columns
    for columns in df_col2:
    # Use the search function to check if the columns starts with a number
        match = regex.search(columns)
        if match:
        # Use the sub function to replace the columns with 'col'
            modified_column = regex.sub('col', columns)
            df.rename(columns = {columns:modified_column},inplace=True)
        else: continue


    # 컬럼명이 중복될 경우 중복된 열 이름 변경
    # Check for duplicate column names
    duplicated = df.columns.duplicated()
    if True in duplicated:
        # Create a list of duplicate column names
        duplicate_columns = [[i, column] for i, column in enumerate(df.columns) if duplicated[i]]
        # Rename the duplicate columns
        counter = 2
        df_col = list(df.columns)
        # print(duplicate_columns)
        #for tuple in duplicated_colomn 이렇게 수정하기
        last_value = ''
        for idx, col in duplicate_columns:
            # print(idx, col)
            if col != last_value: counter = 2
            df_col[idx] =  df_col[idx] + str(counter)
            counter += 1
            last_value = col        #이전 컬럼명과 같은지 체크하는 기능
        df.columns = df_col

    return df
# patterns가 컬럼명에 있으면 pattern 부분을 다음과 같이 바꿔주는 함수
# 1. 컬럼명에 예약어, 특수문자, 공백이 있는 경우 col_{컬럼번호} 이렇게 바꿔준다
# 2. 컬럼명이 숫자로 시작될 경우 숫자를 col로 바꿔준다 (공백으로 처리하지 않은 이유는 숫자만 있는 경우 공백으로 바꾸면 안되기 때문이다)
# 3. 컬럼명이 중복될 경우 중복된 열 이름을 바꿔준다. ex) test test test name name -> test test2 test3 name name2 이렇게 바꿔준다
