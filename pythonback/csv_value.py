import re
def check_word(df, patterns):       # patterns가 속성값에 있으면 True, 없으면 False 반환하는 함수
    # Use the applymap() method to apply a lambda function to each element in the DataFrame
    result = df.applymap(lambda x: isinstance(x, str) and any(pattern.lower() in x.lower() for pattern in patterns))
    return result


def change_word(df, patterns):      # patterns가 속성값에 있으면 pattern 부분만 '_'로 바꿔주는 함수
    #for index_test in list(df.columns): print(df[index_test].dtypes) #check columns dtype
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '_', x) if isinstance(x, str) else x)
    return result


def change_word_null(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)
    return result
