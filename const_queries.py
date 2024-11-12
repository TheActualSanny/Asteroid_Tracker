'''The constant queries which are used in other modules program will be stored here.'''
from logger_conf import log_results

@log_results(logger_name = 'insert_query_const')
def insert_query(table_name, *columns):
    placeholder_repeat = ', '.join(['%s' for _ in range(len(columns))])
    parentheses_columns = ', '.join(columns)
    query = f'''INSERT INTO {table_name}({parentheses_columns}) VALUES({placeholder_repeat})'''
    return query

@log_results(logger_name = 'create_query_const')
def create_query(table_name, **columns):
    column_pairs = ', '.join([f'{column} {columns[column]}' for column in columns])
    return f'CREATE TABLE IF NOT EXISTS {table_name}({column_pairs})'
