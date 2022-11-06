import os
import duckdb

# Query module

# print out columns in a table
def print_columns(table, cursor):
    txt = f"PRAGMA table_info('core.{table}');"
    q = cursor.execute(txt)
    print('Columns:')
    for ix, col in enumerate(q.fetchall()):
        [print((ix+1, val)) for ct, val in enumerate(col) if ct == 1]

# print out table row counts
def print_row_count(table, cursor):
    txt = f"SELECT COUNT(*) FROM core.{table};"
    q = cursor.execute(txt)
    print(f'Row count: {q.fetchall()[0]}')

# print out tables, row counts, and columns
def print_table_info(cursor):
    q = cursor.execute("PRAGMA show_tables;")
    for i, table in enumerate(q.fetchall()):
        table = table[0]
        # skip views
        if table[:3] == 'vw_':
            pass
        else:
            print(f'\nTable {i+1}: {table}')
            print_row_count(table, cursor)
            print_columns(table, cursor)

# print out database size
def print_db_size(cursor):
    q = cursor.execute('PRAGMA database_size;')
    res = q.fetchall()
    cols = ['database_size', 'block_size', 'total_blocks', 'used_blocks'
            , 'free_blocks', 'wal_size', 'memory_usage', 'memory_limit']
    print('')
    for k, v in zip(cols, res[0]):
        print(k, ':',  v)
