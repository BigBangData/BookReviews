import os
import duckdb

# paths
GIT_DIR = os.path.join(os.environ['HOME'], 'Documents', 'GitHub', 'BookReviews')
DATA_DIR = os.path.join(GIT_DIR, 'data')
CORE_DIR = os.path.join(GIT_DIR, 'book_reviews', 'core')

# create a table from a csv file
def create_table_from_csv(csv_name, cursor):
    tbl_name = csv_name.split(".")[0]
    csv_path = os.path.join(CORE_DIR, csv_name)
    beg = "CREATE TABLE IF NOT EXISTS core."
    mid = " AS SELECT * FROM read_csv_auto('"
    end = "');"
    txt = ''.join([beg, tbl_name, mid, csv_path, end])
    cursor.execute(txt)

# create a table for each csv file in core
def create_core_tables(cursor):
    num_tables = len(os.listdir(CORE_DIR))
    print(f'Found {num_tables} seed files in core.')
    for csv_name in os.listdir(CORE_DIR):
        table_name = csv_name.split(".")[0]
        print(f'Creating {table_name}...')
        create_table_from_csv(csv_name, cursor)
    print('Created core tables.')

# print out columns in a table
def print_columns(table, cursor):
    text = f"PRAGMA table_info('core.{table}');"
    q = cursor.execute(text)
    print('Columns:')
    for ix, col in enumerate(q.fetchall()):
        [print((ix+1, val)) for ct, val in enumerate(col) if ct == 1]

# print out table row counts
def print_row_count(table, cursor):
    txt = f"SELECT COUNT(*) FROM core.{table};"
    q = cursor.execute(txt)
    print(f'Row count: {q.fetchall()[0]}')

# print out database size
def print_db_size(cursor):
    q = cursor.execute('PRAGMA database_size;')
    res = q.fetchall()
    cols = ['database_size', 'block_size', 'total_blocks', 'used_blocks'
            , 'free_blocks', 'wal_size', 'memory_usage', 'memory_limit']
    print('')
    for k, v in zip(cols, res[0]):
        print(k, ':',  v)

# print out tables, row counts, columns, and db size
def print_info(cursor, skipviews=True):
    print('Gathering info...')
    q = cursor.execute("PRAGMA show_tables;")
    for k, v in enumerate(q.fetchall()):
        table = v[0]
        # skip views
        if skipviews:
            if table[:3] == 'vw_':
                pass
            else:
                print(f'\nTable {k+1}: {table}')
                print_row_count(table, cursor)
                print_columns(table, cursor)
        else:
            print(f'\nTable {k+1}: {table}')
            print_row_count(table, cursor)
            print_columns(table, cursor)
    # db size
    print_db_size(cursor)

def main():
    # establish conn
    conn = duckdb.connect(os.path.join(DATA_DIR, 'books.duckdb'))
    cursor = conn.cursor()
    # create core schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")
    # create core tables
    create_core_tables(cursor)
    # print out all info
    print_info(cursor)
    # close conn
    cursor.close()
    conn.close()


if __name__=="__main__":
    main()
