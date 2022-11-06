import os
import duckdb
import query_db

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

def main():
    # establish conn
    conn = duckdb.connect(os.path.join(DATA_DIR, 'books.duckdb'))
    cursor = conn.cursor()
    # create core schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS core;")
    # create core tables
    create_core_tables(cursor)
    # print out table info
    query_db.print_table_info(cursor)
    # print out db size
    query_db.print_db_size(cursor)
    # close conn
    cursor.close()
    conn.close()


if __name__=="__main__":
    main()
