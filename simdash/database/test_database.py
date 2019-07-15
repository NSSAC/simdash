import tab
import db
import pytest
import sqlite3
import json
#from vega_datasets import data

"""Tests for db.py and tab.py"""

@pytest.fixture()
def connect_db():
    mem = ":memory:"
    test_db = db.Db(mem)
    yield test_db
    #creates a new database in mem every time


@pytest.fixture()
def create_tab_tuple():
    table_name = ("test_table")
    columns = ["dogs", "cats", "lizards", "l_time", "r_time"]
    dtypes = ["INT", "FLOAT", "TEXT", "FLOAT", "INT"]
    vtypes = ["Q", "N", "O", "T", "T"]
    return (table_name, columns, dtypes, vtypes)
    
    
def test_connection(connect_db):
    #Tests that the connection is created from a Db initializations
    assert isinstance(connect_db.conn, sqlite3.Connection)

def test_meta_table_columns(connect_db):
    # Tests that Db initialization produces a meta table with the desired columns
    sql_string = "PRAGMA table_info(meta_table);"
    column_names = ["table_name", "columns", "dtypes", "vtypes", "l_time_column", "r_time_column"]
    received_columns = []
    with connect_db.conn:
        c = connect_db.conn.cursor()
        for row in c.execute(sql_string):
            received_columns.append(row[1])
    for i in range(6):
        assert(column_names[i] == received_columns[i])
    
def test_column_length(connect_db, create_tab_tuple):
    # Tests that if columns is of different length than vtypes or dtypes, an error is raised
    create_tab_tuple[1].append("other_column")
    tup = create_tab_tuple
    with pytest.raises(ValueError):
        connect_db.make_table(tup[0], tup[1], tup[2], tup[3])

def test_vtype_length(connect_db, create_tab_tuple):
    # Tests that if the vtype is of different length than columns or dtypes, an error is raised
    create_tab_tuple[3].append("other_vtype")
    tup = create_tab_tuple
    with pytest.raises(ValueError):
        connect_db.make_table(tup[0], tup[1], tup[2], tup[3])

def test_dtype_length(connect_db, create_tab_tuple):
    # Tests that if the dtype is of different length than columns or vtypes, an error is raised
    create_tab_tuple[2].append("other_dtype")
    tup = create_tab_tuple
    with pytest.raises(ValueError):
        connect_db.make_table(tup[0], tup[1], tup[2], tup[3])

def test_log_time_param(connect_db, create_tab_tuple):
    # Tests that an error is raised if the l_time_column is given a value not in columns
    tup = create_tab_tuple
    with pytest.raises(ValueError):
        connect_db.make_table(tup[0], tup[1], tup[2], tup[3], "not_logical_time")

def test_real_time_param(connect_db, create_tab_tuple):
    # Tests that an error is raised if the r_time_column is given a value not in columns
    tup = create_tab_tuple
    with pytest.raises(ValueError):
        connect_db.make_table(tup[0], tup[1], tup[2], tup[3], r_time_column="wrong_real_time")

def test_meta_table_insertion(connect_db, create_tab_tuple):
    # Tests that the data from make_table is inserted correctly into the meta_table
    tup = create_tab_tuple
    connect_db.make_table(tup[0], tup[1],tup[2], tup[3])
    print(tup[2])
    print(tup[3])
    with connect_db.conn:
        c = connect_db.conn.cursor()
        sql_string = 'SELECT table_name, columns, dtypes, vtypes FROM meta_table WHERE table_name IN ("test_table");'
        for row in c.execute(sql_string):
            print(row)
            assert row[0] == "test_table"
            assert row[1] == json.dumps(tup[1])
            assert row[2] == json.dumps(tup[2])
            assert row[3] == json.dumps(tup[3])

def test_non_meta_table(connect_db, create_tab_tuple):
    # Tests that the columns of the created table from make_table are correct
    tup = create_tab_tuple
    sql_string = 'PRAGMA table_info("test_table")'
    received_columns = []
    desired_columns = ["r_time", "l_time", "dogs", "cats", "lizards"]
    connect_db.make_table(tup[0], tup[1], tup[2], tup[3])
    with connect_db.conn:
        c = connect_db.conn.cursor()
        for row in c.execute(sql_string):
            received_columns.append(row[1])
    for i in range(5):
        assert desired_columns[i] == received_columns[i]
    

def test_l_time_insert(connect_db, create_tab_tuple):
    # Tests that the columns of the created table from make_table are correct even if l_time was given a different value
    tup = create_tab_tuple
    tup[1][3] = "some_other_logical_time"
    sql_string = 'PRAGMA table_info("test_table")'
    received_columns = []
    desired_columns = ["r_time", "some_other_logical_time", "dogs", "cats", "lizards"]
    connect_db.make_table(tup[0], tup[1], tup[2], tup[3], "some_other_logical_time")
    with connect_db.conn:
        c = connect_db.conn.cursor()
        for row in c.execute(sql_string):
            received_columns.append(row[1])
    for i in range(5):
        assert desired_columns[i] == received_columns[i]
   

def test_r_time_insert(connect_db, create_tab_tuple):
    # Tests that the columns of the created table from make_table are correct even if r_time was given a different value
    tup = create_tab_tuple
    tup[1][4] = "some_other_real_time"
    sql_string = 'PRAGMA table_info("test_table")'
    received_columns = []
    desired_columns = ["some_other_real_time","l_time", "dogs", "cats", "lizards"]
    connect_db.make_table(tup[0], tup[1], tup[2], tup[3], r_time_column="some_other_real_time")
    with connect_db.conn:
        c = connect_db.conn.cursor()
        for row in c.execute(sql_string):
            received_columns.append(row[1])
    for i in range(5):
        assert desired_columns[i] == received_columns[i]
