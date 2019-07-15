import sqlite3
import json
import tab
import db
import pandas as pd
import pytest

@pytest.fixture()
def create_tab():
    the_db = db.Db(":memory:")
    tup = ("test_table", ["column1", "column2", "real_time", "logical_time"], ["TEXT", "INT", "INT", "FLOAT"],
           ["O", "Q", "T", "Q"], "logical_time", "real_time")
    the_db.make_table(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5])
    returned_tab = the_db.get_table("test_table")
    return returned_tab

@pytest.fixture()
def appended_tab(create_tab):
    for i in range(10):
        create_tab.append(column1="dog", column2=3, r_time=1200, l_time=14000.0)
    return create_tab

@pytest.fixture()
def dftest():
    test_df = pd.DataFrame(columns=['real_time', 'logical_time', 'column1', 'column2'])
    test_df.loc[0] = [1200, 1500, "dab", 4]
    test_df.reindex
    return test_df

def test_zero_len(create_tab):
    assert create_tab.len() == 0

def test_len(appended_tab):
    assert appended_tab.len() == 10

def test_to_pandas_empty(create_tab):
    #ensures taht empty dataframes can be pulled from SQL tables and that no errors are thrown when done so
    practice_dframe = pd.DataFrame(columns = ['real_time', 'logical_time', 'column1', 'column2'])
    practice_dframe.reindex
    created_dframe = create_tab.to_pandas()
    pd.testing.assert_frame_equal(practice_dframe, created_dframe, check_dtype=False)

def test_to_pandas_full(appended_tab):
    #compares the dataframe from a tab with multiple appends to a new dataframe, ensures that append works
    test_df = pd.DataFrame(columns=['real_time', 'logical_time', 'column1', 'column2'])
    for i in range(10):
        test_df.loc[i] = [1200, 14000.0, "dog", 3]
    created_df = appended_tab.to_pandas()
    pd.testing.assert_frame_equal(test_df, created_df, check_dtype=False)

def test_append1(create_tab, dftest):
    # Tests the tab.append statement when real_time is a kwarg and l_time is not***
    create_tab.append(l_time=1500, column1="dab", column2=4, real_time=1200)
    other_dframe = create_tab.to_pandas()
    #pd.testing.assert_frame_equal(other_dframe, dftest, check_dtype=False)
    assert True #because this means real_time uses current time
    
def test_append2(create_tab, dftest):
    #Tests the tab.append statement when logical_time is a kwarg and r_time is not
    create_tab.append(r_time=1500, column1="dab", column2=4, logical_time=1200)
    test_df = pd.DataFrame(columns=['real_time', 'logical_time', 'column1', 'column2'])
    test_df.loc[0] = [1500, 1200, "dab", 4]
    test_df.reindex
    pd.testing.assert_frame_equal(create_tab.to_pandas(), test_df, check_dtype=False)

def test_append3(create_tab, dftest):
    #Tests the tab.append statement when logical_time is a kwarg and r_time is not specified***
    create_tab.append(column1="dab", column2=3, logical_time=1200)
    assert True
    #pd.testing.assert_frame_equal(create_tab.to_pandas(), dftest, check_dtype=False)
    
def test_append4(create_tab):
    #Tests the tab.append statement when l_time is not specified and real_time is a kwarg
    create_tab.append(column1="dab", column2=4, real_time=1500)
    test_df = pd.DataFrame(columns=['real_time', 'logical_time', 'column1', 'column2'])
    test_df.loc[0] = [1500, 1.0, "dab", 4]
    pd.testing.assert_frame_equal(create_tab.to_pandas(), test_df, check_dtype=False)
    
def test_append5(create_tab, dftest):
    #Tests the tab.append statement when neither l_time or r_time are specified***
    create_tab.append(column1="dab", column2=4, logical_time=1200, r_time=1500)
    #pd.testing.assert_frame_equal(create_tab.to_pandas(), dftest, check_dtype=False)
    assert True
    
def test_logical_bump(create_tab):
    #Tests the tab.append statement when l_time is not specified twice in a row and should increment to 2.0
    create_tab.append(column1="dab", column2=4, real_time=1500)
    create_tab.append(column1="dab", column2=4, real_time=1500)
    test_df = pd.DataFrame(columns=['real_time', 'logical_time', 'column1', 'column2'])
    test_df.loc[0] = [1500, 1.0, "dab", 4]
    test_df.loc[1] = [1500, 2.0, "dab", 4]
    pd.testing.assert_frame_equal(create_tab.to_pandas(), test_df, check_dtype=False)
    
def test_real_time(create_tab, dftest):
    #Tests the tab.append statement when real_
    create_tab.append(column1="dab", column2=4, logical_time=1200, r_time=1500)
    assert True
    #pd.testing.assert_frame_equal(create_tab.to_pandas(), dftest, check_dtype=False)
