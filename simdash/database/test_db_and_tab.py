import database
import sqlite3
import table
import json
import pytest
import pandas as pd

def test_one_and_only():
    the_db = database.Database("/home/kieran/Desktop/SQLChartsTrial/simdash/simdash/database/test_db.db")
    tester_conn = sqlite3.connect("/home/kieran/Desktop/SQLChartsTrial/simdash/simdash/database/test_db.db")
    cols = ["a", "bbb", "c4", "real_time", "logic_time"]
    dtypes = ["TEXT", "FLOAT", "INT", "INT", "FLOAT"]
    vtypes = ["N", "Q", "Q", "T", "Q"]
    the_db.make_table("test_table", cols, dtypes, vtypes, "logic_time", "real_time")
    tab = the_db.get_table("test_table")
    tab.append(a="dogs", bbb=15.0, c4=12, real_time=109)
    tab.append(a="lizards", bbb=16.0, c4=12, r_time = 110)
    tab.append(a="dolphins", bbb=17.0, c4=12, logic_time=12, r_time=1011)
    tab.append(a="manatees", bbb=18.0, c4=12, l_time=9)
    tab.append(a="turtoises", bbb=19.0, l_time=4000, c4=12, r_time=1001)
    tab.append(a="cats", bbb=20.0, r_time=3, c4=15)
    assert(tab.len() == 6)
    with pytest.warns(UserWarning):
        the_db.make_table("test_table", cols, dtypes, vtypes, "logic_time", "real_time")
    tab2 = the_db.get_table("test_table")
    expected_a = ["dogs", "lizards", "dolphins", "manatees", "turtoises", "cats"]
    expected_bbb = [15.0, 16.0, 17.0, 18.0, 19.0, 20.0]
    expected_4c = [12, 12, 12, 12, 12, 15]
    expected_real_time = [109, 110, 1011, pd.Timestamp("now").timestamp(), 1001, 3]
    expected_logic_time = [1.0, 2.0, 12.0, 9.0, 4000.0, 4001.0]
    with tester_conn:
        c = tester_conn.cursor()
        count = 0
        for row in c.execute("SELECT * FROM test_table;"):
            if round(row[0], -5) == 0:
                assert row[0] == expected_real_time[count]
            else:
                assert round(row[0], -2) == round(expected_real_time[count], -2) #r_time   
                assert row[1] == expected_logic_time[count] #l_time
                assert row[2] == expected_a[count] #a
                assert row[3] == expected_bbb[count] #bbb
                assert row[4] == expected_4c[count] #4c
            count += 1
    the_db.remove_table("test_table")
    the_db.make_table("other_table", cols, dtypes, vtypes, "logic_time", "real_time")
    with tester_conn:
        c = tester_conn.cursor()
        for row in c.execute('SELECT name FROM sqlite_master WHERE type = "table";'):
            assert "test_table" not in row
        for row in c.execute('SELECT table_name FROM "meta_table";'):
            assert "test_table" not in row
    with pytest.raises(ValueError):
        the_db.get_table("test_table")
    with pytest.raises(ValueError):
        the_db.get_table("some_table")
    other_table = the_db.get_table("other_table")
    other_table.append(real_time=109, logic_time=12, a="dogs", bbb=15.0, c4=12)
    other_df = other_table.to_pandas()
    test_df = pd.DataFrame(columns=['real_time', 'logic_time', 'a', 'bbb', 'c4'])
    test_df.loc[0] = [109, 12, "dogs", 15.0, 12]
    pd.testing.assert_frame_equal(test_df, other_df, check_dtype=False)
    the_db.remove_table("other_table")
    
