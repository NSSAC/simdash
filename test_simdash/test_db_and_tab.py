import pandas as pd
import pytest
import sqlite3
from simdash.database.table import Table
from simdash.database.database import Database
 
def test_one_and_only():
    the_db = Database("/home/kieran/Desktop/SQLChartsTrial/simdash/test_simdash/test_db.db")
    t_conn = sqlite3.connect("/home/kieran/Desktop/SQLChartsTrial/simdash/test_simdash/test_db.db")
    cols = ["logic_time", "real_time", "a", "bbb", "c4"]
    dtypes = ["FLOAT", "INT", "TEXT", "INT", "FLOAT"]
    vtypes = ["N", "Q", "Q", "T", "Q"]
    the_db.make_table("test_table", cols, dtypes, vtypes)
    tab = the_db.get_table("test_table")
    tab.append(a="dogs", bbb=15.0, c4=12, real_time="08-06-2013")
    tab.append(a="lizards", bbb=16.0, c4=12, r_time ="08-07-2013")
    tab.append(a="dolphins", bbb=17.0, c4=12, logic_time=12.0, r_time="08-08-2013")
    tab.append(a="manatees", bbb=18.0, c4=12, l_time=1000.0)
    tab.append(a="turtoises", bbb=19.0, l_time=4000.0, c4=12, r_time="08-10-2013")
    tab.append(a="cats", bbb=20.0, c4=15)
    assert(tab.len() == 6)
    with pytest.warns(UserWarning):
        the_db.make_table("test_table", cols, dtypes, vtypes)
    tab2 = the_db.get_table("test_table")
    expected_a = ["dogs", "lizards", "dolphins", "manatees", "turtoises", "cats"]
    expected_bbb = [15.0, 16.0, 17.0, 18.0, 19.0, 20.0]
    expected_4c = [12, 12, 12, 12, 12, 15]
    expected_real_time = [pd.Timestamp("08-06-2013").timestamp(), pd.Timestamp("08-07-2013").timestamp(),
                          pd.Timestamp("08-08-2013").timestamp(), pd.Timestamp("now").timestamp(),
                          pd.Timestamp("08-10-2013").timestamp(), pd.Timestamp("now").timestamp()]
    expected_logic_time = [1.0, 2.0, 12.0, 1000.0, 4000.0, 4001.0]
    with t_conn:
        c = t_conn.cursor()
        count = 0
        for row in c.execute("SELECT * FROM test_table;"):
            if round(row[1], -5) == 0:
                assert row[1] == expected_real_time[count]
            else:
                assert round(row[1], -5) == round(expected_real_time[count], -5) #r_time   
                assert row[0] == expected_logic_time[count] #l_time
                assert row[2] == expected_a[count] #a
                assert row[3] == expected_bbb[count] #bbb
                assert row[4] == expected_4c[count] #4c
            count += 1
    the_db.remove_table("test_table")
    the_db.make_table("other_table", cols, dtypes, vtypes)
    with t_conn:
        c = t_conn.cursor()
        for row in c.execute('SELECT name FROM sqlite_master WHERE type = "table";'):
            assert "test_table" not in row
            for row in c.execute('SELECT table_name FROM "meta_table";'):
                assert "test_table" not in row
    with pytest.raises(ValueError):
        the_db.get_table("test_table")
    with pytest.raises(ValueError):
        the_db.get_table("some_table")
    other_table = the_db.get_table("other_table")
    other_table.append(real_time="08-09-2015", logic_time=12, a="dogs", bbb=15.0, c4=12)
    other_df = other_table.to_pandas()
    test_df = pd.DataFrame(columns=['logic_time', 'real_time', 'a', 'bbb', 'c4'])
    test_df.loc[0] = [12, pd.Timestamp("08-09-2015").timestamp(), "dogs", 15.0, 12]
    pd.testing.assert_frame_equal(test_df, other_df, check_dtype=False)
    the_db.remove_table("other_table")
    
