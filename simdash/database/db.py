import sqlite3
import json
import tab

class Db:

    def __init__(self, filename):
        """
        Initializes the database connection and meta table
        """
        #self.conn = None
        if filename is not None:
            try:
                self.conn=sqlite3.connect(filename)
                with self.conn:
                    c = self.conn.cursor()
                    create_table_string = """CREATE TABLE IF NOT EXISTS
                    meta_table(table_name TEXT, columns TEXT, dtypes TEXT,
                    vtypes TEXT, l_time_column TEXT, r_time_column TEXT);"""
                    c.execute(create_table_string)
            except sqlite3.Error as e:
                print("SQLite error: %s" %e)
            except Exception as e:
                print("Exception: %s" %e)
        
    def make_table(
            self, table_name, columns, dtypes, vtypes,
            l_time_column="l_time",
            r_time_column="r_time"):
        """
        Inserts a row into meta table containing what columns, datatypes,
        variable types, and the names of the logical and real time variables
        then creates this table elsewhere in the SQL database
        """
        possible_dtype_list = ["INT", "FLOAT", "TEXT"]
        possible_vtype_list = ["Q", "T", "O", "N"]
        if len(columns) != len(dtypes):
            raise ValueError("Length of columns, vtypes and types are not the same")
        if len(columns) != len(vtypes):
            raise ValueError("Length of columns, vtypes and types are not the same")
        if len(dtypes) != len(vtypes):
            raise ValueError("Length of columns, vtypes and types are not the same")
        for value in dtypes:
            if value.upper() not in possible_dtype_list:
                raise ValueError("dytpe %s is not of the correct type" %value)
        for value in vtypes:
            if value not in possible_vtype_list:
                raise ValueError("vtype %s is not of the correct type" %value)
        if l_time_column not in columns:
            raise ValueError("Logical time column %s is not in column list" %l_time_column)
        if r_time_column not in columns:
            raise ValueError("Real time column %s is not in column list" %r_time_column)
        with self.conn:
            c = self.conn.cursor()
            already_created = False
            for row in c.execute("SELECT name FROM sqlite_master WHERE type='table';"):
                if (row[0] == table_name):
                    already_created = True
            if not already_created:
                c.execute("INSERT INTO meta_table VALUES(?, ?, ?, ?, ?, ?);",
                          (table_name, json.dumps(columns), json.dumps(dtypes), json.dumps(vtypes),
                           l_time_column, r_time_column))
                c.execute("CREATE TABLE IF NOT EXISTS {tn}({rt} INT, {lt} FLOAT);".format(tn=table_name,
                                                                                          rt=r_time_column,
                                                                                          lt=l_time_column))
                for i in range(len(columns)):
                    if (columns[i] != l_time_column and columns[i] != r_time_column):
                        #makes sure that this column hasn't already been added in initial creation
                        c.execute('ALTER TABLE {tn} ADD COLUMN "{cn}" "{ct}";'
                                  .format(tn=table_name, cn=columns[i], ct=dtypes[i]))
            else:
                print("This table has already been created!")
                #raise NameError("This table has already been created!")
            
    def get_table(self, table_name):
        """
        Gets the tab object with the given table name keeping track of the logical time and real time columns.
        """
        with self.conn:
            table_there = False
            c=self.conn.cursor()
            for row in c.execute('''SELECT table_name FROM meta_table;'''):
                if row[0] == table_name:
                    table_there = True
            if table_there:
                for row in c.execute('''SELECT l_time_column, r_time_column, table_name FROM meta_table 
                WHERE table_name IN ("{tn}");'''.format(tn=table_name)):
                    l_time_=str(row[0])
                    r_time_=str(row[1])
            else:
                raise ValueError("This table hasn't been made yet")
        the_returned_tab = tab.Tab(self.conn, table_name, l_time_, r_time_)
        return the_returned_tab

        
