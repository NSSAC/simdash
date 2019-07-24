"""
A Database is a collection of Tables with a variable meta_table that holds information about all the other tables.

A Database can create Tables while keeping track of each Table's columns and their respective Altair variable types.
This information is stored in the Database's "meta_table" and allows for the simple addition of data points
to each table.
"""

import json
import sqlite3
import warnings

from .table import Table

class Database:
    """
    A Database is a collection of Tables with a variable meta_table that holds information about all the other tables.

    A Database can create Tables while keeping track of each Table's columns and their respective Altair variable types.
    This information is stored in the Database's "meta_table" and allows for the simple addition of data points
    to each table.
    """
    def __init__(self, filename):
        """
        Initialize the database connection and meta table.

        Args:
            filename (str): Path to the file where you want the database to be created, should end in .db
        """
        if filename is not None:
            try:
                self.conn = sqlite3.connect(filename)
                with self.conn:
                    curs = self.conn.cursor()
                    create_table_string = """CREATE TABLE IF NOT EXISTS
                    meta_table(table_name TEXT, columns TEXT, dtypes TEXT,
                    vtypes TEXT, l_time_column TEXT, r_time_column TEXT);"""
                    curs.execute(create_table_string)
            except sqlite3.Error as err:
                print("SQLite error: %s" %err)
        self.filename = filename
        self.conn.execute("PRAGMA journal_mode=wal;")

    def make_table(self, table_name, columns, dtypes, vtypes):
        """
        Make a Table with the corresponding columns and add this new table's information to the meta table.

        The first item in columns must be the variable name that is used for logical time, and the second
        item must be the variable name that is used for real time.
        Args:
            table_name: Name of your desired table. Proper care should be taken to ensure that the same table
                 is not made twice
            columns (list(str)): Names of the columns of your new table. The first two items in this list must
                be the logical time column and the real time column respectively
            dtypes (list(str)): The datatypes of each of the columns that have been passed in. Supported now are
                INT, FLOAT, and TEXT.  Each index of dtypes should match with the column at that index in
                columns.
            vtypes (list(str)): The variable types or Altair encodings of each of the passed in columns, must be 'Q',
                'T', 'O', or 'N'.  Each index of vtypes should match with the column at that index in columns.
        """
        possible_dtype_list = ["INT", "FLOAT", "TEXT"]
        possible_vtype_list = ["Q", "T", "O", "N"]
        if not len(columns) == len(dtypes) == len(vtypes):
            raise ValueError("Length of columns, vtypes and dtypes must be the same")
        for value in dtypes:
            if value.upper() not in possible_dtype_list:
                raise ValueError("dytpe %s is not of the correct type, must be 'INT', 'FLOAT' or 'TEXT'" %value)
        for value in vtypes:
            if value not in possible_vtype_list:
                raise ValueError("vtype %s is not of the correct type, must be 'N' 'O', 'T' or 'Q'" %value)

        # create the meta_table
        with self.conn:
            curs = self.conn.cursor()
            sql_get_table_names = "SELECT name FROM sqlite_master WHERE type='table';"
            for row in curs.execute(sql_get_table_names):
                if row[0] == table_name:
                    warnings.warn("This table has already been created", UserWarning)
                    return
            sql_insert_meta_string = "INSERT INTO meta_table VALUES(?, ?, ?, ?, ?, ?);"
            insert_meta_tuple = (table_name, json.dumps(columns), json.dumps(dtypes),
                                 json.dumps(vtypes), columns[0], columns[1])
            curs.execute(sql_insert_meta_string, insert_meta_tuple)

            # Create the table
            sql_create_table_string = f"CREATE TABLE IF NOT EXISTS '{table_name}'({columns[0]} FLOAT);"
            curs.execute(sql_create_table_string)
            for i, value in enumerate(columns[1:], 1):
                sql_alter_table = 'ALTER TABLE {tn} ADD COLUMN "{cn}" "{ct}";'
                curs.execute(sql_alter_table.format(tn=table_name, cn=value, ct=dtypes[i]))
            return

    def get_table(self, table_name):
        """
        Get the Table with the specified name.

        Often used so that a Table can be accessed, appended, or turned into a pandas DataFrame.

        Args:
            table_name: the name of the table that you want to access
        Returns:
            the_returned_tab (Table): The Table object associated with the passed table_name
        """
        with self.conn:
            curs = self.conn.cursor()
            sql_get_table = f"SELECT table_name, l_time_column, r_time_column FROM meta_table WHERE table_name = ?;"
            curs.execute(sql_get_table, (table_name,))
            tup = curs.fetchone()

            # Keep track of logical time and real time col names
            if tup is not None:
                l_time_ = str(tup[1])
                r_time_ = str(tup[2])
            else:
                raise ValueError("This table hasn't been made yet. Make this table before getting it")
        the_returned_tab = Table(self.filename, table_name, l_time_, r_time_)
        return the_returned_tab

    def remove_table(self, table_name):
        """
        Remove the table and delete its information from the meta_table file.

        Args:
            table_name: the name of the table that will be deleted
        """
        with self.conn:
            curs = self.conn.cursor()
            sql_drop_table = "DROP TABLE IF EXISTS {tn};".format(tn=table_name)
            sql_delete_from_meta = 'DELETE FROM meta_table WHERE table_name="{tn}"'.format(tn=table_name)
            curs.execute(sql_drop_table)
            curs.execute(sql_delete_from_meta)

    def get_table_list(self):
        """
        Get a list of the tables in the database.
        """
        tab_list = []
        with self.conn:
            sql = "SELECT table_name FROM meta_table;"
            curs = self.conn.cursor()
            for row in curs.execute(sql):
                tab_list.append(row[0])
        return tab_list

    def get_tables_and_info(self):
        """
        Get a tuple containing a list of tables, a list of columns, and a list of vtypes in the database.
        """
        tab_list = []
        col_list = []
        vtype_list = []
        with self.conn:
            sql = "SELECT table_name, columns, vtypes from meta_table;"
            curs = self.conn.cursor()
            for row in curs.execute(sql):
                tab_list.append(row[0])
                col_list.append(json.loads(row[1]))
                vtype_list.append(json.loads(row[2]))
        return (tab_list, col_list, vtype_list)
