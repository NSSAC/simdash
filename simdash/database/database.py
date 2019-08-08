"""
A Database is a collection of Tables with a variable meta_table that holds information about all other tables.

A Database can create Tables while keeping track of each Table's columns and their respective Altair variable types.
"""

import json
import sqlite3
import warnings

from .table import Table

class Database:
    """
    A Database is a collection of Tables with a variable meta_table that holds information about all other tables.

    A Database can create Tables while keeping track of each Table's columns and their respective Altair variable types.
    """
    def __init__(self, filename):
        """
        Initialize the database connection and meta table.

        Args:
            filename: Path to the file, should end in .db
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
        Make a Table with the corresponding columns.

        Args:
            table_name: Name of table
                Tables with the same name should not be made twice
            columns: List of column names for the new table
                The first item in this list must be the logical time column
                The second item must be the real time column
            dtypes: List of the datatypes of each of the columns
                One of (INT, FLOAT, or TEXT)
            vtypes: List of the variable types (Altair encodings) of each of the columns,
                One of ('Q', 'T', 'O', 'N')
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
        Get the Table object with the specified name.

        Args:
            table_name: the name of the table
        Returns:
            the_returned_tab: The Table object associated with the passed table_name
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
        Remove the table and delete its information.

        Args:
            table_name: the name of the table that will be deleted
        """
        with self.conn:
            curs = self.conn.cursor()
            sql_drop_table = "DROP TABLE IF EXISTS {tn};".format(tn=table_name)
            sql_delete_from_meta = f'DELETE FROM meta_table WHERE table_name="{table_name}"'
            curs.execute(sql_drop_table)
            curs.execute(sql_delete_from_meta)

    def get_table_list(self):
        """
        Get a list of the tables in the database.

        Returns:
            A list of the tables in the database
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
        Get a tuple containing a list of all tables, a list of columns, and a list of vtypes from the database.

        Returns:
            A tuple containing (List of tables, List of columns, list of vtypes)
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

    def get_table_cols_and_vtypes(self, table_name):
        """
        Get a tuple containing the column list and vtype list from a specific table.

        Args:
            table_name: name of table that columns and vtypes are desired.
        Returns:
            A tuple containing (list of columns, list of vtypes)
        """
        sql = f"SELECT table_name, columns, vtypes FROM meta_table WHERE table_name='{table_name}'"
        with self.conn:
            curs = self.conn.cursor()
            curs.execute(sql)
            fetched = curs.fetchone()
            return (json.loads(fetched[1]), json.loads(fetched[2]))

    def check_if_table_exists(self, table_name):
        """
        Return True or False depending on if a table is present in a database.

        Args:
            table_name: name of table that is being checked
        """
        sql = f"SELECT table_name FROM meta_table WHERE table_name='{table_name}'"
        with self.conn:
            curs = self.conn.cursor()
            curs.execute(sql)
            return curs.fetchone() is not None
