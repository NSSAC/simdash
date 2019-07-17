"""
Creates a Database based upon a given database file name

A Database initializes the meta_table and allows the creation of additional tables
within the given database file with the make_table function.  These additional tables
can then be retrieved and edited as a Tab with the get_table function.
"""
import json
import sqlite3
import warnings
import table

class Database:
    """
    Creates a Database based upon a given database file name

    A Database initializes the meta_table and allows the creation of additional tables
    within the given database file with the make_table function.  These additional tables
    can then be retrieved and edited as a Table with the get_table function.

    Attributes:
        conn (sqlite3 connection): The connection to the database file
    """
    
    def __init__(self, filename):
        """
        Initializes the database connection and meta table.

        Args:
            filename (str): Path to the file where you want the database to be created, should end in .db
        """
        if filename is not None:
            try:
                self.conn = sqlite3.connect(filename)
                with self.conn:
                    c = self.conn.cursor()
                    create_table_string = """CREATE TABLE IF NOT EXISTS
                    meta_table(table_name TEXT, columns TEXT, dtypes TEXT,
                    vtypes TEXT, l_time_column TEXT, r_time_column TEXT);"""
                    c.execute(create_table_string)
            except sqlite3.Error as er:
                print("SQLite error: %s" %er)

    def make_table(self, table_name, columns, dtypes, vtypes, l_time_column="l_time", r_time_column="r_time"):
        """
        Inserts a row into meta table with this information and creates the according table in the SQL file.

        Inserts a row into the meta table containing the columns, datatypes, variable types and the names of
        the logical and real time variables of the table "table_name". Then this table is created elsewhere
        in your SQL database.

        Args:
            table_name (str): name of your desired table, this will be it's name in the SQL file and
                proper care should be taken to ensure that the same table is not made twice
            columns (list(str)): The names of the columns of your new table.  Should include the logical
                time and real time columns even if they are specified in the function header.
            dtypes (list(str)): The datatypes or SQL datatypes of each of the columns that have been passed in.
                Supported now are INT, FLOAT, and TEXT
            vtypes (list(str)): The variable types or Altair encodings of each of the passed in columns, must be 'Q',
                'T', 'O', or 'N'
            l_time_column (str): the name of the column that represents the logical time of the graphic,
                defaults to 'l_time' and the value of this variable must also be in columns
            r_time_column (str): the name of the column that represents the temporal time of the graphic,
                defaults to 'r_time' and the value of this variable must also be in columns
        Raises:
            ValueError: If the length of columns, vtypes or dtypes is different
            ValueError: If any dtype is not 'INT', 'FLOAT' or 'TEXT'
            ValueError: If any vtype is not 'N', 'O', 'T', or 'Q'
            ValueError: If the logical time column is not in columns
            ValueError: If the real time column is not in columns
            UserWarning: If the table has already been created in the file
        Returns:
            None
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
        if l_time_column not in columns:
            raise ValueError("Logical time column %s must be in the column list" %l_time_column)
        if r_time_column not in columns:
            raise ValueError("Real time column %s must be in the column list" %r_time_column)
        with self.conn:
            c = self.conn.cursor()
            already_created = False
            sql_get_table_names = "SELECT name FROM sqlite_master WHERE type='table';"
            for row in c.execute(sql_get_table_names):
                if row[0] == table_name:
                    already_created = True
                    if already_created:
                        warnings.warn("This table has already been created", UserWarning)
                        return
            sql_insert_meta_string = "INSERT INTO meta_table VALUES(?, ?, ?, ?, ?, ?);"
            insert_meta_tuple = (table_name, json.dumps(columns), json.dumps(dtypes),
                                 json.dumps(vtypes), l_time_column, r_time_column)
            c.execute(sql_insert_meta_string, insert_meta_tuple)
            sql_create_table_string = "CREATE TABLE IF NOT EXISTS {tn}({rt} INT, {lt} FLOAT);"
            c.execute(sql_create_table_string.format(tn=table_name, rt=r_time_column, lt=l_time_column))
            for i, value in enumerate(columns, 0): # append all other columns onto the table
                if value not in(l_time_column, r_time_column):
                    sql_alter_table = 'ALTER TABLE {tn} ADD COLUMN "{cn}" "{ct}";'
                    c.execute(sql_alter_table.format(tn=table_name, cn=value, ct=dtypes[i]))
            return

    def get_table(self, table_name):
        """
        Gets the table object with the given table name.

        Often used so that a table object can be accessed, appended, and turned into a pandas DataFrame

        Args:
            table_name (str): the name of the table that you want to access
        Raises:
            ValueError: If the Table has not been made before calling get_table
        Returns:
            the_returned_tab (Table): The new Table object pointing to table_name in the SQL file
        """
        with self.conn:
            table_there = False
            c = self.conn.cursor()
            sql_get_names = "SELECT table_name FROM meta_table"
            for row in c.execute(sql_get_names):
                if row[0] == table_name:
                    table_there = True
            if table_there:
                sql_find_ltime_rtime = '''SELECT l_time_column, r_time_column, table_name FROM meta_table WHERE
                table_name IN ("{tn}");'''.format(tn=table_name)
                for row in c.execute(sql_find_ltime_rtime):
                    l_time_ = str(row[0])
                    r_time_ = str(row[1]) # keeping track of the logical and real time column names
            else:
                raise ValueError("This table hasn't been made yet. Make this table before getting it")
        the_returned_tab = table.Table(self.conn, table_name, l_time_, r_time_)
        return the_returned_tab

    def remove_table(self, table_name):
        """
        Removes the table from the SQL file and deletes its information from the meta_table file.

        Args:
            table_name (str): the name of the table that you want to delete
        Returns:
            None
        """
        with self.conn:
            c = self.conn.cursor()
            sql_drop_table = "DROP TABLE IF EXISTS {tn};".format(tn=table_name)
            sql_delete_from_meta = 'DELETE FROM meta_table WHERE table_name="{tn}"'.format(tn=table_name)
            c.execute(sql_drop_table)
            c.execute(sql_delete_from_meta)
