"""
Tables are associated with specific tables in an SQL database.

Tables allow users to append to these SQL tables in a variety of ways as well as
turn the table into a pandas dataframe and access the table's length.
"""
import pandas as pd

class Table:

    """
    Tables are associated with specific tables in an SQL database.

    Tables allow users to append to these SQL tables in a variety of ways as well as
    turn the table into a pandas dataframe and access the table's length.

    Attributes:
        table_name (str): the name of the table within the SQL file
        conn (sqlite3.connection): holds the connection to the SQL file
        l_column (str): holds the name of the logical time column
        r_column (str): holds the name of the real time column
        logical_time (float): holds the value of the highest logical time in the table
            so that when an append is called without specifying the l_time or logical time
            column, this is incremented by 1 and attached to the data point
    """

    def __init__(self, conn, table_name, l_column, r_column):
        """
        Initializes the Table which points to a specific table in an SQL database.

        Args:
            conn (sqlite3.connection): the connection to the SQL file
            table_name (str): the name of the table that you wish to access
            l_column (str): the name of the logical time column so that the append statement
                can understand what is being input
            r_column (str): the name of the real time column so that the append statement can
                understand what is being input
        """
        self.table_name = table_name
        self.conn = conn
        self.l_column = l_column
        self.r_column = r_column
        self.logical_time = 0.0
        with self.conn: # checks for highest logical time and adjusts
            c = self.conn.cursor()
            sql_string_logic_time = "SELECT {lc} FROM {tn}".format(lc=self.l_column, tn=self.table_name)
            for row in c.execute(sql_string_logic_time):
                if row[0] > self.logical_time:
                    self.logical_time = row[0]

    def append(self, l_time=None, r_time=None, **kwargs):
        """
        Appends to the table the desired data.

        Uses column names in the kwargs, use l_time and r_time to specify a certain time quality
        If the Logical time is not specified, the highest logical time in the table
        will be incremented by 1.  If real time is not specified, the real time
        will be taken as a pandas timestamp with the current local time.

        Args:
            l_time (float): the logical time of the data point that is being uploaded,
                if not specified: this defaults to 1.0 higher than the highest already input
                logical time (starts at 0.0)
            r_time (int): the real time of the data point that is being uploaded,
                if not specified: this defaults to the pandas.Timestamp("now").timestamp()
            **kwargs: in the form of column_name=value, allows input into the table in any
                order as long as the column is specified
        Raises:
            ValueError: If the keyword l_time and the column for logical time are both used in one function call
            ValueError: If the keyword r_time and the column for real time are both used in one function call
        Returns:
            None
        """
        columns = []
        keys_list = []
        values_list = []
        for key, value in kwargs.items():
            columns.append(key)

        if r_time and self.r_column in columns:
            raise ValueError("Too many r_time arguments, must specify r_time only once in the function call")
        if l_time and self.l_column in columns:
            raise ValueError("Too many l_time arguments, must specify l_time only once in the function call")
        if r_time is None and self.r_column not in columns: # no real time specified so real time is now
            keys_list.append(self.r_column)
            values_list.append(pd.Timestamp("now").timestamp())
        elif r_time is None and self.r_column in columns: # r_time was none, but value was passed in kwargs
            keys_list.append(self.r_column)
            values_list.append(kwargs[self.r_column])
        else: #r_time was specified
            keys_list.append(self.r_column)
            values_list.append(r_time)
        if l_time is None and self.l_column not in columns: # no logical time specified so logical_time increments 1.0
            self.logical_time += 1.0
            keys_list.append(self.l_column)
            values_list.append(self.logical_time)
        elif l_time is None and self.l_column in columns: # l_time was none, but value was passed in kwargs
            the_time = kwargs[self.l_column]
            if the_time > self.logical_time:
                self.logical_time = the_time 
            keys_list.append(self.l_column)
            values_list.append(the_time)
        else: # l_time was passed in the header
            if l_time > self.logical_time:
                self.logical_time = l_time # marks l_time to this new value
            keys_list.append(self.l_column)
            values_list.append(l_time)
        for key, value in kwargs.items():
            if key in (self.l_column, self.r_column):
                pass # these values were already dealt with in the above if/else statements
            else:
                keys_list.append(key)
                values_list.append(value)
        sql_string = "INSERT INTO {tn}(".format(tn=self.table_name)
        for col in keys_list:
            if col == keys_list[-1]:
                sql_string += "%s) VALUES(" %col
            else:
                sql_string += "%s, " %col
        for i, val in enumerate(values_list, 1):
            if i == len(values_list):
                sql_string += "?);"
            else:
                sql_string += "?, "
        values_tuple = tuple(values_list)
        with self.conn:
            c = self.conn.cursor()
            c.execute(sql_string, values_tuple)

    def len(self):
        """
        Determines length of the table in the form of length of the pandas dataframe (number of data points).

        Returns:
            the_length (int): the length of the table
        """
        query = 'SELECT * FROM "%s"' %self.table_name
        dframe = pd.read_sql(query, self.conn)
        the_length = len(dframe.index)
        return the_length

    def to_pandas(self):
        """
        Creates and returns a  Pandas DataFrame (reindexed so that any of its values can be used in charts).

        Returns:
            dframe (pd.DataFrame): a Pandas DataFrame with all of the data from the table of the SQL file
        """
        query = 'SELECT * FROM "%s"' %self.table_name
        dframe = pd.read_sql(query, self.conn)
        dframe.reindex()
        return dframe
