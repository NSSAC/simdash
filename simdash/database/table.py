"""
Tables are associated with specific tables in an SQL database.

Tables allow users to append to these SQL tables in a variety of ways as well as
turn the table into a pandas dataframe and access the table's length.
"""

import json
import pandas as pd

class Table:
    """
    A Table is temporal dataframe with values associated with changing time.

    A Table is a collection of values associated with time.
    It is the data structure used to store changing values.

    Attributes:
        table_name: the name of the table
        columns: list of data columns
        l_column: the name of the logical time column
        r_column: the name of the real time column
    """

    def __init__(self, conn, table_name, l_column, r_column):
        self.table_name = table_name
        self.l_column = l_column
        self.r_column = r_column

        self.conn = conn

        # Load the column names
        with self.conn:
            sql = f"SELECT columns from meta_table where table_name = ?"
            cur = self.conn.execute(sql, (self.table_name,))
            columns = cur.fetchone()[0]
            columns = json.loads(columns)
            self.columns = columns

        # Load the max logical time
        with self.conn:
            sql = f"SELECT max({self.l_column}) FROM {self.table_name}"
            cur = self.conn.execute(sql)
            try:
                self.logical_time = cur.fetchone()[0]
            except TypeError:
                self.logical_time = 0.0

        # Construct the sql
        sql = f"INSERT INTO {self.table_name} VALUES (%s)"
        sql = sql % (",".join(["?"] * len(self.columns)))
        self.insert_sql = sql

    def append(self, l_time=None, r_time=None, *args, **kwargs):
        """
        Append a row of values to the table.

        Uses column names in the kwargs, use l_time and r_time to specify a certain time quality
        If the Logical time is not specified, the highest logical time in the table
        will be incremented by 1.  If real time is not specified, the real time
        will be taken as a pandas timestamp with the current local time.

        logical_time (float): holds the value of the highest logical time in the table
            so that when an append is called without specifying the l_time or logical time
            column, this is incremented by 1 and attached to the data point

        Args:
            l_time (float): the logical time of the data point that is being uploaded,
                if not specified: this defaults to 1.0 higher than the highest already input
                logical time (starts at 0.0)
            r_time (int): the real time of the data point that is being uploaded,
                if not specified: this defaults to the pandas.Timestamp("now").timestamp()
            **kwargs: in the form of column_name=value, allows input into the table in any
                order as long as the column is specified
        """

        if l_time is not None:
            if self.l_column in kwargs:
                raise ValueError(f"Only one of l_time or {self.l_column} must be specified")
            kwargs[self.l_column] = l_time

        if r_time is not None:
            if self.r_column in kwargs:
                raise ValueError(f"Only one of r_time or {self.r_column} must be specified")
            kwargs[self.r_column] = r_time

        for k, v in zip(self.columns[2:], args):
            if k in kwargs:
                raise ValueError(f"Value for columne {k} is specific twice")
            kwargs[k] = v

        # Convert rtime to timestamp
        r_time = kwargs.get(self.r_column, None)
        if r_time is None:
            r_time = pd.Timestamp("now")
        if not isinstance(r_time, pd.Timestamp):
            r_time = pd.Timestamp(r_time)
        r_time = r_time.timestamp()
        kwargs[self.r_column] = r_time

        # Set default l_time
        if self.l_column not in kwargs:
            kwargs[self.l_column] = self.logical_time + 1.0
        self.logical_time = kwargs[self.l_column]

        # Construct the parameters
        params = (kwargs.get(param, None) for param in self.columns)

        # Execute the sql
        with self.conn:
            self.conn.execute(self.insert_sql, params)

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
