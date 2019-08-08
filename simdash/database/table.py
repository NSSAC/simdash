"""
A Table is temporal dataframe with values associated with changing time.

A Table is a collection of values associated with time.
It is the data structure used to store changing values.
"""

import json
import sqlite3
import pandas as pd


class Table:
    """
    A Table is temporal dataframe with values associated with changing time.

    A Table is a collection of values associated with time.
    It is the data structure used to store changing values.

    Attributes:
        table_name: the name of the table
        columns: list of data columns
    """
    def __init__(self, filename, table_name, l_column, r_column):
        self.table_name = table_name
        self.l_column_ = l_column
        self.r_column_ = r_column

        self.conn_ = sqlite3.connect(filename)
        self.conn_.execute("PRAGMA journal_mode=wal")

        # Load the column names
        with self.conn_:
            sql = f"SELECT columns from meta_table where table_name = ?;"
            cur = self.conn_.execute(sql, (self.table_name,))
            columns = cur.fetchone()[0]
            columns = json.loads(columns)
            self.columns = columns

        # Load the max logical time
        with self.conn_:
            sql = f"SELECT max({self.l_column_}) FROM {self.table_name};"
            cur = self.conn_.execute(sql)
            try:
                self.logical_time = float(cur.fetchone()[0])
                if self.logical_time is None:
                    self.logical_time = 0.0
            except TypeError:
                self.logical_time = 0.0

        # Construct the sql
        sql = f"INSERT INTO {self.table_name} VALUES (%s);"
        sql = sql % (",".join(["?"] * len(self.columns)))
        self.insert_sql_ = sql

    def append(self, l_time=None, r_time=None, *args, **kwargs):
        """
        Append a row of values to the table.

        Use column names in the kwargs, use l_time and r_time to specify a desired time
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
        """

        if l_time is not None:
            if self.l_column_ in kwargs:
                raise ValueError(f"Only one of l_time or {self.l_column_} must be specified")
            kwargs[self.l_column_] = l_time

        if r_time is not None:
            if self.r_column_ in kwargs:
                raise ValueError(f"Only one of r_time or {self.r_column_} must be specified")
            kwargs[self.r_column_] = r_time

        for key, val in zip(self.columns[2:], args):
            if key in kwargs:
                raise ValueError(f"Value for column {key} is specified twice")
            kwargs[key] = val

        # Convert rtime to timestamp
        if r_time is None:
            r_time = kwargs.get(self.r_column_, None)
        if r_time is None:
            r_time = pd.Timestamp("now")
        if not isinstance(r_time, pd.Timestamp):
            r_time = pd.Timestamp(r_time)
        r_time = r_time.timestamp()
        kwargs[self.r_column_] = r_time

        # Set default l_time
        if self.l_column_ not in kwargs:
            kwargs[self.l_column_] = self.logical_time + 1.0
        self.logical_time = kwargs[self.l_column_]

        # Construct the parameters
        params = tuple(kwargs.get(param, None) for param in self.columns)

        # Execute the sql
        with self.conn_:
            self.conn_.execute(self.insert_sql_, params)

    def len(self):
        """
        Get the length of the table which is equivalent to the number of datapoints.

        Returns:
            the_length (int): the length of the table
        """
        query = 'SELECT * FROM "%s"' %self.table_name
        dframe = pd.read_sql(query, self.conn_)
        the_length = len(dframe.index)
        return the_length

    def to_pandas(self):
        """
        Create and return a Pandas DataFrame (reindexed so that any of its values can be used in charts).

        Returns:
            dframe (pd.DataFrame): a Pandas DataFrame with all of the data from the table of the SQL file
        """
        query = 'SELECT * FROM "%s"' %self.table_name
        dframe = pd.read_sql(query, self.conn_)
        dframe.reindex()
        return dframe
