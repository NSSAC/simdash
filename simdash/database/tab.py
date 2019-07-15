import sqlite3
import json
import db
import pandas as pd

class Tab:

    """
    Tabs are associated with specific tables in an SQL database.
    
    Tabs allow users to append to these tables in a variety of ways as well as
    turn the table into a pandas dataframe and access the tab's length.
    """
    
    def __init__(self, conn, table_name, l_column, r_column):
        """Initializes the tab which points to a specific table in an SQL database."""

        self.table_name = table_name
        self.conn = conn
        self.l_column = l_column #String tracks name of logical time column
        self.r_column = r_column #String tracks name of real time column
        self.logical_time = 0.0
        with self.conn: # checks for highest logical time and adjusts
            c = self.conn.cursor()
            for row in c.execute("SELECT {lc} FROM {tn}".format(lc=self.l_column, tn=self.table_name)):
                if row[0] > self.logical_time:
                    self.logical_time = row[0]

    def append(self, l_time=None, r_time=None, **kwargs):
        """
        Appends to the tab the desired data.

        Uses column names in the kwargs, use l_time and r_time to specify a certain time quality
        If the Logical time is not specified, the highest logical time in the table
        will be incremented by 1.  If real time is not specified, the real time
        will be taken as a pandas timestamp with the current local time.
        """
        columns = []
        keys_list = []
        values_list = []
        for key, value in kwargs.items():
            columns.append(key)

        if r_time and self.r_column in columns:
            raise ValueError("Too many real time arguments")
        if l_time and self.l_column in columns:
            raise ValueError("Too many logical time arguments")
        if r_time is None and self.r_column not in columns: # no real time specified so real_time is now
            keys_list.append(self.r_column)
            values_list.append(pd.Timestamp("now").timestamp())
        elif r_time is None and self.r_column in columns: #r_time was none, but value was passed in kwargs
            keys_list.append(self.r_column)
            values_list.append(kwargs[self.r_column])
        else: #r_time was specified
            keys_list.append(self.r_column)
            values_list.append(r_time)
        if l_time is None and self.l_column not in columns: #no logical time specified so logical_time increments 1.0
            self.logical_time += 1.0
            keys_list.append(self.l_column)
            values_list.append(self.logical_time)
        elif l_time is None and self.l_column in columns: #l_time was none, but value was passed in kwargs
            the_time = kwargs[self.l_column]
            if the_time > self.logical_time:
                self.logical_time = the_time #marks l_time to this new value
            keys_list.append(self.l_column)
            values_list.append(the_time)
        else: #l_time was passed in the header
            if l_time > self.logical_time:
                self.logical_time = l_time #marks l_time to this new value
            keys_list.append(self.l_column)
            values_list.append(l_time)
        for key, value in kwargs.items():
            if key == self.l_column or key == self.r_column:
                pass #these values were already dealt with in the above if/else statements
            else:
                keys_list.append(key)
                values_list.append(value)
        sql_string = "INSERT INTO {tn}(".format(tn=self.table_name)
        for col in keys_list:
            if col == keys_list[-1]:
                sql_string += "%s) VALUES(" %col
            else:
                sql_string += "%s, " %col
        for val in values_list:
            if val == values_list[-1]:
                sql_string += "?);"
            else:
                sql_string += "?, "
        values_tuple = tuple(values_list)
        with self.conn:
            c = self.conn.cursor()
            c.execute(sql_string, values_tuple)
            
    def len(self):
        """Returns the length of the table in the form of length of the pandas dataframe (number of data points)"""
        query = 'SELECT * FROM "%s"' %self.table_name 
        dframe = pd.read_sql(query, self.conn)
        return len(dframe.index)

    def to_pandas(self):
        """Returns a reindexed Pandas Dataframe (reindexed so that any of its values can be used in charts)"""
        query = 'SELECT * FROM "%s"' %self.table_name
        dframe = pd.read_sql(query, self.conn)
        dframe.reindex
        return dframe
