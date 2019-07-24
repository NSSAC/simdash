"""
The visualization interface for creating charts from a database.
"""
import datetime

import altair as alt

from ..database import database

def get_pid_charts(db_name):
    """
    Returns the memory and cpu charts layered on top of each other for easy viewing.
    """
    chart_list = []
    the_db = database.Database(db_name)
    table_list = the_db.get_table_list()
    for tab in table_list:
        the_table = the_db.get_table(tab)
        dframe = the_table.to_pandas()
        dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
        the_mem_chart = make_mem_chart(dframe)
        the_cpu_chart = make_cpu_chart(dframe)
        final_chart = alt.layer(the_cpu_chart, the_mem_chart).properties(width=650, height=400)
        chart_list.append(final_chart.to_json())
    return chart_list

def make_mem_chart(dframe):
    """
    Make a chart with x as real_time and y as memory percentage, this chart has a right side y axis.
    """
    ret_chart = alt.Chart(dframe).mark_line(interpolate='basis', color='blue').encode(
        x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
        y=alt.Y('mem_percent:Q', title="Memory Percentage",
                axis=alt.Axis(orient='right', labelFontSize=12.0, titleFontSize=14.0)),
        tooltip=['real_time:T', 'mem_percent:Q', 'cpu_percent:Q']
    )
    return ret_chart

def make_cpu_chart(dframe):
    """
    Make a chart with x as real_time and y as cpu percentage.
    """
    ret_chart = alt.Chart(dframe).mark_line(interpolate='basis', color='red').encode(
        x=alt.X('real_time:T'),
        y=alt.Y('cpu_percent:Q', title="CPU Percentage",
                axis=alt.Axis(orient='left', labelFontSize=12.0, titleFontSize=14.0)),
        tooltip=['real_time:T', 'mem_percent:Q', 'cpu_percent:Q']
    )
    return ret_chart

def get_real_time_charts(db_name):
    """
    Create all the charts from a simdash database using the real time variable on the x axis.
    """
    the_db = database.Database(db_name)
    tup = the_db.get_tables_and_info()
    tab_list = tup[0]
    col_list = tup[1]
    vtype_list = tup[2]
    chart_list = []
    for i in range(len(tab_list)):
        the_table = the_db.get_table(tab_list[i])
        cols = col_list[i]
        vtypes = vtype_list[i]
        dframe = the_table.to_pandas()
        dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
        for j in range(2, len(cols)):
            chart_list.append(make_generic_circle_chart(dframe, cols[1], vtypes[1], cols[j], vtypes[j]))
    return chart_list

def make_generic_circle_chart(dframe, x_col, x_type, y_col, y_type):
    """
    Create a generic circle chart with the passed in dataframe, columns and column types.

    Args:
        dframe: pandas dataframe with the desired data to create the chart
        x_col: name of the x column in the dframe
        x_type: type of x data ("Q", "T", "O", or "N")
        y_col: name of the y column in the dframe
        y_type: type of the y data ("Q", "T", "O" or "N")
    """
    ret_chart = alt.Chart(dframe).mark_circle().encode(
        x=f'{x_col}:{x_type}',
        y=f'{y_col}:{y_type}',
        tooltip=[f'{x_col}:{x_type}', f'{y_col}:{y_type}']
    )
    return ret_chart.to_json()
