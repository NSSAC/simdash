"""
The visualization interface for creating charts from a database.
"""
import datetime
import os

import altair as alt
import pandas as pd

from ..database import database

def make_pid_chart(db_name, table_name):
    """
    Returns a CPU and memory chart directly from a getpid database.

    Args:
        db_name: path to database file
        table_name: name of table within database file to create a PID chart from
    """
    the_db = database.Database(db_name)
    the_tab = the_db.get_table(table_name)
    dframe = the_tab.to_pandas()
    dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
    columns = list(dframe.columns)
    dframe2 = dframe.melt(id_vars=[columns[0], columns[1]], var_name='usage', value_name='percent')
    
    some_chart = alt.Chart(dframe2).mark_line(interpolate='basis').encode(
            x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
            y=alt.Y('percent:Q', title="Percentage Used",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
            color=alt.Color('usage:O', scale=alt.Scale(range=['salmon', 'steelblue'])),
            tooltip=['usage:O', 'real_time:T']
        ).properties(width=650, height=400)
    return some_chart.to_json()

def get_pid_charts(db_name):
    """
    Returns the a list of layered memory and cpu charts, one for every PID in the database.

    Args:
        db_name: path to the database file
    """
    chart_list = []
    the_db = database.Database(db_name)
    table_list = the_db.get_table_list()
    for tab in table_list:
        the_table = the_db.get_table(tab)
        dframe = the_table.to_pandas()
        dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
        columns = list(dframe.columns)
        dframe2 = dframe.melt(id_vars=[columns[0], columns[1]], var_name='usage', value_name='percent')
        some_chart = alt.Chart(dframe2).mark_line(interpolate='basis').encode(
            x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
            y=alt.Y('percent:Q', title="Percentage Used",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
            color=alt.Color('usage:O', scale=alt.Scale(range=['salmon', 'steelblue'])),
            tooltip=['usage:O', 'real_time:T']
        ).properties(width=650, height=400)
        the_mem_chart = make_mem_chart(dframe)
        the_cpu_chart = make_cpu_chart(dframe)
        final_chart = alt.layer(the_cpu_chart, the_mem_chart).properties(width=650, height=400)
        final_chart2 = final_chart & some_chart
        chart_list.append(some_chart.to_json())
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

def get_system_charts(db_name):
    """
    Produce system charts from file where getpid --system is run.
    
    Produce charts for cpu load, average load, physical memory usage, and swap memory usage.
    Args:
        db_name: Path to the database file
    Returns:
        One Altair chart object containing each of the graphs hconcat and vconcat together
    """
    the_db = database.Database(db_name)
    the_sys_tab = the_db.get_table("sys_usage")
    dframe = the_sys_tab.to_pandas()
    dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
    brush = alt.selection_interval(encodings=['x'])
    brush2 = alt.selection_interval(encodings=['x'])
    brush3 = alt.selection_interval(encodings=['x'])
    brush4 = alt.selection_interval(encodings=['x'])
    cpu_load_chart = make_cpu_load_chart(dframe)
    load_avg_chart = make_load_avg_chart(dframe)
    phys_mem_chart = make_phys_mem_chart(dframe)
    swap_mem_chart = make_swap_mem_chart(dframe)
    upper = alt.hconcat(phys_mem_chart, swap_mem_chart)
    lower = alt.hconcat(cpu_load_chart, load_avg_chart)
    final_chart = alt.vconcat(lower, upper)
    
    return(final_chart.to_json())

def make_cpu_load_chart(dframe):
    """
    Create the chart for visualizing CPU load.

    Args:
        dframe: pandas dframe from getpid --system database
    Returns:
        An Altair chart object with time on the x axis and cpu_load on the y axis
    """
    num_cpus = dframe.iloc[0, 3]
    the_chart = alt.Chart(dframe).mark_area(interpolate='linear').encode(
        x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
        y=alt.Y('cpu_load:Q', title="CPU Load", scale=alt.Scale(domain=[0, num_cpus]), stack=None),
    )
    return the_chart

def make_load_avg_chart(dframe):
    """
    Create the chart for visualizing load average.

    Args:
        dframe: pandas dframe from getpid --system database
    Returns:
        An Altair chart object with time on the x axis and load average on the y axis
    """
    the_chart = alt.Chart(dframe).mark_area(interpolate='linear').encode(
        x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
        y=alt.Y('load_avg:Q', title="Load Average", stack=None),
    )
    return the_chart

def make_phys_mem_chart(dframe):
    """
    Create the chart for visualizing physical memory usage.

    Args:
        dframe: pandas dframe from getpid --system database
    Returns:
        An Altair chart object with time on the x axis and used and available memory usage on the y axis
    """
    real_time = dframe['real_time']
    logic_time = dframe['logic_time']
    used_phys_mem = dframe['used_phys_mem']
    total_phys_mem = dframe['total_phys_mem']
    new_dframe = pd.DataFrame({'real_time': real_time, 'logic_time': logic_time,
                               'total_phys_mem': total_phys_mem, 'used_phys_mem': used_phys_mem})
    dframe2 = new_dframe.melt(id_vars=['real_time', 'logic_time'], var_name='type', value_name='mem_usage')
    the_chart = alt.Chart(dframe2).mark_area(interpolate='linear').encode(
        x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
        y=alt.Y('mem_usage:Q', title="Physical Memory", stack=None),
        color=alt.Color('type:O', scale=alt.Scale(range=['steelblue', 'salmon'])),
    )
    return the_chart

def make_swap_mem_chart(dframe):
    """
    Create the chart for visualizing swap memory.

    Args:
        dframe: pandas dframe from getpid --system database
    Returns:
        An Altair chart object with time on the x axis and swap memory usage on the y axis
    """
    total_swap_mem = dframe.iloc[0, 8]
    the_chart = alt.Chart(dframe).mark_area().encode(
        x=alt.X('yearmonthdatehoursminutes(real_time):T', title="Real Time",
                    axis=alt.Axis(labelFontSize=12.0, titleFontSize=14.0)),
        y=alt.Y('used_swap_mem:Q', title="Swap Memory", scale=alt.Scale(domain=[0, total_swap_mem])),
    )
    return the_chart
