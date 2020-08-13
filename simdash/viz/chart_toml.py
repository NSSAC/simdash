"""
Render charts from a toml config file.
"""
import datetime

import altair as alt
import toml

from ..database import database

def create_all_charts_from_toml(db_file, config_file):
    """
    Create Altair charts from a database and toml config file.
    Args:
        db_file: String of path to database file
        config_file: String of path to configuration file
    Returns:
        chart_list: A list of altair chart objects converted to json
    """
    chart_list = []
    dbase = database.Database(db_file)
    with open(config_file, 'r', encoding='utf-8') as tfile:
        master_dict = toml.load(tfile)

    for table in master_dict['tab']:
        current_table = dbase.get_table(table['table_name'])
        dframe = current_table.to_pandas()
        dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
        the_chart = alt.Chart(dframe)
        marked_chart = getattr(the_chart, "mark_%s" %table['mark'])()
        encoding_dict = table['encode']
        encoded_chart = marked_chart.encode(**encoding_dict).properties(width=650, height=400)
        chart_list.append(encoded_chart.to_json())
    return chart_list

def create_toml_charts_without_encodings(db_file, config_file):
    """
    Create Altair Charts from a database and toml config file where encodings aren't specified.
    Args:
        db_file: path to database file
        config_file: path to Toml config file
    Returns:
        chart_list: a list of Altair chart objects converted to json
    """
    chart_list = []
    dbase = database.Database(db_file)
    with open(config_file, 'r', encoding='utf-8') as tfile:
        master_dict = toml.load(tfile)

    for table in master_dict['tab']:
        current_table=dbase.get_table(table['table_name'])
        dframe = current_table.to_pandas()
        dframe.iloc[:, 1] = dframe.iloc[:, 1].map(lambda x: datetime.datetime.fromtimestamp(x))
        cols_vtypes_tup = dbase.get_table_cols_and_vtypes(table['table_name'])
        the_chart = alt.Chart(dframe)
        marked_chart = getattr(the_chart, "mark_%s" %table['mark'])()
        encoding_dict = table['encode']
        encoding_dict['x'] = f"{encoding_dict['x']}:{cols_vtypes_tup[1][cols_vtypes_tup[0].index(encoding_dict['x'])]}"
        encoding_dict['y'] = f"{encoding_dict['y']}:{cols_vtypes_tup[1][cols_vtypes_tup[0].index(encoding_dict['y'])]}"
        encoded_chart = marked_chart.encode(**encoding_dict)
        chart_list.append(encoded_chart.to_json())
    return chart_list
