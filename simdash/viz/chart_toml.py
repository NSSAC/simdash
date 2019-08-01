"""
Render charts from a toml config file.
"""
import datetime

import altair as alt
import toml

from ..database import database
'''
def charts_from_toml(db_file, config_file):
    """
    Generate a list of charts from a config file.

    Args:
        db_file: path to the database file
        config_file: path to the toml configuration file
    Returns:
        chart_list: a list containing every chart specified by the configuration file
    """
    chart_list = []
    dbase = database.Database(db_file)
    names_and_encodings_tup = read_toml_encodings(config_file)
    for i, table_name in enumerate(names_and_encodings_tup[0], 0):
        table_cols_vtypes = dbase.get_table_cols_and_vtypes(table_name)
        chart_string = get_chart_string(names_and_encodings_tup[1][i], table_cols_vtypes[0], table_cols_vtypes[1])
        new_chart = generate_chart(dbase.get_table(table_name), chart_string)
        chart_list.append(new_chart.to_json())
    return []

def read_toml_encodings(config_file):
    """
    Read from the toml file get a tuple with table names and encoding dictionaries.

    Args:
        config_file: path to the toml configuration file
    Returns:
        returned_tuple: a tuple where the first element is the table_name
            and the second element is the encodings for that table.
    """
    table_list = []
    encodings_list = []
    returned_tuple = (table_list, encodings_list)
    with open(config_file, 'r', encoding='utf-8') as tfile:
        master_dict = toml.load(tfile)

    for table in master_dict['tab']:
        current_table = table['table_name']
        for poss_encoding in table['plot']:
            returned_tuple[0].append(current_table)
            returned_tuple[1].append(poss_encoding)

    return returned_tuple

def get_chart_string(encodings, columns, vtypes):
    """
    Creates a string that can be executed to make an Altair chart.

    Args:
        encodings: a dictionary with the specific encodings for this chart
        columns: a list of the columns in the table
        vtypes: a list of vtypes in the table
    Returns:
        ret_string: a string that can be executed to create an altair chart named ret_chart
    """
    if 'title' in encodings:
        exec_list = [f"ret_chart = alt.Chart(dframe, title={encodings['title']}).mark_{encodings['mark']}().encode("]
    else:
        exec_list = [f"ret_chart = alt.Chart(dframe).mark_{encodings['mark']}().encode("]
    if 'x' in encodings:
        exec_list.append(f"x='{encodings['x']}:{vtypes[columns.index(encodings['x'])]}', ")
    if 'y' in encodings:
        exec_list.append(f"y='{encodings['y']}:{vtypes[columns.index(encodings['y'])]}',")
    if 'y2' in encodings:
        exec_list.append(f"'y2={encodings['y2']}:{vtypes[columns.index(encodings['y2'])]}',")
    if 'x2' in encodings:
        exec_list.append(f"'x2={encodings['x2']}:{vtypes[columns.index(encodings['x2'])]}',")
    exec_list.append(")")
    ret_string = ''.join(exec_list)
    print(ret_string)
    return ret_string

def generate_chart(table, chart_string):
    """
    Create an altair chart from a string.
    """
    dframe = table.to_pandas()
    #may need to melt the dframe here
    d = {}
    exec(chart_string, d)
    return d['ret_chart']
'''

def create_all_charts_from_toml(db_file, config_file):
    """
    Create Altair charts from a database and toml config file.

    Args:
        db_file: String of path to database file
        config_file: String of path to configuration file
            config file should
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
        chart_list: a list of the json encodings of all of the Altair charts
    """
    chart_list = []
    dbase = database.Database(db_file)
    with open(config_file, 'r', encoding='utf-8') as tfile:
        master_dict = toml.load(tfile)

    for table in master_dict['tab']:
        #Assumes encodings for x and y variables have not been provided
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
