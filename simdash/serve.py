"""
SimDash server.
"""

import click
from flask import Flask, flash, render_template, request

import pandas as pd
import altair as alt

from . import cli_main
from .database import database
from .viz import chart_toml, viz

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q*z\n\xec]/'
CONFIG_PATH = None
DB_PATH = None

@app.route("/displayconfig/")
def display_from_config():
    """
    Display specified charts from config file.
    """
    chart_list = chart_toml.create_toml_charts_without_encodings(DB_PATH, CONFIG_PATH)
    num_list = list(range(len(chart_list)))
    return render_template("config_child.html", on_config=True, chart_list=chart_list, chart_label_list=num_list)

@app.route("/pid/", methods=['GET', 'POST'])
def display_pids():
    """
    Renders onto the HTML page the selected PID charts.
    """
    if DB_PATH is None:
        return render_template("no_pid_child.html")

    the_db = database.Database(DB_PATH)
    if not the_db.check_if_table_exists("displayed_charts"):
        the_db.make_table("displayed_charts", ["l_time", "r_time", "chart_name"], ["FLOAT", "INT", "TEXT"],
                          ["Q", "T", "N"])

    chart_tab = the_db.get_table("displayed_charts")
    chart_dframe = chart_tab.to_pandas()
    chart_label_list = chart_dframe['chart_name'].tolist()
    chart_list = []
    for title in chart_label_list:
        chart_list.append(viz.make_pid_chart(DB_PATH, title))

    if request.method == 'GET':    
        return render_template("pid_child.html", on_pids=True, chart_label_list=chart_label_list, chart_list=chart_list)
    else:
        if request.form.get('UserValue') is not None:
            user_value = request.form.get('UserValue')
            pid_value = request.form.get('PIDValue')
            table_name = f"{user_value}pid{pid_value}"
            if the_db.check_if_table_exists(table_name):
                chart_tab.append(chart_name=table_name)
                chart_label_list.append(table_name)
                chart_list.append(viz.make_pid_chart(DB_PATH, table_name))
            else:
                flash("This PID does not exist!")

        if request.form.getlist('chartcheck') is not None:
            for item in request.form.getlist('chartcheck'):
                chart_list.pop(chart_label_list.index(item))
                chart_label_list.remove(item)
                with the_db.conn:
                    curs = the_db.conn.cursor()
                    sql = f'DELETE FROM displayed_charts WHERE chart_name="{item}"'
                    curs.execute(sql)

        if request.form.get('remove_all') is not None:
            chart_label_list = []
            chart_list = []
            the_db.remove_table("displayed_charts")

        return render_template("pid_child.html", on_pids=True, chart_label_list=chart_label_list, chart_list=chart_list)

@app.route("/")
def display_homepage():
    """
    Display the homepage with a toolbar to access other elements of the server.
    """
    return render_template("toolbar_template.html", on_home=True)

@app.route("/embed")
def display_embed():
    return render_template("embed_child.html", on_embed=True)

@app.route("/sys_usage")
def display_sys_usage():
    the_chart = viz.get_system_charts(DB_PATH)
    return render_template("system_usage_child.html", the_chart=the_chart, on_sys_usage=True)

@cli_main.command()
@click.option("-h", "--host", default="localhost",
              help="Host to bind to.")
@click.option("-p", "--port", default=8888,
              help="Port to bind to.")
@click.option("-c", "--config", help="Path to config file")
@click.option("-d", "--database1", help="Path to database file")
def serve(host, port, config, database1):
    """
    Start the local simdash server.
    """
    global DB_PATH
    global CONFIG_PATH
    DB_PATH = database1
    CONFIG_PATH = config
    print(DB_PATH)
    print(CONFIG_PATH)
    app.run(host=host, port=port, debug=True)
