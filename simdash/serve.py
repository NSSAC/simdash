"""
SimDash server.
"""

import time

import click
from flask import Flask, render_template

import pandas as pd
import altair as alt

from . import cli_main
from .viz import viz

app = Flask(__name__)

@app.route("/")
def route_():
    """
    Display the root page.
    """

    data = pd.DataFrame({
        'x': ['A', 'B', 'C', 'D', 'E'],
        'y': [5, 3, 6, 7, 2]
    })

    chart = alt.Chart(data).mark_bar().encode(
        x='x',
        y='y',
    )

    fmt = """
<!DOCTYPE html>
<html>
  <head>
    <title>Embedding Vega-Lite</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.4.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@3.3.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@4.2.0"></script>
  </head>
  <body>
    <div id="vis"></div>

    <script type="text/javascript">
      var yourVlSpec = %s;
      vegaEmbed('#vis', yourVlSpec);
    </script>
  </body>
</html>
    """.strip()

    return fmt % (chart.to_json(),)

@app.route("/pids/<path:databasename>/")
def pids(databasename):
    """
    Display the relative PID information from the database file.
    """
    databasename = "/%s" %databasename
    the_charts = viz.get_real_time_charts(databasename)
    num_list = list(range(len(the_charts)))
    return render_template("temp1.html", chart_list=the_charts, num_list=num_list)
        
    
@cli_main.command()
@click.option("-h", "--host", default="localhost",
              help="Host to bind to.")
@click.option("-p", "--port", default=8888,
              help="Port to bind to.")
def serve(host, port):
    """
    Start the local simdash server.
    """

    app.run(host=host, port=port, debug=True)
