import pandas as pd
import pytest
import sqlite3
import os
from simdash.database.table import Table
from simdash.database.database import Database
from simdash.viz.chart_toml import create_toml_charts_without_encodings
 
def test_one_and_only():
    dbase_file = os.path.join(os.getcwd(), "test_toml_db.db")
    config_file = os.path.join(os.getcwd(), "test_toml.toml")
    the_db = Database(dbase_file)
    cols = ["logic_time", "real_time", "a", "b"]
    dtypes = ["FLOAT", "INT", "INT", "FLOAT"]
    vtypes = ["Q", "Q", "Q", "Q"]
    the_db.make_table("test_toml_table", cols, dtypes, vtypes)
    tab = the_db.get_table("test_toml_table")
    tab.append(a=1, b=2, real_time="08-06-2013")
    tab.append(a=3, b=2, real_time="08-07-2013")
    tab.append(a=4, b=4, real_time="08-08-2013")
    the_json = create_toml_charts_without_encodings(dbase_file, config_file)[0]
    the_json_part_1 = the_json[:203]
    the_json_part_2 = the_json[322:]
    chart_string = """{
  "$schema": "https://vega.github.io/schema/vega-lite/v3.3.0.json",
  "config": {
    "mark": {
      "tooltip": null
    },
    "view": {
      "height": 300,
      "width": 400
    }
  },
  "data": {"""
    chart_string_2 = """[
      {
        "a": 1,
        "b": 2.0,
        "logic_time": 1.0,
        "real_time": "2013-08-05T20:00:00"
      },
      {
        "a": 3,
        "b": 2.0,
        "logic_time": 2.0,
        "real_time": "2013-08-06T20:00:00"
      },
      {
        "a": 4,
        "b": 4.0,
        "logic_time": 3.0,
        "real_time": "2013-08-07T20:00:00"
      }
    ]
  },
  "encoding": {
    "x": {
      "field": "logic_time",
      "type": "quantitative"
    },
    "y": {
      "field": "a",
      "type": "quantitative"
    }
  },
  "mark": "line"
}"""
    assert chart_string == the_json_part_1
    assert chart_string_2 == the_json_part_2
    the_db.remove_table("test_toml_table")
