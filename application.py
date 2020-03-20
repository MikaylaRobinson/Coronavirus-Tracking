import json
import psycopg2
import os
import collections
from flask import Flask, render_template, redirect, request, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

# Initialize app
app = Flask(__name__)

# Pull from env
load_dotenv() 

# # Configure db connection
con = psycopg2.connect(database= os.getenv("database_name"), host= os.getenv("host_name"), port= os.getenv("port_number"), user =os.getenv("db_username"), password = os.getenv("user_password"))
cur = con.cursor()

usa_cases_total = 0
def get_usa_cases():
    global usa_cases_total
    cur.execute("SELECT sum(confirmed) FROM coronaworld where date_recorded = '2020-03-19' and country_or_region = 'US';")
    num = cur.fetchone()
    usa_cases_total = str(num[0])

italy_cases_total = 0
def get_italy_cases():
    global italy_cases_total
    cur.execute("SELECT sum(confirmed) FROM coronaworld where date_recorded = '2020-03-19' and country_or_region = 'Italy';")
    num = cur.fetchone()
    italy_cases_total = str(num[0])

usa_deaths_total = 0
def get_usa_deaths():
    global usa_deaths_total
    cur.execute("SELECT sum(deaths) FROM coronaworld where date_recorded = '2020-03-19' and country_or_region = 'US';")
    num = cur.fetchone()
    usa_deaths_total = str(num[0])

mapdata = None
def get_map_data():
    global mapdata
    cur.execute("Select * from coronaworld where country_or_region = 'US';")
    rows = cur.fetchall()
    mapdata = sql_to_json(rows)

@app.route('/')
def index():
    return render_template('index.jinja.html')

@app.route('/api/country/aggregate/cases/usa')
def api_country_aggregate_cases_usa():
    return usa_cases_total

@app.route('/api/country/aggregate/cases/italy')
def api_country_aggregate_cases_italy():
    return italy_cases_total

@app.route('/api/country/aggregate/deaths/usa')
def api_country_aggregate_deaths_usa():
    return usa_deaths_total

@app.route('/api/country/usa/graph', methods=['GET'])
def api_country_usa_count_graph():
    cur.execute("Select * from coronaworld where country_or_region = 'US' and confirmed > 100 order by date_recorded;")
    rows = cur.fetchall()
    return sql_to_json(rows)

@app.route('/api/country/usa/mapdata')
def api_country_usa_map():
    return mapdata

@app.route('/api/country/italy', methods=['GET'])
def api_country_italy():
    cur.execute("Select * from italycases limit 10;")
    rows = cur.fetchall()
    return sql_to_json(rows)


# Transforms rows returned from SQL query to json
def sql_to_json(rows):
    objects_list = []
    for row in rows:
        info_by_column = collections.OrderedDict()
        info_by_column['country_or_region']=row[0]
        info_by_column['province_or_state']=row[1]
        info_by_column['latitude']=row[2]
        info_by_column['longitude']=row[3]
        info_by_column['confirmed']=row[4]
        info_by_column['recovered']=row[5]
        info_by_column['deaths']=row[6]
        info_by_column['date_recorded']=row[7]

        objects_list.append(info_by_column)
    converted_json = json.dumps(objects_list, default=str)
    return converted_json

if __name__ == '__main__':
    get_usa_cases()
    get_usa_deaths()
    get_map_data()
    app.debug = os.environ.get("DEBUG_MODE")
    app.run()