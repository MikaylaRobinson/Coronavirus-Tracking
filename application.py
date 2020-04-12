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
    cur.execute("SELECT sum(confirmed) FROM coronaworld where date_recorded = '2020-04-09' and country_or_region = 'US';")
    num = cur.fetchone()
    usa_cases_total = str(int(num[0]))

usa_deaths_total = 0
def get_usa_deaths():
    global usa_deaths_total
    cur.execute("SELECT sum(deaths) FROM coronaworld where date_recorded = '2020-04-09' and country_or_region = 'US';")
    num = cur.fetchone()
    usa_deaths_total = str(int(num[0]))

world_cases_total = 0
def get_world_cases():
    global world_cases_total
    cur.execute("SELECT sum(confirmed) FROM coronaworld where date_recorded = '2020-04-09';")
    num = cur.fetchone()
    world_cases_total = str(int(num[0]))

world_deaths_total = 0
def get_world_deaths():
    global world_deaths_total
    cur.execute("SELECT sum(deaths) FROM coronaworld where date_recorded = '2020-04-09';")
    num = cur.fetchone()
    world_deaths_total = str(int(num[0]))

world_recovered_total = 0
def get_world_recovered():
    global world_recovered_total
    cur.execute("SELECT sum(recovered) FROM coronaworld where date_recorded = '2020-04-09';")
    num = cur.fetchone()
    world_recovered_total = str(int(num[0]))

mapdata = None
def get_map_data():
    global mapdata
    cur.execute("Select * from coronaworld where country_or_region = 'US';")
    rows = cur.fetchall()
    mapdata = sql_to_json_all_columns(rows)

@app.route('/')
def index():
    return render_template('index.jinja.html')

@app.route('/api/country/aggregate/cases/usa')
def api_country_aggregate_cases_usa():
    return usa_cases_total

@app.route('/api/country/aggregate/deaths/usa')
def api_country_aggregate_deaths_usa():
    return usa_deaths_total

@app.route('/api/country/aggregate/cases/world')
def api_country_aggregate_cases_world():
    return world_cases_total

@app.route('/api/country/aggregate/deaths/world')
def api_country_aggregate_deaths_world():
    return world_deaths_total

@app.route('/api/country/aggregate/recovered/world')
def api_country_aggregate_recovered_world():
    return world_recovered_total

@app.route('/api/country/usa/graph', methods=['GET'])
def api_country_usa_count_graph():
    cur.execute("Select sum(confirmed), date_recorded from coronaworld where country_or_region = 'US' group by date_recorded;")
    rows = cur.fetchall()
    return prepare_graph_data(rows)

@app.route('/api/country/usa/mapdata')
def api_country_usa_map():
    return mapdata

@app.route('/api/country/graph/<country_name>', methods=['GET'])
def api_country_general(country_name):
    query = "Select sum(confirmed), date_recorded from coronaworld where country_or_region = %(country)s group by date_recorded;"
    cur.execute(query, {"country": country_name})
    rows = cur.fetchall()
    return prepare_graph_data(rows)


# Transforms rows returned from SQL query to json
def sql_to_json_all_columns(rows):
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

def sql_to_json_sum_date(rows):
    objects_list = []
    for row in rows:
        info_by_column = collections.OrderedDict()
        info_by_column['total_confirmed']=row[0]
        info_by_column['date']=row[1]

        objects_list.append(info_by_column)
    converted_json = json.dumps(objects_list, default=str)
    return converted_json

def prepare_graph_data(rows):
    new_json_data = []
    day_counter = 1
    for row in rows:
        if day_counter <= 25 and row[0] >= 50:
            info_by_column = collections.OrderedDict()
            info_by_column['day_count'] = day_counter
            info_by_column['total_confirmed']=row[0]
            info_by_column['date']= row[1]
            new_json_data.append(info_by_column)
            day_counter += 1
    converted_json = json.dumps(new_json_data, default=str)
    return converted_json

if __name__ == '__main__':
    get_usa_cases()
    get_usa_deaths()
    get_world_deaths()
    get_world_recovered()
    get_world_cases()
    get_map_data()
    app.debug = os.environ.get("DEBUG_MODE")
    app.run()