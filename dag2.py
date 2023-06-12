# import libraries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import requests
import json
from datetime import datetime
import pytz


# get variables and current datetime
class Config():
    api_key = Variable.get("API_KEY")
    airport_api_key = Variable.get("X-RapidAPI-Key")
    airport_api_host = Variable.get("X-RapidAPI-Host")

    thai_tz = pytz.timezone("Asia/Bangkok")
    date = datetime.now(thai_tz).strftime("%Y-%m-%d")
    time = datetime.now(thai_tz).strftime("%Y%m%d_%H%M")
    datetime = datetime.now(thai_tz).strftime("%Y-%m-%d %H:%M:%S")


# define fetch live flight data through API function
def fetch_data_api():
    # API url
    url = f"https://airlabs.co/api/v9/flights?api_key={Config.api_key}&airline_icao=THA"
    # response from API
    response = requests.get(url=url)
    # convert to dictionary and select only response section
    result = response.json()["response"]
    # convert to json
    result_json = json.dumps(result)
    
    # write raw data to json file
    with open(f"live_flight_{Config.time}.json", "w") as f:
        f.write(result_json)

    # hook minio connection
    s3_hook = S3Hook(aws_conn_id = "minio")
    # load raw data to minio
    s3_hook.load_file(
        f"live_flight_{Config.time}.json",
        key = f"{Config.date}/{Config.time}.json",
        bucket_name = "aviation-project",
        replace = True
    )


# define download raw data from minio function
def download_raw_data(**context):
    # hook minio connection
    s3_hook = S3Hook(aws_conn_id = "minio")
    # download raw data
    raw_filename = s3_hook.download_file(
        key = f"{Config.date}/{Config.time}.json",
        bucket_name = "aviation-project"
    )
    # push raw data filename to xcom
    context["ti"].xcom_push(key = "raw_filename", value = raw_filename)


# define convert unix time to datetime function
def convert_time(unix_time):
    return datetime.fromtimestamp(unix_time, Config.thai_tz).strftime("%Y-%m-%d %H:%M:%S")


# define get airport name from API
def airport_name(airport_iata):
    # API url
    url = f"https://aviation-reference-data.p.rapidapi.com/airports/{airport_iata}"
    # headers
    headers = {"X-RapidAPI-Key": Config.airport_api_key, "X-RapidAPI-Host": Config.airport_api_host}
    # response from API
    response = requests.get(url=url, headers=headers)
    # convert to dictionary and select only name section
    result = response.json()["name"]
    return result


# define transform live flight raw data and load to temporary table in postgres function
def flight_transform(**context):
    # pull raw data file name from xcom
    raw_filename = context["ti"].xcom_pull(key = "raw_filename")
    # read raw data json file
    with open(raw_filename, "r") as f:
        flight_data = json.load(f)

    # define empty transformed live flight data dictionary
    live_flight_dict = {}
    # for loop to extract data from raw data dictionary
    for index, data in enumerate(flight_data):
        # Note: there is some case that no data of arrival airport. In this case, I define it as BKK.
        # try to get arrival airport
        try:
            arr_iata = data["arr_iata"]
        # except not have arrival airport -> define as BKK
        except KeyError:
            arr_iata = "BKK"
        
        # append data to dictionary
        live_flight_dict[index] = {
            "reg": data["reg_number"],
            "flight": data["flight_iata"],
            "dep_iata": data["dep_iata"],
            "arr_iata": arr_iata,
            "status": data["status"],
            "updated_unix": data["updated"]
        }
    
    # convert transformed live flight data to dataframe
    live_flight_df = pd.DataFrame(live_flight_dict).transpose()
    # get departure airport name column by applying airport_name function
    live_flight_df["dep_airport"] = live_flight_df.apply(lambda x: airport_name(x["dep_iata"]), axis = 1)
    # get arrival airport name column by applying airport_name function
    live_flight_df["arr_airport"] = live_flight_df.apply(lambda x: airport_name(x["arr_iata"]), axis = 1)
    # get last updated time column by applying convert_time function
    live_flight_df["updated"] = live_flight_df.apply(lambda x: convert_time(x["updated_unix"]), axis = 1)
    # drop dep_iata, arr_iata, updated_unix columns
    live_flight_df = live_flight_df.drop(["dep_iata", "arr_iata", "updated_unix"], axis = 1)

    # hook postgres connection
    postgres = PostgresHook(postgres_conn_id = "postgres")
    # get connection
    conn = postgres.get_conn()
    # get cursor
    cur = conn.cursor()

    # sql of insert into flight table (temporary table)
    flight_sql = """
        INSERT INTO flight (
            reg,
            flight,
            status,
            dep_airport,
            arr_airport,
            updated
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    # for loop to insert to flight table (temporary table)
    for row in live_flight_df.values:
        cur.execute(flight_sql, tuple(row))
    conn.commit()

    # close cursor and connection
    cur.close()
    conn.close()


# define get fleet and flight data from postgres
def transform_load(**context):
    # hook postgres connection
    postgres = PostgresHook(postgres_conn_id = "postgres")
    # postgres connection
    conn = postgres.get_conn()
    # get cursor
    cur = conn.cursor()

    # select reg, aircraft_type, remark from fleet table
    cur.execute("SELECT reg, aircraft_type, remark FROM fleet;")
    # fetch all of data
    fleet_result = cur.fetchall()
    # convert to pandas dataframe
    fleet_df = pd.DataFrame(fleet_result, columns = ["reg", "aircraft_type", "remark"])

    # select all data from flight table
    cur.execute("SELECT * from flight;")
    # fetch all of data
    flight_result = cur.fetchall()
    # convert to pandas dataframe
    flight_df = pd.DataFrame(flight_result, columns = ["reg", "flight", "status", "dep_airport", "arr_airport", "updated"])

    # close cursor and connection
    cur.close()
    conn.close()

    # enroute dataframe = merge fleet and flight dataframe with inner type
    enroute_df = fleet_df.merge(flight_df, how = "inner", on = "reg")
    # ground dataframe = fleet dataframe except enroute dataframe -> merge with outer
    ground_df = fleet_df.merge(flight_df, how="outer", on = "reg", indicator = True)
    # select row that is left
    ground_df = ground_df.query("_merge=='left_only'")
    # drop column that contain na and drop indicator column
    ground_df = ground_df.dropna(axis = 1).drop("_merge", axis=1)
    # add status column = ground
    ground_df["status"] = "ground"
    # add updated column = current datetime
    ground_df["updated"] = Config.datetime

    # hook mongodb connection
    mongo = MongoHook(mongo_conn_id = "mongo_default")
    # get connection
    conn = mongo.get_conn()
    # aviation database
    db = conn["aviation_db"]
    # aviation collection
    col = db["aviation_col"]

    # if collection is empty
    if col.count_documents({}) == 0:
        # insert enroute dataframe to mongodb
        col.insert_many(enroute_df.to_dict("reg"))
        # insert ground dataframe to mongodb
        col.insert_many(ground_df.to_dict("reg"))
    # else collection is not empty
    else:
        # for loop to update document in mongodb
        for row in ground_df.to_dict("reg"):
            # match with aircraft registration
            query = {"reg": row["reg"]}
            # drop flight, departure airport, arrival airport and update status, updated
            values = {"$unset": {"flight": "", "dep_airport": "", "arr_airport": ""},
                    "$set": {"status": row["status"], "updated": row["updated"]} }
            # update document
            col.update_one(query, values)

        # for loop to update document in mongodb
        for row in enroute_df.to_dict("reg"):
            # match with aircraft registration
            query = {"reg": row["reg"]}
            # update status, updated, flight, departure airport, arrival airport
            values = {"$set": {"status": row["status"], 
                               "updated": row["updated"], 
                               "flight": row["flight"], 
                               "dep_airport": row["dep_airport"], 
                               "arr_airport": row["arr_airport"]}}
            # update document
            col.update_one(query, values)


# define default arguments
default_args = {
    "owner": "WarmNatchapol",
    "start_date": datetime(2023, 6, 12, tzinfo=Config.thai_tz)
}

# DAG
with DAG(
    "Aviation_DAG_2",
    default_args = default_args,
    schedule_interval = "0 * * * *"
) as dag:

    # fetch live flight data through API and load to minio task
    fetch_data_api = PythonOperator(
        task_id = "fetch_data_api",
        python_callable = fetch_data_api
    )

    # download live flight raw data from minio task
    download_raw_data = PythonOperator(
        task_id = "download_raw_data",
        python_callable = download_raw_data
    )

    # create temporary table in postgres task
    create_table_postgres = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "postgres",
        sql = """
            CREATE TABLE IF NOT EXISTS flight (
                reg VARCHAR(6) PRIMARY KEY,
                flight VARCHAR(6),
                status VARCHAR(20),
                dep_airport VARCHAR(50),
                arr_airport VARCHAR(50),
                updated TIMESTAMP
            );
        """
    )
    
    # transform live flight raw data and load to temporary table task
    flight_transform = PythonOperator(
        task_id = "flight_transform",
        python_callable = flight_transform
    )
    
    # get data from fleet table and temporary table, transform and load to mongodb task
    transform_load = PythonOperator(
        task_id = "transform_load",
        python_callable = transform_load
    )

    # drop temporary table task
    drop_table_postgres = PostgresOperator(
        task_id = "drop_table_postgres",
        postgres_conn_id = "postgres",
        sql = """
            DROP TABLE flight;
        """
    )

    # define dependencies
    fetch_data_api >> [download_raw_data, create_table_postgres] >> flight_transform
    flight_transform >> transform_load >> drop_table_postgres