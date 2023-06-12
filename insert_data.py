# import libraries
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import mysql.connector


# get mysql connection from environment variables.
class Config:
    load_dotenv()
    mysql_host = os.environ.get("MYSQL_HOST")
    mysql_database = os.environ.get("MYSQL_DATABASE")
    mysql_username = os.environ.get("MYSQL_USERNAME")
    mysql_password = os.environ.get("MYSQL_PASSWORD")
    mysql_port = os.environ.get("MYSQL_PORT")


# define read csv file function
def read_csv(filename):
    fleet_df = pd.read_csv(filename)
    return fleet_df


# define insert data to mysql database function
def insert_data(fleet_df):
    # get connection
    conn = mysql.connector.connect(
        host = Config.mysql_host,
        database = Config.mysql_database,
        user = Config.mysql_username,
        password = Config.mysql_password,
        port = Config.mysql_port
    )
    # get cursor
    cur = conn.cursor()

    # sql of insert data to fleet table
    insert_sql = """
        INSERT INTO fleet (
            reg, 
            icao24, 
            aircraft_type, 
            aircraft_type_resign, 
            aircraft_name, 
            delivered, 
            age_years, 
            remark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            icao24 = icao24, 
            aircraft_type = aircraft_type, 
            aircraft_type_resign = aircraft_type_resign, 
            aircraft_name = aircraft_name, 
            delivered = delivered, 
            age_years = age_years, 
            remark = remark;
    """

    # for loop to insert fleet data to mysql database from dataframe
    for row in fleet_df.values:
        cur.execute(insert_sql, tuple(row))
    conn.commit()

    # close cursor and connection
    cur.close()
    conn.close()


# define main function
def main():
    # call read csv file function
    fleet_df = read_csv("fleet.csv")
    # call insert data to database function
    insert_data(fleet_df)


# call main function
if __name__ == "__main__":
    main()