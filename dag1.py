# import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pytz


# define extract data from mysql and load to postgres function
def mysql_to_postgres():
    # hook mysql connection
    mysql = MySqlHook(mysql_conn_id = "mysql")
    # get fleet data from mysql as pandas dataframe
    fleet_data = mysql.get_pandas_df(sql = "SELECT * FROM fleet_db.fleet;")

    # hook postgres connection
    postgres = PostgresHook(postgres_conn_id = "postgres")
    # get connection
    conn = postgres.get_conn()
    # get cursor
    cur = conn.cursor()

    # sql of insert data into fleet table in postgres
    fleet_sql = """
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
        ON CONFLICT (reg)
        DO UPDATE SET
            icao24 = EXCLUDED.icao24,
            aircraft_type = EXCLUDED.aircraft_type,
            aircraft_type_resign = EXCLUDED.aircraft_type_resign,
            aircraft_name = EXCLUDED.aircraft_name,
            delivered = EXCLUDED.delivered,
            age_years = EXCLUDED.age_years,
            remark = EXCLUDED.remark;
    """

    # for loop to insert data into postgres
    for row in fleet_data.values:
        cur.execute(fleet_sql, tuple(row))
    conn.commit()
    
    # close cursor and connection
    cur.close()
    conn.close()


# define default arguments
default_args = {
    "owner": "WarmNatchapol",
    "start_date": datetime(2023, 6, 11, tzinfo=pytz.timezone("Asia/Bangkok"))
}

# DAG
with DAG(
    "Aviation_DAG_1",
    default_args = default_args,
    schedule_interval = None
) as dag:

    # create fleet table in postgres task
    create_table_postgres = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "postgres",
        sql = """
            CREATE TABLE IF NOT EXISTS fleet (
                reg VARCHAR(6) PRIMARY KEY,
                icao24 CHAR(6),
                aircraft_type VARCHAR(50),
                aircraft_type_resign CHAR(4),
                aircraft_name VARCHAR(50),
                delivered DATE,
                age_years FLOAT(3),
                remark VARCHAR(20)
            );
        """
    )

    # extract data from mysql and load to postgres task
    mysql_to_postgres = PythonOperator(
        task_id = "mysql_to_postgres",
        python_callable = mysql_to_postgres
    )

    # define dependencies
    create_table_postgres >> mysql_to_postgres