import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd


def get_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy_utils import database_exists, create_database
    import sys
    user = "postgres"
    passwd = "pass_pgsql"
    host = "172.20.0.6"
    port = "5432"
    db = "ortex"
    try:
        url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
        if not database_exists(url):
            create_database(url)
        engine = create_engine(url, pool_size=50, echo=False)
        return engine
    except ValueError:
        print("Could not convert data to an integer.")
    except Exception as exception:
        print("Unexpected error:", sys.exc_info()[0])
        print(exception.message)
    raise


def start_env():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # CREATE DATABASE
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
                                host="localhost",
                                database="postgres",
                                user="postgres",
                                password="pass_pgsql")

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # create a cursor
        cur = conn.cursor()
        sqlCreateDatabase = "CREATE DATABASE ORTEX ;"
        cur.execute(sqlCreateDatabase)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed. Create Db')
    try:
        # CREATE TABLE
        conn = None
        conn = psycopg2.connect(
                                host="localhost",
                                database="ortex",
                                user="postgres",
                                password="pass_pgsql")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        sqlCreateTable = (
            """
            CREATE TABLE TRANSACTIONS (
                transactionid   numeric primary key,
                gvkey           numeric ,
                companyname     varchar(250),
                companyisin     varchar(250),
                companysedol    varchar(250),
                insiderid       numeric,
                insidername     varchar(250),
                insiderrelation varchar(250),
                insiderlevel    varchar(1),
                connectiontype  varchar(30),
                connectedinsidername      varchar(250),
                connectedinsiderposition  varchar(250),
                transactiontype           varchar(30),
                transactionlabel          varchar(30),
                iid                       varchar(30),
                securityisin              varchar(250),
                securitysedol             varchar(250),
                securitydisplay           varchar(250),
                assetclass                varchar(250),
                shares                    numeric,
                inputdate                  date,
                tradedate                 date,
                maxtradedate              date ,
                price                     float,
                maxprice	              float,
                value                     float,
                currency                  varchar(30),
                valueeur                  float,
                unit                      varchar(30),
                correctedtransactionid    numeric,
                source                    varchar(30),
                tradesignificance         numeric,
                holdings                  numeric,
                filingurl                 varchar(250),
				exchange                  varchar(250)
            )
            """)
        cur.execute(sqlCreateTable)
        # LOAD DATA
        df = pd.read_csv('/home/acorico/Projects/ortex_solution/2017.csv')
        # DATA CLEANING
        df['inputdate'] = pd.to_datetime(df['inputdate'], format="%Y%m%d")
        df['tradedate'] = pd.to_datetime(df['tradedate'], format="%Y%m%d")
        df['maxtradedate'] = pd.to_datetime(df['maxtradedate'], format="%Y%m%d")

        conn_2 = get_engine()
        df.to_sql('transactions', conn_2, if_exists='append', index=False)
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error2:
        print(error2)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed. Create Table')
