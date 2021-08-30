#! /usr/bin/python

from splitfile import split
from config import Configure
import snowflake.connector
import datetime
import threading
from pathlib import Path
from os import listdir
from os.path import join, isdir
import os.path
import shutil
from snowflake.connector.errors import DatabaseError, ProgrammingError
from log import logger


def connect(config_file):
    connection = ""
    try:
        cone = Configure(config_file)
        config_data = cone.config()
        connection = snowflake.connector.connect(
            user=config_data["user"],
            password=config_data["password"],
            account=config_data["account"], )
        logger.info("Connection established successfully")
    except DatabaseError as db_ex:
        if db_ex.errno == 250001:
            print(f"Invalid username/password, please re-enter username and password...")
            logger.warning(f"Invalid username/password, please re-enter username and password...")
        else:
            raise
    except Exception as ex:
        print(f"New exception raised {ex}")
        logger.error(f"New exception raised {ex}")
        raise
    return connection


def upload(config_file, source_file, database):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = connect(config_file)
    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])
    cs = connection.cursor()
    try:
        sql = "PUT file:///" + source_file + " @" + database + ";"
        cs.execute(sql)
        logger.info(sql + " executed")
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        logger.error(f"Programming error: {db_ex}")
        raise
    finally:
        cs.close()
    connection.close()


def main():
    print("Split_start", datetime.datetime.now())
    con = Configure("cred.json")
    config_datas = con.config()
    resultfiles = split(config_datas["source"])
    thread_list = []
    print("startupload:  ", datetime.datetime.now())
    logger.info(" Uploading files into Database stages")
    for file in resultfiles:
        for direc in listdir(config_datas["source"]):
            if isdir(join(config_datas["source"], direc)) and str(direc) in file:
                for dire in listdir(join(config_datas["source"])):
                    if dire in file:
                        thread = threading.Thread(target=upload, args=("cred.json", file, direc))
                        thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def copy(config_file, database, table):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = connect(config_file)
    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])
    cs = connection.cursor()
    try:
        sql = """COPY into table
        FROM @stage
        file_format = (type = csv field_optionally_enclosed_by='"' skip_header=1)
        pattern = '.*table_[1-6].csv.gz'
        on_error = 'ABORT_STATEMENT';"""
        res = sql.replace("table", table, 2)
        res_ = res.replace("stage", database, 1)
        cs.execute(res_)
        logger.info(res_ + " executed")
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        logger.error(f"Programming error: {db_ex}")
        raise
    finally:
        cs.close()
    connection.close()


def copy_main():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    thread_list = []
    print("startcopy:  ", datetime.datetime.now())
    logger.info("Copying files into stage database")
    for database in listdir(source):
        if isdir(join(source, database)):
            for table in listdir(join(source, database)):
                if not isdir(join(join(source, database), table)):
                    thread = threading.Thread(target=copy, args=("cred.json", database, Path(table).stem))
                    thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def remove_old_staged_files(config_file, database):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = connect(config_file)
    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])
    cs = connection.cursor()
    try:
        sql = "REMOVE @" + database + " pattern='.*.csv.gz';"
        cs.execute(sql)
        logger.info(sql + " executed")
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        logger.error(f"Programming error: {db_ex}")
        raise
    finally:
        cs.close()
    connection.close()


def delete_old_staged_files():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    thread_list = []
    print("remove old stage files:  ", datetime.datetime.now())
    logger.info("Deleting old staged files from database stages")
    for database in listdir(source):
        if isdir(join(source, database)):
            thread = threading.Thread(target=remove_old_staged_files, args=("cred.json", database))
            thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def load_history(config_file, database):
    cone = Configure(config_file)
    config_data = cone.config()
    connection = connect(config_file)
    connection.cursor().execute("USE WAREHOUSE " + config_data["warehouse"])
    connection.cursor().execute("USE DATABASE " + database)
    connection.cursor().execute("USE SCHEMA " + config_data["schema"])
    connection.cursor().execute("USE ROLE " + config_data["role"])
    cs = connection.cursor()
    try:
        sql = "select * from information_schema.load_history order by last_load_time desc;"
        cs.execute(sql)
        result = cs.fetch_pandas_all()
        res = result.loc[result['STATUS'] != "LOADED"]
        if len(res.index) > 0:
            print("Error:   ", database)
        else:
            print("Loaded successfully:   ", database)
            logger.info("Copy/Load history for database has No error")
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        logger.error(f"Programming error: {db_ex}")
        raise
    finally:
        cs.close()
    connection.close()


def history():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    thread_list = []
    print("Checking loading history:  ", datetime.datetime.now())
    logger.info("Getting Copy/Load history for each database")
    for database in listdir(source):
        if isdir(join(source, database)):
            thread = threading.Thread(target=load_history, args=("cred.json", database))
            thread_list.append(thread)
    for thr in thread_list:
        thr.start()
    for thre in thread_list:
        thre.join()


def archive():
    con = Configure("cred.json")
    config_datas = con.config()
    source = config_datas["source"]
    target = config_datas["archive"]
    logger.info("Moving processed files from source to archive")
    for file in listdir(source):
        shutil.move(os.path.join(source, file), target)
