import os
from config import Configure
from os import listdir
from os.path import isfile, join, isdir
import shutil
import snowflake_main
import datetime


def check_for_client_files(config):
    cone = Configure(config)
    config_data = cone.config()
    source = config_data["temp"]
    temp = config_data["source"]
    clients = cone.config()
    databasetoprocess = []
    if os.path.exists(source):
        for database in listdir(source):
            if database in clients.keys() and isdir(join(source, database)) and \
                    clients[database] == listdir(join(source, database)):
                for item in clients[database]:
                    file = join(source, database, item)
                    if isfile(file):
                        databasetoprocess.append(database)
                        shutil.move(join(source, database), temp)
    return databasetoprocess


if __name__ == "__main__":
    try :
        datafiles_received = check_for_client_files("cred.json")
        if len(datafiles_received) >= 1:
            print("Client/Clients received")
            snowflake_main.delete_old_staged_files()
            snowflake_main.main()
            snowflake_main.copy_main()
            snowflake_main.history()
            snowflake_main.archive()
            snowflake_main.logger.info("End of Process")
            print("end:  ", datetime.datetime.now())
        else:
            snowflake_main.logger.info("Monitoring client Folders")
            print("Waiting for client")
    except KeyboardInterrupt:
        print('KeyboardInterrupted')
