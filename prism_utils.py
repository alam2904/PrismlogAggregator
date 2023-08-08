import socket
import logging
import oarm_modules

def get_db_parameters(config):
    db_name = None
    db_host = None
    hostname = socket.gethostname()

    try:
        db_name = config[hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["DB_NAME"]
    except KeyError as err:
        logging.info(err)
        try:
            web_services = [webService for webService in config[hostname]["PRISM"]["PRISM_TOMCAT"]]
            for web_service in web_services:
                db_name = config[hostname]["PRISM"]["PRISM_TOMCAT"][web_service]["DB_NAME"]
                if db_name:
                    break
        except KeyError as err:
            logging.info(err)

    try:  
        db_host = config[hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["DB_IP"]
    except KeyError as err:
        logging.info(err)
        try:
            web_services = [webService for webService in config[hostname]["PRISM"]["PRISM_TOMCAT"]]
            for web_service in web_services:
                db_host = config[hostname]["PRISM"]["PRISM_TOMCAT"][web_service]["DB_IP"]
                if db_host:
                    break
        except KeyError as err:
            logging.info(err)

    logging.info("DB_NAME: %s and DB_HOST: %s", db_name, db_host)
    return db_name, db_host

def query_executor(db_name, db_host, query, query_type):
    data_map = None
    # logging.info("QUERY: %s AND QUERY_TYPE: %s", query, query_type)
    
    if (db_name and str(db_name).strip()) and (db_host and str(db_host).strip()):
        
        if query_type == "SELECT":
            try:
                data_map = oarm_modules.oarm_database_select(db_name, db_host, query)
            except Exception as ex:
                logging.error(ex)            
        
        elif query_type == "UPDATE":
            try:
                data_map = oarm_modules.oarm_database_update(db_name, db_host, query)
            except Exception as ex:
                logging.error(ex)
    
    else:
        logging.debug("Eigther DB_NAME or DB_HOST is None or Empty in %s.json file", socket.gethostname())
    
    logging.info("DATA_MAP: %s", data_map)
    return data_map
