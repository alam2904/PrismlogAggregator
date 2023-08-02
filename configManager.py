from collections import OrderedDict
import json
import logging
import oarm_modules
import socket
import os
# from query_executor import QueryExecutor
# from database_connection import DatabaseConnection

class ConfigManager:
    """
        This is the class responsible for db config fetch
    """
    
    def __init__(self, config, validation_object=None):
        # Create a DatabaseConnection instance
        # self.db_connection = DatabaseConnection(
        #     host= "172.19.113.108",
        #     user="root",
        #     passwd="Onm0bile",
        #     db="safaricom"
        # )
        # Connect to the database
        # self.db_connection.create_connection()
        self.config = config
        self.validation_object = validation_object
        self.handler_info = []
        self.handler_map = []
        self.handler_processor_info = []
        self.handler_processor_map = []
        self.subtype_parameter = []
        # self.initialize_subtype_parameter()   
    
    def initialize_subtype_parameter(self):
        #initializing prism_config_params subtype boolean parameter
        try:
            if self.validation_object.is_multitenant_system:
                query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE SITE_ID = %s AND PARAM_NAME LIKE '%%SUBTYPE%%'" % (self.validation_object.site_id)
                # params = (self.validation_object.site_id,)
            else:
                query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE SITE_ID = %s AND PARAM_NAME LIKE '%%SUBTYPE%%'" % ("-1")
                # params = (-1,)
            # logging.info("PARAMS: %s", params)
            # configMap = self.get_db_config_map(Query, params, self.db_connection)
            db_name, db_host = self.get_db_parameters()
            
            if db_name and db_host:
                configMap = oarm_modules.oarm_database_select(db_name, db_host, query)
            
            if configMap:
                self.subtype_parameter.append(json.loads(configMap, object_pairs_hook=OrderedDict))
        except Exception as ex:
            logging.error(ex)
    
    def get_prism_config_param_value(self, module_name, site_id, param_name):
        #get prism_config_param value
        param_value = None
        try:
            Query = "SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME = %s"
            params = (module_name, site_id, param_name)
                
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                pcp_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if pcp_value_row:
                    for row in pcp_value_row:
                        param_value = row["PARAM_VALUE"]
            return param_value
        except Exception as ex:
            logging.error(ex)
            
    def get_services_ref_param_value(self, service_id, site_id, param_name):
        #get prism_config_param value
        param_value = None
        try:
            Query = "SELECT PARAM_VALUE FROM SERVICES_REF WHERE SERVICE_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s"
            params = (service_id, site_id, param_name)
                
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                sr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if sr_value_row:
                    for row in sr_value_row:
                        param_value = row["PARAM_VALUE"]
            return param_value
        except Exception as ex:
            logging.error(ex)
    
    def get_service_charges_ref_param_value(self, sc_id, site_id, param_name):
        #get prism_config_param value
        param_value = None
        try:
            Query = "SELECT PARAM_VALUE FROM SERVICE_CHARGES_REF WHERE SC_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s"
            params = (sc_id, site_id, param_name)

            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                scr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if scr_value_row:
                    for row in scr_value_row:
                        param_value = row["PARAM_VALUE"]
            return param_value
        except Exception as ex:
            logging.error(ex)
    
    def is_multitenant_system(self):
        is_global_instance = False
        try:
            Query = "SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME = %s"
            params = ('SYSTEM', -1, 'IS_SINGLE_INSTANCE')
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                param_value = json.loads(configMap, object_pairs_hook=OrderedDict)
                if param_value:
                    for row in param_value:
                        if self.is_boolean(row["PARAM_VALUE"]):
                            is_global_instance = str(row["PARAM_VALUE"]).lower() == 'true'
        except KeyError as error:
            logging.info(error)
            
        return is_global_instance
                    
    def get_flow_handler_mapping(self):
        #generic flow handler mapping
        try:
            if self.validation_object.is_multitenant_system:
                Query = "SELECT hm.TRANSACTION_TYPE, hi.PARAMS FROM HANDLER_PROCESSOR_INFO hi INNER JOIN HANDLER_PROCESSOR_MAP hm ON hi.HANDLER_ID = hm.HANDLER_ID WHERE hi.HANDLER_NAME = 'com.onmobile.prism.generic.flowHandler.GenericFlowHandler' AND hm.SITE_ID = %s GROUP BY hi.PARAMS, hm.TRANSACTION_TYPE"
                params = (self.validation_object.site_id,)
            else:
                Query = "SELECT hm.TRANSACTION_TYPE, hi.PARAMS FROM HANDLER_PROCESSOR_INFO hi INNER JOIN HANDLER_PROCESSOR_MAP hm ON hi.HANDLER_ID = hm.HANDLER_ID WHERE hi.HANDLER_NAME = 'com.onmobile.prism.generic.flowHandler.GenericFlowHandler' AND hm.SITE_ID = %s GROUP BY hi.PARAMS, hm.TRANSACTION_TYPE"
                params = (-1,)
                
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                return json.loads(configMap, object_pairs_hook=OrderedDict)
            return None
        except Exception as ex:
            logging.error(ex)
    
    def get_operator_site_map(self, operator_id):
        #initializing prism_config_params subtype boolean parameter
        operator_site_map = None
        try:
            Query = "SELECT SITE_ID, TIME_ZONE, FILE_IDS FROM OPERATOR_SITE_MAP WHERE OPERATOR_ID = %s"
            params = (operator_id,)
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                operator_site_map = json.loads(configMap, object_pairs_hook=OrderedDict)
                if operator_site_map:
                    for row in operator_site_map:
                        logging.info("SITE_ID: %s AND TIME_ZONE: %s AND FILE_IDS: %s", row["SITE_ID"], row["TIME_ZONE"], row["FILE_IDS"])
                        operator_site_map = row["SITE_ID"], row["TIME_ZONE"], row["FILE_IDS"] 
            return operator_site_map
        except KeyError as ex:
            logging.exception("operator_id: %s site map is not found", operator_id, ex)
    
    def get_operator_url_map(self, operator_id):
        #initializing prism_config_params subtype boolean parameter
        urls = []
        try:
            Query = "SELECT OPERATOR_URL FROM OPERATOR_URL_MAPPING WHERE OPERATOR_ID = %s"
            params = (operator_id,)
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                urls.extend([row["OPERATOR_URL"] for row in json.loads(configMap, object_pairs_hook=OrderedDict)])
                logging.info("URL_LIST: %s", urls)
            return urls
        except KeyError as ex:
            logging.exception("operator_id: %s url map is not found", operator_id, ex)
    
    def get_operator_url_from_pcp(self, module_name, site_id):
        #get prism_config_param value
        try:
            urls = []

            Query = "SELECT PARAM_NAME FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME LIKE '%%:action'"
            params = (module_name, site_id)
                
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                param_rows = json.loads(configMap, object_pairs_hook=OrderedDict)
                if param_rows:
                    for row in param_rows:
                        urls.append(str(row["PARAM_NAME"]).split(":")[0])
            return urls
        except Exception as ex:
            logging.error(ex)
        
    def get_handler_info(self, issue_handler_task_type_map):
        handler_id_list = []
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                
                if handler_id and handler_id not in handler_id_list:
                    handler_id_list.append(handler_id)
            
            for handler_id in handler_id_list:
                try:
                    # Prepare the SQL statement
                    Query = "SELECT * FROM HANDLER_INFO WHERE HANDLER_ID = %s"
                    params = (handler_id,)
                        
                    configMap = self.get_db_config_map(Query, params, self.db_connection)
                    
                    if configMap:
                        self.handler_info.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                    else:
                        logging.debug("No handler configured for handler_id = %s", handler_id)
                    
                except Exception as ex:
                    logging.info(ex)
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_info: %s", self.handler_info)
    
    def get_handler_map(self, issue_handler_task_type_map):
        
        for params in issue_handler_task_type_map:
            try:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                if handler_id:
                    handler_id = '%' + handler_id + '%'
                    # logging.info("CONF_HANDLER_ID: %s", handler_id)
                
                    if self.validation_object.is_multitenant_system:
                        sparam = (self.validation_object.site_id, task_type, handler_id, sub_type, srv_id)
                        wsparam = (self.validation_object.site_id, task_type, handler_id, sub_type, flow_id)
                    else:
                        sparam = (-1, task_type, handler_id, sub_type, srv_id)
                        wsparam = (-1, task_type, handler_id, sub_type, flow_id)
                    
                    # Prepare the SQL statement
                    Query_srv = "SELECT * FROM HANDLER_MAP WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID LIKE %s AND SUB_TYPE = %s AND SRV_ID in %s"
                    
                    Query = "SELECT * FROM HANDLER_MAP WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID LIKE %s AND SUB_TYPE = %s AND (FLOW_ID in %s OR FLOW_ID = '-1')"

                    configMap = self.get_db_config_map(Query_srv, sparam, self.db_connection)
                    
                    if configMap:
                        pass
                    else:
                        configMap = self.get_db_config_map(Query, wsparam, self.db_connection)
                    
                    self.handler_map.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                    
            except Exception as ex:
                logging.info(ex)
        
        logging.info("HANDLER_MAP: %s", self.handler_map)
    
    def get_file_info(self):
        file_info = []
        file_ids = []
        
        if not self.validation_object.is_multitenant_system:
            global_file_ids = self.get_prism_config_param_value('SYSTEM', -1, 'GLOBAL_FILE_IDS')
            realtime_global_file_ids = self.get_prism_config_param_value('SYSTEM', -1, 'REALTIME_GLOBAL_FILE_IDS')
            
            if global_file_ids:
                file_ids.extend(str(global_file_ids).split(","))
            if realtime_global_file_ids:
                file_ids.extend(str(realtime_global_file_ids).split(","))
        else:
            logging.info("file ids must be configured in operator_site_map")
            file_ids.extend(str(self.validation_object.file_ids).split(","))
        
        if file_ids:
            try:
                # Prepare the SQL statement
                Query = "SELECT FILE_ID, FILE_PREFIX, FILE_DATETIME_FMT, FILE_SUFFIX, FILE_LOCAL_DIR FROM FILE_INFO WHERE FILE_ID in %s AND FILE_TYPE = %s"
                
                params = (tuple(file_ids), 'PUSH')
                    
                configMap = self.get_db_config_map(Query, params, self.db_connection)
                
                if configMap:
                    file_info.extend(json.loads(configMap, object_pairs_hook=OrderedDict))
                else:
                    logging.debug("No file_info configured for file_ids = %s", file_ids)
                        
            except Exception as ex:
                logging.info(ex)
        if file_info:
            return file_info
        # logging.info("FILE_INFO: %s", file_info)
    
    def get_db_config_map(self, query, params, conn):
        configMap = None
        logging.info('SELECT_QUERY: %s AND PARAMS: %s', query, params)
        
        try:
            # Create a QueryExecutor instance with the connection object
            query_executor = QueryExecutor(conn)

            # Execute the query
            query_executor.execute(self.select(), query, params)

            if query_executor.result_set:
                result_set = query_executor.result_set
                
                # Convert result_set(ordered dictionary) to JSON object
                configMap = json.dumps(result_set)
        except Exception as e:
            logging.error('Error occurred: %s', e)
    
        # finally:
        #     logging.info('reached subs finally block')
        #     self.db_connection.close()
        
        return configMap

    def get_db_parameters(self):
        db_name = None
        db_host = None
        hostname = socket.gethostname()
        
        try:
            db_name = self.config[hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["DB_NAME"]
        except KeyError as err:
            logging.info(err)
            try:
                web_services = [webService for webService in self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"]]
                for web_service in web_services:
                    db_name = self.config[hostname]["PRISM"]["PRISM_TOMCAT"][web_service]["DB_NAME"]
                    if db_name:
                        break
            except KeyError as err:
                logging.info(err)
        
        try:  
            self.config[hostname]["PRISM"]["PRISM_DEAMON"]["PRISM_DEAMON"]["DB_IP"]
        except KeyError as err:
            logging.info(err)
            try:
                web_services = [webService for webService in self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"]]
                for web_service in web_services:
                    db_host = self.config[hostname]["PRISM"]["PRISM_TOMCAT"][web_service]["DB_IP"]
                    if db_host:
                        break
            except KeyError as err:
                logging.info(err)
        
        return db_name, db_host
                    
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
    
    def select(self):
        return "SELECT"