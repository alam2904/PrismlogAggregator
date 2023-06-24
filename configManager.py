from collections import OrderedDict
import json
import logging
from database_connection import DatabaseConnection
from query_executor import QueryExecutor

class ConfigManager:
    """
        This is the class responsible for db config fetch
    """
    
    def __init__(self, validation_object=None):
        # Create a DatabaseConnection instance
        self.db_connection = DatabaseConnection(
            host= "172.19.113.108",
            user="root",
            passwd="Onm0bile",
            db="safaricom"
        )
        # Connect to the database
        self.db_connection.create_connection()
        self.validation_object = validation_object
        self.handler_info = []
        self.handler_map = []
        self.handler_processor_info = []
        self.handler_processor_map = []
        self.subtype_parameter = []
        # self.initialize_subtype_parameter()   
    
    def initialize_subtype_parameter(self):
        #initializing prism_config_params subtype boolean parameter
        Query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE PARAM_NAME LIKE '%SUBTYPE%'"
        configMap = self.get_db_config_map(Query, None, self.db_connection)
        
        if configMap:
            self.subtype_parameter.append(json.loads(configMap, object_pairs_hook=OrderedDict))
    
    def is_multitenant_system(self):
        is_global_instance = False
        try:
            Query = "SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = 'SYSTEM' AND SITE_ID = -1 AND PARAM_NAME = 'IS_SINGLE_INSTANCE'"
            configMap = self.get_db_config_map(Query, None, self.db_connection)
            
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
        logging.info("getting flow handler mapping")
        Query = "SELECT hm.transaction_type, hi.params FROM handler_processor_info hi INNER JOIN handler_processor_map hm ON hi.handler_id = hm.handler_id WHERE hi.handler_name = 'com.onmobile.prism.generic.flowHandler.GenericFlowHandler' GROUP BY hi.params, hm.transaction_type"
        
        configMap = self.get_db_config_map(Query, None, self.db_connection)
        
        if configMap:
            return json.loads(configMap, object_pairs_hook=OrderedDict)
        return None
    
    def get_operator_site_map(self, operator_id):
        #initializing prism_config_params subtype boolean parameter
        operator_site_map = None
        try:
            Query = "SELECT site_id, time_zone FROM operator_site_map WHERE operator_id = %s"
            configMap = self.get_db_config_map(Query, (operator_id,), self.db_connection)
            if configMap:
                operator_site_map = json.loads(configMap, object_pairs_hook=OrderedDict)
                # logging.info("OPERATOR_SITE_MAP: %s", operator_site_map)
                if operator_site_map:
                    for row in operator_site_map:
                        logging.info("SITE_ID: %s AND TIME_ZONE: %s", row["site_id"], row["time_zone"])
                        operator_site_map = row["site_id"], row["time_zone"]    
            return operator_site_map
        except KeyError as ex:
            logging.exception("operator_id: %s site map is not found", operator_id, ex)

    def get_handler_info(self, issue_handler_task_type_map):
        # Connect to the database
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                # Prepare the SQL statement
                if handler_id:
                    Query = "SELECT * FROM handler_info WHERE handler_id = %s"
                    configMap = self.get_db_config_map(Query, (handler_id,), self.db_connection)
                    
                    if configMap:
                        self.handler_info.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                    else:
                        logging.debug("No handler configured for handler_id = %s", handler_id)
                    
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_info: %s", self.handler_info)
    
    def get_handler_map(self, issue_handler_task_type_map):
        
        try:
            for params in issue_handler_task_type_map:
                # Prepare the SQL statement
                # Query = "SELECT * FROM HANDLER_MAP WHERE sub_type = '{0}' AND task_type = '{1}' AND flow_id = '{2}' AND handler_id = '{3}'".format(sub_type, task_type, flow_id, handler_id)
                task_type, handler_id, sub_type, srv_id, flow_id = params
                sparam = task_type, handler_id, sub_type, srv_id
                wsparam = task_type, handler_id, sub_type, flow_id
                
                if handler_id:
                    Query_srv = "SELECT * FROM handler_map WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND srv_id in %s"
                    
                    Query = "SELECT * FROM handler_map WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND (flow_id in %s OR flow_id = '-1')"

                    configMap = self.get_db_config_map(Query_srv, sparam, self.db_connection)
                    
                    if configMap:
                        pass
                    else:
                        configMap = self.get_db_config_map(Query, wsparam, self.db_connection)
                    
                    self.handler_map.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                    
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_map: %s", self.handler_map)
    
    def get_db_config_map(self, query, params, conn):
        configMap = None
        logging.info('SELECT_QUERY: %s and params: %s', query, params)
        
        # Create a QueryExecutor instance with the connection object
        query_executor = QueryExecutor(conn)

        # Execute the query
        query_executor.execute(self.select(), query, params)
            
        if query_executor.result_set:
            result_set = query_executor.result_set
            
            # Convert result_set(ordered dictionary) to JSON object
            configMap = json.dumps(result_set)
        
        return configMap
    
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
    
    def select(self):
        return "SELECT"