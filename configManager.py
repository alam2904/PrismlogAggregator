from collections import OrderedDict
import json
import logging
from database_connection import DatabaseConnection
from query_executor import QueryExecutor

class ConfigManager:
    """
        This is the class responsible for db config fetch
    """
    
    def __init__(self):
        # Create a DatabaseConnection instance
        self.db_connection = DatabaseConnection(
            host= "172.19.113.108",
            user="root",
            passwd="Onm0bile",
            db="prism"
        )
        # Connect to the database
        self.db_connection.create_connection()
        self.handler_info = []
        self.handler_map = []
        self.handler_processor_info = []
        self.handler_processor_map = []
        self.subtype_parameter = []
        
        self.initialize_handler_processors()
        self.initialize_subtype_parameter()    
    
    def initialize_subtype_parameter(self):
        #initializing prism_config_params subtype boolean parameter
        Query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE PARAM_NAME LIKE '%SUBTYPE%'"
        configMap = self.get_db_config_map(Query, None, self.db_connection)
        
        if configMap:
            self.subtype_parameter.append(json.loads(configMap, object_pairs_hook=OrderedDict))
    
    def initialize_handler_processors(self):
        #initializing handler_processor_info and handler_processor_map
        logging.info("Initializing handler_processor_info_map")
        Query_hpi = "SELECT * FROM HANDLER_PROCESSOR_INFO"
        configMap = self.get_db_config_map(Query_hpi, None, self.db_connection)
        
        if configMap:
            self.handler_processor_info.append(json.loads(configMap, object_pairs_hook=OrderedDict))
            
        Query_hpm = "SELECT * FROM HANDLER_PROCESSOR_MAP"
        configMap = self.get_db_config_map(Query_hpm, None, self.db_connection)
        
        if configMap:
            self.handler_processor_map.append(json.loads(configMap, object_pairs_hook=OrderedDict))

    def getHandlerInfo(self, issue_handler_task_type_map):
        # Connect to the database
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                # Prepare the SQL statement
                
                Query = "SELECT * FROM HANDLER_INFO WHERE handler_id = %s"
                configMap = self.get_db_config_map(Query, (handler_id,), self.db_connection)
                
                if configMap:
                    self.handler_info.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                else:
                    logging.debug("No handler configured for handler_id= %s", handler_id)
                    
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_info: %s", self.handler_info)
    
    def getHandlerMap(self, issue_handler_task_type_map):
        
        try:
            for params in issue_handler_task_type_map:
                # Prepare the SQL statement
                # Query = "SELECT * FROM HANDLER_MAP WHERE sub_type = '{0}' AND task_type = '{1}' AND flow_id = '{2}' AND handler_id = '{3}'".format(sub_type, task_type, flow_id, handler_id)
                task_type, handler_id, sub_type, srv_id, flow_id = params
                sparam = task_type, handler_id, sub_type, srv_id
                wsparam = task_type, handler_id, sub_type, flow_id
                
                Query_srv = "SELECT * FROM HANDLER_MAP WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND srv_id in %s"
                
                Query = "SELECT * FROM HANDLER_MAP WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND (flow_id in %s OR flow_id = '-1')"

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
    
    def select(self):
        return "SELECT"