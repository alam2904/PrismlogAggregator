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

    def getHandlerInfo(self, issue_handler_task_type_map, handler_info):
        # Connect to the database
        try:
            for item in issue_handler_task_type_map:
                task_type, handler_id, sub_type, flow_id = item
                # Prepare the SQL statement
                
                Query = "SELECT * FROM HANDLER_INFO WHERE handler_id = '{}'".format(handler_id)
                self.get_handler_info_map(Query, self.db_connection)
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_info: %s", self.handler_info)
    
    def getHandlerMap(self, issue_handler_task_type_map, handler_map):
        
        try:
            for item in issue_handler_task_type_map:
                task_type, handler_id, sub_type, flow_id = item
                # Prepare the SQL statement
                Query = "SELECT * FROM HANDLER_MAP WHERE sub_type = '{0}' AND task_type = '{1}' AND flow_id = '{2}' AND handler_id = '{3}'".format(sub_type, task_type, flow_id, handler_id)
                self.get_handler_info_map(Query, self.db_connection)
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_map: %s", self.handler_map)
    
    def get_handler_info_map(self, query, conn):
        query_type = "SELECT"
        logging.info('SELECT_QUERY: %s', query)
        
        # Create a QueryExecutor instance with the connection object
        query_executor = QueryExecutor(conn)

        # Execute the query
        query_executor.execute(query_type, query)
            
        if query_executor.result_set:
            result_set = query_executor.result_set
            
            # Convert result_set(ordered dictionary) to JSON object
            h_info = json.dumps(result_set)
            
            self.handler_info.append(json.loads(h_info, object_pairs_hook=OrderedDict))