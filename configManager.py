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

    def getHandlerInfo(self, issue_handler_task_type_map, handler_table):
        # Connect to the database
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                # Prepare the SQL statement
                
                Query = "SELECT * FROM HANDLER_INFO WHERE handler_id = %s"
                self.get_handler_info_map(Query, (handler_id,), self.db_connection, handler_table)
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_info: %s", self.handler_info)
    
    def getHandlerMap(self, issue_handler_task_type_map, handler_table):
        
        try:
            for params in issue_handler_task_type_map:
                # Prepare the SQL statement
                # Query = "SELECT * FROM HANDLER_MAP WHERE sub_type = '{0}' AND task_type = '{1}' AND flow_id = '{2}' AND handler_id = '{3}'".format(sub_type, task_type, flow_id, handler_id)
                task_type, handler_id, sub_type, srv_id, flow_id = params
                sparam = task_type, handler_id, sub_type, srv_id
                wsparam = task_type, handler_id, sub_type, flow_id
                
                Query_srv = "SELECT * FROM HANDLER_MAP WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND srv_id in %s"
                
                Query = "SELECT * FROM HANDLER_MAP WHERE task_type = %s AND handler_id = %s AND sub_type = %s AND (flow_id in %s OR flow_id = '-1')"

                if self.get_handler_info_map(Query_srv, sparam, self.db_connection, handler_table):
                    pass
                else:
                    self.get_handler_info_map(Query, wsparam, self.db_connection, handler_table)
                    
        except Exception as ex:
            logging.info(ex)
        
        logging.info("handler_map: %s", self.handler_map)
    
    def get_handler_info_map(self, query, params, conn, handler_table):
        query_type = "SELECT"
        logging.info('SELECT_QUERY: %s and params: %s', query, params)
        
        # Create a QueryExecutor instance with the connection object
        query_executor = QueryExecutor(conn)

        # Execute the query
        query_executor.execute(query_type, query, params)
            
        if query_executor.result_set:
            result_set = query_executor.result_set
            
            # Convert result_set(ordered dictionary) to JSON object
            h_info = json.dumps(result_set)
            
            if handler_table == "handler_info":
                self.handler_info.append(json.loads(h_info, object_pairs_hook=OrderedDict))
            elif handler_table == "handler_map":
                self.handler_map.append(json.loads(h_info, object_pairs_hook=OrderedDict))
            
            return True
        else:
            return False