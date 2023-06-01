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
        self.handler_info = []

    def getHandlerConfig(self, handler_ids):
        # Create a DatabaseConnection instance
        db_connection = DatabaseConnection(
            host= "172.19.113.108",
            user="root",
            passwd="Onm0bile",
            db="prism"
        )

        # Connect to the database
        db_connection.create_connection()
        
        try:
            for id in handler_ids:
                # Prepare the SQL statement
                Query = "SELECT * FROM HANDLER_INFO WHERE handler_id ='{}'".format(id)
                query_type = "SELECT"
                logging.info('SELECT_QUERY: %s', Query)
                
                # Create a QueryExecutor instance with the connection object
                query_executor = QueryExecutor(db_connection)

                # Execute the query
                query_executor.execute(query_type, Query)
                
                if query_executor.formatted_row:
                    result_set = query_executor.formatted_row
                    
                    # Convert result_set(ordered dictionary) to JSON object
                    h_info = json.dumps(result_set)
                    
                    self.handler_info.append(json.loads(h_info, object_pairs_hook=OrderedDict))
                    logging.info("handler_info ordered dict: %s", self.handler_info)
        except Exception as ex:
            logging.info(ex)