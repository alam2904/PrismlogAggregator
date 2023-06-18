from collections import OrderedDict
import logging
import json
from database_connection import DatabaseConnection
from query_executor import QueryExecutor
from datetime import datetime
from update_query_criteria import UpdateQueryCriteria


class SubscriptionController:
    """
        This is the class responsible for SELECT and UPDATE db query processing
        based on various subscription condition 
    """
    def __init__(self, pname, sbn_thread_dict, process_subs_data):
        self.sbn_thread_dict = sbn_thread_dict
        self.process_subs_data = process_subs_data
        self.subscription_data = []
        self.pname = pname
        
    def get_subscription(self, is_reprocessing_required, reprocess_sbnId=None):
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
            if self.process_subs_data:
                for sbnId, thread in self.sbn_thread_dict.items():
                    # Prepare the SQL statement
                    Query = "SELECT * FROM SUBSCRIPTIONS WHERE sbn_id = %s"
                    query_type = "SELECT"
                    logging.info('SELECT_QUERY: %s and params: %s', Query, sbnId)
                    
                    # Create a QueryExecutor instance with the connection object
                    query_executor = QueryExecutor(db_connection)

                    # Execute the query
                    query_executor.execute(query_type, Query, sbnId)
                    
                    if query_executor.result_set:
                        result_set = query_executor.result_set
                        
                        # Convert result_set(ordered dictionary) to JSON object.
                        subscription_json_object = json.dumps(result_set)
                        logging.info("subscription json object: %s", subscription_json_object)
                        
                        self.subscription_data.append(json.loads(subscription_json_object, object_pairs_hook=OrderedDict))
                        
                        if is_reprocessing_required and self.subscription_data:
                            subscriptionRecord = self.get_subscription_dict()
                            if subscriptionRecord:
                                if self.pname == "PRISM_TOMCAT":
                                    current_system_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    if subscriptionRecord["charge_schedule"] > current_system_datetime:
                                        self.execute_update(query_executor, sbnId, subscriptionRecord)
                                else:
                                    self.execute_update(query_executor, sbnId, subscriptionRecord)
                           
            else:
                Query = "SELECT * FROM SUBSCRIPTIONS WHERE sbn_id = %s"
                query_type = "SELECT"
                # logging.info('SELECT_QUERY: %s', Query)
                
                # Create a QueryExecutor instance with the connection object
                query_executor = QueryExecutor(db_connection)

                # Execute the query
                query_executor.execute(query_type, Query, reprocess_sbnId)
                
                if query_executor.result_set:
                    result_set = query_executor.result_set
                    
                    # Convert result_set(ordered dictionary) to JSON object
                    subscription_json_object = json.dumps(result_set)
                    logging.info("subscription json object after update: %s", subscription_json_object)
                    
                    self.constructor_parameter_reinitialize()
                    self.subscription_data.append(json.loads(subscription_json_object, object_pairs_hook=OrderedDict))
            
            return self.subscription_data
        except Exception as ex:
            logging.exception(ex)
        
        
        finally:
            logging.info('reached subs finally block')
            db_connection.close()
    
    def get_subscription_dict(self):
        for subscriptionRecords in self.subscription_data:
            if subscriptionRecords:
                for subscriptionRecord in subscriptionRecords:
                    logging.info('subscription record: %s', subscriptionRecord)
                    if subscriptionRecord:
                        return subscriptionRecord
        
    def execute_update(self, query_executor, sbnId, subscriptionRecord):
        query_type = "UPDATE"
        Query = ""
        params = sbnId
        
        if (subscriptionRecord["SUB_STATUS"] not in ('E', 'F') and subscriptionRecord["task_type"] != 'N'
            and subscriptionRecord["task_status"] in (97, 98)):
            
            updateCriteria_object = UpdateQueryCriteria(subscriptionRecord)
            updateCriteria_object.update_query_formatter()
        
        if updateCriteria_object.update_query:
            Query = updateCriteria_object.update_query
            logging.info("Update query: %s", Query)
            # Execute update query
            query_executor.execute(query_type, Query, params)
            if query_executor.is_success:
                logging.info('is update success: %s', query_executor.is_success)
                self.process_subs_data = False
                self.get_subscription(False, sbnId)
            else:
                self.subscription_data = None
    
    def constructor_parameter_reinitialize(self):
        self.subscription_data = []
            
            
            