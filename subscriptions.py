from collections import OrderedDict
import logging
import json
from database_connection import DatabaseConnection
from query_executor import QueryExecutor
from datetime import datetime


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
                        
                        if is_reprocessing_required:
                            if self.pname == "PRISM_TOMCAT":
                                current_system_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                if self.subscription_data["charge_schedule"] > current_system_datetime:
                                    self.update_query_formatter(query_executor, sbnId, self.subscription_data)
                            else:
                                self.update_query_formatter(query_executor, sbnId)
                           
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
        
    def update_query_formatter(self, query_executor, sbnId):
        query_type = "UPDATE"
        Query = ""
        
        # logging.info('subscription: %s', self.subscription_data)
        for subscriptionRecords in self.subscription_data:
            if subscriptionRecords:
                for subscriptionRecord in subscriptionRecords:
                    logging.info('subscription record: %s', subscriptionRecord)
                    if subscriptionRecord:
                        if (subscriptionRecord["SUB_STATUS"] not in ('E', 'F') and (subscriptionRecord["task_type"] != 'N')):
                            if subscriptionRecord["pmt_status"] in (3, 40) and subscriptionRecord["task_type"] == 'Q':
                                queue_id = 99,
                                task_status = 0
                                sbn_id = sbnId
                                
                                params = (queue_id, task_status, sbn_id)    
                                Query = "UPDATE subscriptions set queue_id = %s, task_status = %s, charge_schedule = now() where sbn_id = %s"
                            
                            elif subscriptionRecord["pmt_status"] == 3 and subscriptionRecord["task_type"] != 'Q':
                                queue_id = 99,
                                task_status = 0
                                sbn_id = sbnId
                                
                                params = (queue_id, task_status, sbn_id)
                                Query = "UPDATE SUBSCRIPTIONS SET queue_id = %s, task_status = %s, charge_schedule = now() where sbn_id = %s"
            
        
        if Query and params:
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
            
            
            