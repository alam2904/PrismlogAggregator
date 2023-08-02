from collections import OrderedDict
import logging
import json
from database_connection import DatabaseConnection
from query_executor import QueryExecutor
from datetime import datetime
from update_query_criteria import UpdateQueryCriteria


class SubscriptionEventController:
    """
        This is the class responsible for SELECT and UPDATE db query processing
        based on various subscription condition 
    """
    def __init__(self, config, pname, validation_object, sbn_thread_dict, process_subs_data):
        self.sbn_thread_dict = sbn_thread_dict
        self.validation_object = validation_object
        self.process_subs_data = process_subs_data
        self.transaction_table_data = []
        self.config = config
        self.pname = pname
        
    def get_subscription_event(self, transaction_table, is_reprocessing_required, reprocess_sbnId=None):
        # Create a DatabaseConnection instance
        db_connection = DatabaseConnection(
            host= "172.19.113.108",
            user="root",
            passwd="Onm0bile",
            db="safaricom"
        )

        # Connect to the database
        db_connection.create_connection()
        
        try:
            if self.process_subs_data:
                for sbn_event_Id, thread in self.sbn_thread_dict.items():
                    # Prepare the SQL statement
                    if transaction_table == "SUBSCRIPTIONS":
                        Query = "SELECT * FROM SUBSCRIPTIONS WHERE SBN_ID = %s"
                    elif transaction_table == "EVENTS":
                        self.transaction_table_data = []
                        Query = "SELECT * FROM EVENTS WHERE EVENT_ID = %s"
                        
                    query_type = "SELECT"
                    logging.info('SELECT_QUERY: %s and params: %s', Query, sbn_event_Id)
                    
                    # Create a QueryExecutor instance with the connection object
                    query_executor = QueryExecutor(db_connection)

                    # Execute the query
                    query_executor.execute(query_type, Query, sbn_event_Id)
                    
                    if query_executor.result_set:
                        result_set = query_executor.result_set
                        
                        # Convert result_set(ordered dictionary) to JSON object.
                        subscription_json_object = json.dumps(result_set)
                        logging.info("subscription json object: %s", subscription_json_object)
                        
                        self.transaction_table_data.append(json.loads(subscription_json_object, object_pairs_hook=OrderedDict))
                        
                        if is_reprocessing_required and self.transaction_table_data:
                            subscriptionRecord = self.get_subscription_event_dict(transaction_table)
                            if subscriptionRecord:
                                # if self.pname == "PRISM_TOMCAT":
                                #     current_system_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                #     if subscriptionRecord["charge_schedule"] > current_system_datetime:
                                #         self.execute_update(query_executor, sbnId, subscriptionRecord)
                                # else:
                                self.execute_update(query_executor, sbn_event_Id, subscriptionRecord)
                           
            else:
                Query = "SELECT * FROM SUBSCRIPTIONS WHERE SBN_ID = %s"
                
                query_type = "SELECT"
                # logging.info('SELECT_QUERY: %s', Query)
                
                # Create a QueryExecutor instance with the connection object
                query_executor = QueryExecutor(db_connection)

                # Execute the query
                query_executor.execute(query_type, Query, reprocess_sbnId)
                
                if query_executor.result_set:
                    result_set = query_executor.result_set
                    
                    # Convert result_set(ordered dictionary) to JSON object
                    subscription_event_json_object = json.dumps(result_set)
                    logging.info("subscription json object after update: %s", subscription_event_json_object)
                    
                    self.constructor_parameter_reinitialize()
                    self.transaction_table_data.append(json.loads(subscription_event_json_object, object_pairs_hook=OrderedDict))
            
            return self.transaction_table_data
        except Exception as ex:
            logging.exception(ex)
                   
        finally:
            logging.info('reached subs finally block')
            db_connection.close()
    
    def get_subscription_event_dict(self):
        for subscriptionEventRecords in self.transaction_table_data:
            logging.info('subscription_data: %s', subscriptionEventRecords)
            if subscriptionEventRecords:
                for subscriptionEventRecord in subscriptionEventRecords:
                    logging.info('subscription record: %s', subscriptionEventRecord)
                    if subscriptionEventRecord:
                        return subscriptionEventRecord
        
    def execute_update(self, query_executor, sbnId, subscriptionRecord):
        query_type = "UPDATE"
        Query = ""
        params = sbnId
        updateCriteria_object = UpdateQueryCriteria(self.config, self.validation_object, subscriptionRecord)
        logging.info("SUBSCRIPTIONS_SUB_STATUS: %s", subscriptionRecord["sub_status"])
        try:
            if (subscriptionRecord["sub_status"] not in ('E', 'F') and subscriptionRecord["task_type"] != 'N'
                and subscriptionRecord["task_status"] in (97, 98)):
                
                updateCriteria_object.update_query_formatter()
        except KeyError as err:
            logging.debug(err)
            
        if updateCriteria_object.update_query:
            Query = updateCriteria_object.update_query
            
            if updateCriteria_object.next_task_type:
                params = updateCriteria_object.next_task_type, sbnId
                
            logging.info("Update query: %s", Query)
            # Execute update query
            query_executor.execute(query_type, Query, params)
            if query_executor.is_success:
                logging.info('is update success: %s', query_executor.is_success)
                self.process_subs_data = False
                self.get_subscription_event("SUBSCRIPTIONS", False, sbnId)
            else:
                self.transaction_table_data = None
        else:
            self.transaction_table_data = None
            
    
    def constructor_parameter_reinitialize(self):
        self.transaction_table_data = []
            
            
            