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
        if self.validation_object.is_multitenant_system:
            Query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE SITE_ID = %s AND PARAM_NAME LIKE '%%SUBTYPE%%'"
            params = (self.validation_object.site_id,)
        else:
            Query = "SELECT * FROM PRISM_CONFIG_PARAMS WHERE PARAM_NAME LIKE '%SUBTYPE%'"
            params = (-1,)
        logging.info("PARAMS: %s", params)
        configMap = self.get_db_config_map(Query, params, self.db_connection)
        
        if configMap:
            self.subtype_parameter.append(json.loads(configMap, object_pairs_hook=OrderedDict))
    
    def get_prism_config_param_value(self, module_name, site_id, param_name):
        #get prism_config_param value
        param_value = None

        Query = "SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME = %s"
        params = (module_name, site_id, param_name)
            
        configMap = self.get_db_config_map(Query, params, self.db_connection)
        
        if configMap:
            pcp_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
            if pcp_value_row:
                for row in pcp_value_row:
                    param_value = row["PARAM_VALUE"]
        return param_value
    
    def get_services_ref_param_value(self, service_id, site_id, param_name):
        #get prism_config_param value
        param_value = None

        Query = "SELECT PARAM_VALUE FROM SERVICES_REF WHERE SERVICE_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s"
        params = (service_id, site_id, param_name)
            
        configMap = self.get_db_config_map(Query, params, self.db_connection)
        
        if configMap:
            sr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
            if sr_value_row:
                for row in sr_value_row:
                    param_value = row["PARAM_VALUE"]
        return param_value
    
    def get_service_charges_ref_param_value(self, sc_id, site_id, param_name):
        #get prism_config_param value
        param_value = None

        Query = "SELECT PARAM_VALUE FROM SERVICE_CHARGES_REF WHERE SC_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s"
        params = (sc_id, site_id, param_name)

        configMap = self.get_db_config_map(Query, params, self.db_connection)
        
        if configMap:
            scr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
            if scr_value_row:
                for row in scr_value_row:
                    param_value = row["PARAM_VALUE"]
        return param_value
    
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
    
    def get_operator_site_map(self, operator_id):
        #initializing prism_config_params subtype boolean parameter
        operator_site_map = None
        try:
            Query = "SELECT SITE_ID, TIME_ZONE FROM OPERATOR_SITE_MAP WHERE OPERATOR_ID = %s"
            params = (operator_id,)
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                operator_site_map = json.loads(configMap, object_pairs_hook=OrderedDict)
                if operator_site_map:
                    for row in operator_site_map:
                        logging.info("SITE_ID: %s AND TIME_ZONE: %s", row["SITE_ID"], row["TIME_ZONE"])
                        operator_site_map = row["SITE_ID"], row["TIME_ZONE"]    
            return operator_site_map
        except KeyError as ex:
            logging.exception("operator_id: %s site map is not found", operator_id, ex)
    
    def get_operator_url_map(self, operator_id):
        #initializing prism_config_params subtype boolean parameter
        operator_url_map = None
        try:
            Query = "SELECT OPERATOR_URL FROM OPERATOR_URL_MAPPING WHERE OPERATOR_ID = %s"
            params = (operator_id,)
            configMap = self.get_db_config_map(Query, params, self.db_connection)
            
            if configMap:
                operator_url_map = json.loads(configMap, object_pairs_hook=OrderedDict)
                if operator_url_map:
                    for row in operator_url_map:
                        logging.info("OPERATOR_URL: %s", row["OPERATOR_URL"])
                        operator_url_map = row["OPERATOR_URL"]    
            return operator_url_map
        except KeyError as ex:
            logging.exception("operator_id: %s url map is not found", operator_id, ex)
    
    def get_operator_url_from_pcp(self, module_name, site_id):
        #get prism_config_param value
        operator_url = None

        Query = "SELECT PARAM_NAME FROM PRISM_CONFIG_PARAMS WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME LIKE '%%:action'"
        params = (module_name, site_id)
            
        configMap = self.get_db_config_map(Query, params, self.db_connection)
        
        if configMap:
            param_row = json.loads(configMap, object_pairs_hook=OrderedDict)
            if param_row:
                for row in param_row:
                    operator_url = str(row["PARAM_NAME"]).split(":")[0]
        return operator_url
        
    def get_handler_info(self, issue_handler_task_type_map):
        # Connect to the database
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                # Prepare the SQL statement
                if handler_id:
                    Query = "SELECT * FROM HANDLER_INFO WHERE HANDLER_ID = %s"
                    params = (handler_id,)
                        
                    configMap = self.get_db_config_map(Query, params, self.db_connection)
                    
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
                task_type, handler_id, sub_type, srv_id, flow_id = params
                
                if self.validation_object.is_multitenant_system:
                    sparam = (self.validation_object.site_id, task_type, handler_id, sub_type, srv_id)
                    wsparam = (self.validation_object.site_id, task_type, handler_id, sub_type, flow_id)
                else:
                    sparam = (-1, task_type, handler_id, sub_type, srv_id)
                    wsparam = (-1, task_type, handler_id, sub_type, flow_id)
                    
                if handler_id:
                    Query_srv = "SELECT * FROM HANDLER_MAP WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID = %s AND SUB_TYPE = %s AND SRV_ID in %s"
                    
                    Query = "SELECT * FROM HANDLER_MAP WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID = %s AND SUB_TYPE = %s AND (FLOW_ID in %s OR FLOW_ID = '-1')"

                    configMap = self.get_db_config_map(Query_srv, sparam, self.db_connection)
                    
                    if configMap:
                        pass
                    else:
                        configMap = self.get_db_config_map(Query, wsparam, self.db_connection)
                    
                    self.handler_map.append(json.loads(configMap, object_pairs_hook=OrderedDict))
                    
        except Exception as ex:
            logging.info(ex)
        
        logging.info("HANDLER_MAP: %s", self.handler_map)
    
    def get_db_config_map(self, query, params, conn):
        configMap = None
        logging.info('SELECT_QUERY: %s AND PARAMS: %s', query, params)
        
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