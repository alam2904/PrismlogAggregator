from collections import OrderedDict
import json
import logging
from prism_utils import get_db_parameters, query_executor

class ConfigManager:
    """
        This is the class responsible for db config fetch
    """
    def __init__(self, config, validation_object=None):
        # self.config = config
        self.validation_object = validation_object
        self.handler_info = []
        self.handler_map = []
        self.handler_processor_info = []
        self.handler_processor_map = []
        self.subtype_parameter = []
        self.db_name, self.db_host = get_db_parameters(config)
    
    def get_enabled_subtype_parameter(self):
        """
            initializes prism_config_params subtype boolean parameter
        """
        subType_Map = None
        try:
            if self.validation_object.is_multitenant_system:
                query = """
                            SELECT * FROM PRISM_CONFIG_PARAMS
                            WHERE SITE_ID = %s AND PARAM_NAME LIKE '%%SUBTYPE%%'
                        """ % (self.validation_object.site_id)
            else:
                query = """
                            SELECT * FROM PRISM_CONFIG_PARAMS
                            WHERE SITE_ID = -1 AND PARAM_NAME LIKE '%%SUBTYPE%%'
                        """
                        # """ % ("-1")

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                subType_Map = json.loads(configMap, object_pairs_hook=OrderedDict)
        except Exception as ex:
            logging.error(ex)
        
        return subType_Map
    
    def get_prism_config_param_value(self, module_name, site_id, param_name):
        """
            gets prism_config_param value
        """
        param_value = None
        
        try:
            query = """
                        SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS
                        WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME = %s
                    """ % (module_name, site_id, param_name)
            
            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
        
            if configMap:
                pcp_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if pcp_value_row:
                    for row in pcp_value_row:
                        param_value = row["PARAM_VALUE"]
        except Exception as ex:
            logging.error(ex)
        
        return param_value
            
    def get_services_ref_param_value(self, service_id, site_id, param_name):
        """
            gets prism_config_param value
        """
        param_value = None
        
        try:
            query = """
                        SELECT PARAM_VALUE FROM SERVICES_REF
                        WHERE SERVICE_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s
                    """ % (service_id, site_id, param_name)

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                sr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if sr_value_row:
                    for row in sr_value_row:
                        param_value = row["PARAM_VALUE"]
        except Exception as ex:
            logging.error(ex)
        
        return param_value
    
    def get_service_charges_ref_param_value(self, sc_id, site_id, param_name):
        """
            gets prism_config_param value
        """
        param_value = None
        
        try:
            query = """
                        SELECT PARAM_VALUE FROM SERVICE_CHARGES_REF
                        WHERE SC_ID = %s AND SITE_ID = %s AND PARAM_NAME = %s
                    """ % (sc_id, site_id, param_name)

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                scr_value_row = json.loads(configMap, object_pairs_hook=OrderedDict)
                if scr_value_row:
                    for row in scr_value_row:
                        param_value = row["PARAM_VALUE"]
        except Exception as ex:
            logging.error(ex)
            
        return param_value
    
    def is_multitenant_system(self):
        """
            returns true if it is a multitenant system
        """
        is_global_instance = False
        try:
            query = """
                        SELECT PARAM_VALUE FROM PRISM_CONFIG_PARAMS
                        WHERE MODULE_NAME = 'SYSTEM' AND SITE_ID = -1 AND PARAM_NAME = 'IS_SINGLE_INSTANCE'
                    """
                    # """ % ('SYSTEM', -1, 'IS_SINGLE_INSTANCE')

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            logging.info("CONFIG_MAP: %s", configMap)
            
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
        """
            maps generic flow handler from tables handler_processor_info & handler_processor_map
        """
        try:
            if self.validation_object.is_multitenant_system:
                query = """
                            SELECT hm.TRANSACTION_TYPE, hi.PARAMS FROM HANDLER_PROCESSOR_INFO hi
                            INNER JOIN HANDLER_PROCESSOR_MAP hm ON hi.HANDLER_ID = hm.HANDLER_ID
                            WHERE hi.HANDLER_NAME = 'com.onmobile.prism.generic.flowHandler.GenericFlowHandler'
                            AND hm.SITE_ID = %s GROUP BY hi.PARAMS, hm.TRANSACTION_TYPE
                        """ % (self.validation_object.site_id)
            else:
                query = """
                            SELECT hm.TRANSACTION_TYPE, hi.PARAMS FROM HANDLER_PROCESSOR_INFO hi
                            INNER JOIN HANDLER_PROCESSOR_MAP hm ON hi.HANDLER_ID = hm.HANDLER_ID
                            WHERE hi.HANDLER_NAME = 'com.onmobile.prism.generic.flowHandler.GenericFlowHandler'
                            AND hm.SITE_ID = %s GROUP BY hi.PARAMS, hm.TRANSACTION_TYPE
                        """ % ("-1")
                
            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                return json.loads(configMap, object_pairs_hook=OrderedDict)
        except Exception as ex:
            logging.error(ex)
        
        return None
    
    def get_operator_site_map(self, operator_id):
        """
            does operator site mapping
        """
        operator_site_map = None
        
        try:
            query = """
                        SELECT SITE_ID, TIME_ZONE, FILE_IDS FROM OPERATOR_SITE_MAP
                        WHERE OPERATOR_ID = %s
                    """ % (operator_id)

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                operator_site_map = json.loads(configMap, object_pairs_hook=OrderedDict)
                if operator_site_map:
                    for row in operator_site_map:
                        logging.info("SITE_ID: %s AND TIME_ZONE: %s AND FILE_IDS: %s", row["SITE_ID"], row["TIME_ZONE"], row["FILE_IDS"])
                        operator_site_map = row["SITE_ID"], row["TIME_ZONE"], row["FILE_IDS"] 
        except KeyError as ex:
            logging.exception("operator_id: %s site map is not found", operator_id, ex)
        
        return operator_site_map
    
    def get_operator_url_map(self, operator_id):
        """
            does operator url mapping
        """
        urls = []
        
        try:
            query = """
                        SELECT OPERATOR_URL FROM OPERATOR_URL_MAPPING
                        WHERE OPERATOR_ID = %s
                    """ % (operator_id)
                    
            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                urls.extend([row["OPERATOR_URL"] for row in json.loads(configMap, object_pairs_hook=OrderedDict)])
                logging.info("URL_LIST: %s", urls)
        except KeyError as ex:
            logging.exception("operator_id: %s url map is not found", operator_id, ex)
        
        return urls
    
    def get_operator_url_from_pcp(self, module_name, site_id):
        """
            gets operator url from prism_config_param
        """
        urls = []
        
        try:
            query = """
                        SELECT PARAM_NAME FROM PRISM_CONFIG_PARAMS
                        WHERE MODULE_NAME = %s AND SITE_ID = %s AND PARAM_NAME LIKE '%%:action'
                    """ % (module_name, site_id)

            configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
            
            if configMap:
                param_rows = json.loads(configMap, object_pairs_hook=OrderedDict)
                if param_rows:
                    for row in param_rows:
                        urls.append(str(row["PARAM_NAME"]).split(":")[0])
        except Exception as ex:
            logging.error(ex)
        
        return urls
        
    def get_handler_info(self, issue_handler_task_type_map):
        """
            gets handler info
        """
        handler_id_list = []
        
        try:
            for params in issue_handler_task_type_map:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                
                if handler_id and handler_id not in handler_id_list:
                    handler_id_list.append(handler_id)
            
            for handler_id in handler_id_list:
                try:
                    query = """
                                SELECT * FROM HANDLER_INFO
                                WHERE HANDLER_ID = %s
                            """ % (handler_id)
                    
                    configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
                    
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
        """
            gets handler map
        """
        for params in issue_handler_task_type_map:
            try:
                task_type, handler_id, sub_type, srv_id, flow_id = params
                if handler_id:
                    handler_id = '%' + handler_id + '%'
                
                    if self.validation_object.is_multitenant_system:
                        query_srv = """
                                        SELECT * FROM HANDLER_MAP
                                        WHERE SITE_ID = %s AND TASK_TYPE = %s
                                        AND HANDLER_ID LIKE %s AND SUB_TYPE = %s AND SRV_ID in %s
                                    """ % (self.validation_object.site_id, task_type, handler_id, sub_type, srv_id)
                        
                        query = """
                                    SELECT * FROM HANDLER_MAP
                                    WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID LIKE %s
                                    AND SUB_TYPE = %s AND (FLOW_ID in %s OR FLOW_ID = '-1')
                                """ % (self.validation_object.site_id, task_type, handler_id, sub_type, flow_id)
                    else:
                        query_srv = """
                                        SELECT * FROM HANDLER_MAP
                                        WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID LIKE %s
                                        AND SUB_TYPE = %s AND SRV_ID in %s
                                    """ % ("-1", task_type, handler_id, sub_type, srv_id)
                        
                        query = """
                                    SELECT * FROM HANDLER_MAP
                                    WHERE SITE_ID = %s AND TASK_TYPE = %s AND HANDLER_ID LIKE %s
                                    AND SUB_TYPE = %s AND (FLOW_ID in %s OR FLOW_ID = '-1')
                                """ % ("-1", task_type, handler_id, sub_type, flow_id)
                    
                    configMap = query_executor(self.db_name, self.db_host, query_srv, "SELECT")
                    
                    if configMap:
                        pass
                    else:
                        configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
                    
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
                query = """
                            SELECT FILE_ID, FILE_PREFIX, FILE_DATETIME_FMT,
                            FILE_SUFFIX, FILE_LOCAL_DIR FROM FILE_INFO
                            WHERE FILE_ID in %s AND FILE_TYPE = 'PUSH'
                        """ % (tuple(file_ids))

                configMap = query_executor(self.db_name, self.db_host, query, "SELECT")
                
                if configMap:
                    file_info.extend(json.loads(configMap, object_pairs_hook=OrderedDict))
                else:
                    logging.debug("No file_info configured for file_ids = %s", file_ids)           
            except Exception as ex:
                logging.info(ex)
                
        if file_info:
            return file_info
  
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']