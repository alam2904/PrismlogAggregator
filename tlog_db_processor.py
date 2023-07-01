import logging
from tlog import Tlog
from handler_files import HandlerFileProcessor
from generic_server_processing import GENERIC_SERVER_PROCESSOR

class TlogProcessor:
    """
    Parse the tlog for various conditions
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object, log_mode, config,\
                    prism_data_dict_list, prism_data_dict,\
                    prism_ctid, prism_tomcat_tlog_dict, prism_daemon_tlog_dict,\
                    prism_daemon_tlog_thread_dict, prism_tomcat_tlog_thread_dict,\
                    prism_tomcat_handler_generic_http_req_resp_dict,\
                    prism_daemon_handler_generic_http_req_resp_dict,\
                    prism_tomcat_handler_generic_soap_req_resp_dict,\
                    prism_daemon_handler_generic_soap_req_resp_dict,\
                    prism_tomcat_request_log_dict, prism_daemon_request_log_dict,\
                    prism_tomcat_callbackV2_log_dict, prism_daemon_callbackV2_log_dict,\
                    prism_tomcat_perf_log_dict, prism_daemon_perf_log_dict,\
                    prism_handler_info_dict, issue_task_types, issue_handler_task_type_map,\
                    prism_smsd_tlog_dict, non_issue_sbn_thread_dict, oarm_uid):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.log_mode = log_mode
        self.config = config
        self.prism_data_dict_list = prism_data_dict_list
        self.prism_data_dict = prism_data_dict
        
        self.prism_ctid = prism_ctid
        self.prism_tomcat_tlog_dict = prism_tomcat_tlog_dict
        self.prism_daemon_tlog_dict = prism_daemon_tlog_dict
        self.prism_daemon_tlog_thread_dict = prism_daemon_tlog_thread_dict
        self.prism_tomcat_tlog_thread_dict = prism_tomcat_tlog_thread_dict
        
        self.prism_tomcat_handler_generic_http_req_resp_dict = prism_tomcat_handler_generic_http_req_resp_dict
        self.prism_daemon_handler_generic_http_req_resp_dict = prism_daemon_handler_generic_http_req_resp_dict
        self.prism_tomcat_handler_generic_soap_req_resp_dict = prism_tomcat_handler_generic_soap_req_resp_dict
        self.prism_daemon_handler_generic_soap_req_resp_dict = prism_daemon_handler_generic_soap_req_resp_dict
        
        self.prism_tomcat_request_log_dict = prism_tomcat_request_log_dict
        self.prism_daemon_request_log_dict = prism_daemon_request_log_dict
        self.prism_tomcat_callbackV2_log_dict = prism_tomcat_callbackV2_log_dict
        self.prism_daemon_callbackV2_log_dict = prism_daemon_callbackV2_log_dict
        self.prism_tomcat_perf_log_dict = prism_tomcat_perf_log_dict
        self.prism_daemon_perf_log_dict = prism_daemon_perf_log_dict
        self.prism_handler_info_dict = prism_handler_info_dict
        self.issue_task_types = issue_task_types
        self.issue_handler_task_type_map = issue_handler_task_type_map
        
        self.prism_smsd_tlog_dict = prism_smsd_tlog_dict
        self.non_issue_sbn_thread_dict = non_issue_sbn_thread_dict
        self.oarm_uid = oarm_uid
        
        self.combined_perf_data = []
        
    def process_tlog(self, pname):
        
        #tlog object
        tlog_object = Tlog(self.initializedPath_object, self.outputDirectory_object, self.validation_object,\
                            self.log_mode, self.prism_data_dict_list, self.prism_data_dict, self.config,\
                            self.prism_ctid, self.prism_tomcat_tlog_dict, self.prism_daemon_tlog_dict,\
                            self.prism_daemon_tlog_thread_dict, self.prism_tomcat_tlog_thread_dict,\
                            self.prism_tomcat_handler_generic_http_req_resp_dict,\
                            self.prism_daemon_handler_generic_http_req_resp_dict,\
                            self.prism_tomcat_handler_generic_soap_req_resp_dict,\
                            self.prism_daemon_handler_generic_soap_req_resp_dict,\
                            self.prism_tomcat_request_log_dict, self.prism_daemon_request_log_dict,\
                            self.prism_tomcat_callbackV2_log_dict, self.prism_daemon_callbackV2_log_dict,\
                            self.prism_tomcat_perf_log_dict, self.prism_daemon_perf_log_dict, self.combined_perf_data,\
                            self.prism_handler_info_dict, self.issue_task_types, self.issue_handler_task_type_map,\
                            self.prism_smsd_tlog_dict, self.non_issue_sbn_thread_dict, self.oarm_uid)
          
        if pname == "PRISM_TOMCAT":
            # fetching prism tomcat access and tlog
            self.prism_tomcat_tlog_dict = tlog_object.get_tlog(pname)
            
            if self.prism_tomcat_tlog_dict:
                #fetching prism tomcat generic http handler request response
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP")
                
                #fetching prism tomcat generic soap handler request response
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP")
                
                #fetching prism tomcat request response and event callback v2 included
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_REQ_RESP")
                
                #fetching prism tomcat callback v2 request response
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_callbackV2_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_CALLBACK_V2_REQ_RESP")
                
                #fetching prism tomcat perf log
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_PERF_LOG")
                
                if self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"]:
                    generic_server_object = GENERIC_SERVER_PROCESSOR(self.initializedPath_object, self.outputDirectory_object, self.prism_data_dict_list, self.validation_object)
                    generic_server_object.process_generic_server_tlog(self.prism_tomcat_tlog_dict["PRISM_TOMCAT_BILLING_TLOG"])
            else:
                logging.info("NO REALTIME TLOG PRESENT")
                
        elif pname == "PRISM_DEAMON":
            # fetching prism daemon tlog
            self.prism_daemon_tlog_dict = tlog_object.get_tlog(pname)
            
            if self.prism_daemon_tlog_dict:
                #fetching prism daemon generic http handler request response
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_GENERIC_HTTP_REQ_RESP")
                
                #fetching prism daemon generic soap handler request response
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_GENERIC_SOAP_REQ_RESP")
                
                #fetching prism daemon request response and event callback v2 included
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_REQ_RESP")
                
                #fetching prism daemon callback v2 request response
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_callbackV2_req_resp_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_CALLBACK_V2_REQ_RESP")
                
                #fetching prism daemon perf log
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_PERF_LOG")
                
                if self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"]:
                    generic_server_object = GENERIC_SERVER_PROCESSOR(self.initializedPath_object, self.outputDirectory_object, self.prism_data_dict_list, self.validation_object)
                    generic_server_object.process_generic_server_tlog(self.prism_daemon_tlog_dict["PRISM_DAEMON_TLOG"])
                
                subscriptions_data_dict = tlog_object.get_subscription_details()
                
                logging.info('issue tasks are: %s', self.issue_task_types)
                if self.issue_task_types:
                    handler_info = tlog_object.get_issue_handler_details(subscriptions_data_dict)
                    
                    if handler_info:
                        handlerfile_object = HandlerFileProcessor(self.config, handler_info, self.outputDirectory_object, self.oarm_uid)
                        handlerfile_object.getHandler_files()
            else:
                logging.info("NO NON-REALTIME TLOG PRESENT")
                    
        elif pname == "PRISM_SMSD":
            tlog_object.get_tlog(pname)
            