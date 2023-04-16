import logging
from tlog import Tlog

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
                    prism_smsd_tlog_dict, oarm_uid):
        
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
        
        self.prism_smsd_tlog_dict = prism_smsd_tlog_dict
        self.oarm_uid = oarm_uid
        
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
                            self.prism_tomcat_perf_log_dict, self.prism_daemon_perf_log_dict,\
                            self.prism_smsd_tlog_dict, self.oarm_uid)
          
        if pname == "PRISM_TOMCAT":
            # fetching prism tomcat access and tlog
            self.prism_tomcat_tlog_dict = tlog_object.get_tlog(pname)
            
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
                
        elif pname == "PRISM_DEAMON":
            # fetching prism daemon tlog
            tlog_object.get_tlog(pname)
            
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
        
        elif pname == "PRISM_SMSD":
            tlog_object.get_tlog(pname)
            