import logging
from tlog import Tlog
# from generic_server_processing import GENERIC_SERVER_PROCESSOR

class TlogProcessor:
    """
    Parse the tlog for various conditions
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object,\
                    config, prism_data_dict_list):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.config = config
        self.prism_data_dict_list = prism_data_dict_list
        
    def process_tlog_db_enteries(self, pname):
        
        #tlog object
        tlog_object = Tlog(self.initializedPath_object, self.outputDirectory_object, self.validation_object,\
                            self.prism_data_dict_list, self.config)
        
        if pname == "PRISM_TOMCAT":
            # fetching prism tomcat access and tlog
            self.prism_tomcat_tlog_dict = tlog_object.get_tlog(pname)
            
            if self.prism_tomcat_tlog_dict:
                #fetching prism tomcat perf log
                if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]:
                    tlog_object.get_tlog("PRISM_TOMCAT_PERF_LOG")
                    
                # if self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"]:
                #     generic_server_object = GENERIC_SERVER_PROCESSOR(self.initializedPath_object, self.outputDirectory_object,\
                #                             self.prism_data_dict_list, self.validation_object, self.config, self.log_mode, self.oarm_uid)
                #     generic_server_object.process_generic_server_tlog(self.prism_tomcat_tlog_dict["PRISM_TOMCAT_BILLING_TLOG"])    
            else:
                logging.info("NO REALTIME TLOG PRESENT")
                
        elif pname == "PRISM_DEAMON":
            # fetching prism daemon tlog
            self.prism_daemon_tlog_dict = tlog_object.get_tlog(pname)
            
            if self.prism_daemon_tlog_dict:
                #fetching prism daemon perf log
                if self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"]:
                    tlog_object.get_tlog("PRISM_DAEMON_PERF_LOG")
                
                # if self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"]:
                #     generic_server_object = GENERIC_SERVER_PROCESSOR(self.initializedPath_object, self.outputDirectory_object,\
                #                             self.prism_data_dict_list, self.validation_object, self.config, self.log_mode, self.oarm_uid)
                #     generic_server_object.process_generic_server_tlog(self.prism_daemon_tlog_dict["PRISM_DAEMON_TLOG"])
        
            else:
                logging.info("NO NON-REALTIME TLOG PRESENT")
                    
        elif pname == "PRISM_SMSD":
            tlog_object.get_tlog(pname)
            