from datetime import datetime, timedelta
import logging
import os
import socket

class LogFileFinder:
    """
    log file finder class
    """
    def __init__(self, initializedPath_object, validation_object, config):
    
        self.initializedPath_object = initializedPath_object
        self.validation_object = validation_object
        self.config = config
        
        self.start_date = validation_object.start_date
        self.end_date = validation_object.end_date
        
        self.access_log_files = []
        
        self.tlog_files = []
        self.tlog_dir = ""
      
        self.input_date = []
        self.hostname = socket.gethostname()
        
        self.s_date = datetime.strptime(datetime.strftime(self.start_date, "%Y%m%d"), "%Y%m%d")
        self.e_date = datetime.strptime(datetime.strftime(self.end_date, "%Y%m%d"), "%Y%m%d")
    
    def get_tlog_files(self, pname):
        
        #re-initializing constructor parameters
        self.constructor_paramter_reinitialize()
        splitted_tlog_path = ""
        
        if pname == "PRISM_TOMCAT":
            #current tlog file
            self.tlog_files.append('{}/TLOG_BILLING_REALTIME_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"]))
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"]
        
        elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
            #current generic http req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"] + "/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_*.tmp")

            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"]
        
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
            #current generic soap req-resp tlog files
            self.tlog_files.append('{}/TLOG_REQUEST_RESPONSE_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"]))
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"]
        elif pname == "PRISM_TOMCAT_REQ_RESP":
            #current req-resp log files
            self.tlog_files.append('{}/TLOG_REQUEST_LOG_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_req_resp_path"]))
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_req_resp_path"]
        
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            #current callback v2 req-resp log files
            self.tlog_files.append('{}/TLOG_CBCK-V2-REQ-RESPONSE_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_callbackV2_req_resp_path"]))
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_callbackV2_req_resp_path"]
        
        elif pname == "PRISM_TOMCAT_PERF_LOG":
            #current perf log files
            self.tlog_files.append('{}/TLOG_PERF_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]))
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]
            
        elif pname == "PRISM_DEAMON":
            #current tlog file
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"] + "/TLOG_BILLING_*.tmp")
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"]
        
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            #current generic http req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"] + "/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_*.tmp")
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            #current generic soap req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"] + "/TLOG_REQUEST_RESPONSE_*.tmp")
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_REQ_RESP":
            #current generic soap req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_req_resp_path"] + "/TLOG_REQUEST_LOG_*.tmp")
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            #current generic soap req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_callbackV2_req_resp_path"] + '/TLOG_CBCK-V2-REQ-RESPONSE_*.tmp')
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_callbackV2_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_PERF_LOG":
            #current generic soap req-resp tlog files
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"] + "/TLOG_PERF_*.tmp")
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"]
        
        elif pname == "PRISM_SMSD":
            #current tlog file
            self.tlog_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_tlog_path"] + '/TLOG_SMS_*.tmp')
            self.tlog_dir = self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_tlog_path"]
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON"\
            or pname == "PRISM_SMSD" or pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP"\
            or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP"\
            or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP" or pname == "PRISM_TOMCAT_REQ_RESP"\
            or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP" or pname == "PRISM_DAEMON_REQ_RESP"\
            or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP" or pname == "PRISM_TOMCAT_PERF_LOG"\
            or pname == "PRISM_DAEMON_PERF_LOG":
            
            path = os.path.join(self.tlog_dir)
            
            #method call to date range list
            self.input_date = self.date_range_list(self.s_date, self.e_date)
            
            for date in self.input_date:
                # logging.info('search date is: %s', datetime.strftime(date, "%Y-%m-%d"))
                input_date_formatted = ""
                
                if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD"\
                    or pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG":           
                    
                    input_date_formatted = datetime.strftime(date, "%Y%m%d")
                
                #input dated file in the tlog directory
                dated_tlog_files = ""
                
                if pname == "PRISM_TOMCAT":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_BILLING_REALTIME_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_RESPONSE_{}_".format(input_date_formatted)) and p.endswith(".log")]
                    logging.info('tomcat soap path: %s', dated_tlog_files)
                elif pname == "PRISM_TOMCAT_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_LOG_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_CBCK-V2-REQ-RESPONSE_{}_" .format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_TOMCAT_PERF_LOG":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_PERF_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DEAMON":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_BILLING_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_RESPONSE_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DAEMON_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_REQUEST_LOG_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_CBCK-V2-REQ-RESPONSE_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_DAEMON_PERF_LOG":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_PERF_{}_".format(input_date_formatted)) and p.endswith(".log")]
                elif pname == "PRISM_SMSD":
                    dated_tlog_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("TLOG_SMS_{}_".format(input_date_formatted)) and p.endswith(".log")]
                    
                if bool(dated_tlog_files):
                    if pname == "PRISM_TOMCAT":
                        logging.info("TLOG_BILLING_REALTIME_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DEAMON":
                        logging.info("TLOG_BILLING_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file present".format(input_date_formatted))
                    
                    elif pname == "PRISM_SMSD":
                        logging.info("TLOG_SMS_{}_*..log file present".format(input_date_formatted))
                        
                    for files in dated_tlog_files:
                        self.tlog_files.append(str(files))
                    logging.info('tlog files ex: %s', self.tlog_files)
                else:
                    if pname == "PRISM_TOMCAT":
                        logging.info("TLOG_BILLING_REALTIME_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file not present".format(input_date_formatted))
                    
                    elif pname == "PRISM_DEAMON":
                        logging.info("TLOG_BILLING_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
                        logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file not present".format(input_date_formatted))
                    
                    elif pname == "PRISM_SMSD":
                        logging.info("TLOG_SMS_{}_*..log file not present".format(input_date_formatted))
        
        return self.tlog_files
    
    def get_tomcat_access_files(self, pname):
        #re-initializing constructor parameters
        self.constructor_paramter_reinitialize()
        
        hostname = socket.gethostname()
        access_log_path = ""
        access_log_prefix = ""
        access_log_suffix = ""
        
        try:
            if pname == "PRISM_TOMCAT":
                access_log_prefix = self.config[hostname]["PRISM"]["PRISM_TOMCAT"]["ACCESS_LOG_PREFIX"]
                access_log_suffix = self.config[hostname]["PRISM"]["PRISM_TOMCAT"]["ACCESS_LOG_SUFFIX"]
                access_log_path = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]
        
        except KeyError as ex:
            logging.info('key error: %s', ex)
                
        path = os.path.abspath(access_log_path)
        
        #method call to date range list
        self.input_date = self.date_range_list(self.s_date, self.e_date)
        
        for date in self.input_date:
            # logging.info('search date is: %s', datetime.strftime(date, "%Y-%m-%d"))
            input_date_formatted = datetime.strftime(date, "%Y-%m-%d")            
            
            #input dated access file in the access log path
            
            dated_access_files = [os.path.join(path, p) for p in os.listdir(path) if p.startswith("{0}.{1}".format(access_log_prefix, input_date_formatted)) and p.endswith("{}".format(access_log_suffix))]
                        
            if bool(dated_access_files):
                logging.info("{0}.{1}{2} file present".format(access_log_prefix, input_date_formatted, access_log_suffix))        
    
            for files in dated_access_files:
                self.access_log_files.append(str(files))
        
        return self.access_log_files
                    
    def date_range_list(self, start_date, end_date):
        # Return list of datetime.date objects between start_date and end_date (inclusive).
        date_list = []
        curr_date = start_date
        while curr_date <= end_date:
            date_list.append(curr_date)
            curr_date += timedelta(days=1)
        return date_list
        
    def constructor_paramter_reinitialize(self):
        self.access_log_files = []
        self.tlog_files = []
        self.input_date = []
        self.tlog_dir = ""