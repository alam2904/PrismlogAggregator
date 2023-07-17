from datetime import datetime, timedelta
import glob
import logging
import os
import re
import socket
from sys import prefix

def extract_hour(filename):
    return int(filename.split('_')[-1].split('.')[0])
    
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
    
    def get_tlog_files(self, pname, last_modified_time=None):
        
        #re-initializing constructor parameters
        self.constructor_paramter_reinitialize()
        splitted_tlog_path = ""
        
        logging.info("first dated file would be appended")
        
        if pname == "PRISM_TOMCAT":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"]
                
        elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"]
                
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"]
                
        elif pname == "PRISM_TOMCAT_REQ_RESP":
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_req_resp_path"]
        
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_callbackV2_req_resp_path"]
        
        elif pname == "PRISM_TOMCAT_PERF_LOG":
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]
            
        elif pname == "PRISM_DEAMON":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"]
                
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"]
                
        elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"] + "_{}".format(self.validation_object.site_id)
            else:
                self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"]
                
        elif pname == "PRISM_DAEMON_REQ_RESP":
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_callbackV2_req_resp_path"]
        
        elif pname == "PRISM_DAEMON_PERF_LOG":
            self.tlog_dir = self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"]
        elif pname == "GENERIC_SERVER":
            self.tlog_dir = self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"]
        
        elif pname == "PRISM_SMSD":
            self.tlog_dir = self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_tlog_path"]
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON"\
            or pname == "PRISM_SMSD" or pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP"\
            or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP"\
            or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP" or pname == "PRISM_TOMCAT_REQ_RESP"\
            or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP" or pname == "PRISM_DAEMON_REQ_RESP"\
            or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP" or pname == "PRISM_TOMCAT_PERF_LOG"\
            or pname == "PRISM_DAEMON_PERF_LOG" or pname == "GENERIC_SERVER":
            
            logging.info("TLOG_DIRECTORY: %s", self.tlog_dir)
            path = os.path.join(self.tlog_dir)
            fsuffix = ".log"
            
            if last_modified_time:
                self.input_date.append(last_modified_time)    
            else:
                #method call to date range list
                if not pname == "GENERIC_SERVER":
                    self.input_date = self.date_range_list(self.s_date, self.e_date)
                else:
                    s_date = datetime.strptime(self.validation_object.non_converted_start_date, "%Y-%m-%d")
                    e_date = datetime.strptime(self.validation_object.non_converted_end_date, "%Y-%m-%d")
                    self.input_date = self.date_range_list(s_date, e_date)
            
            for date in self.input_date:
                input_date_formatted = ""
                
                if last_modified_time:
                    input_date_formatted = datetime.strftime(datetime.strptime(last_modified_time, "%Y-%m-%d %H:%M:%S"), "%Y%m%d")
                else:
                    input_date_formatted = datetime.strftime(date, "%Y%m%d")
                
                logging.info("input date formated: %s", input_date_formatted)
                dated_tlog_files = ""
                
                try:
                    if pname == "PRISM_TOMCAT":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_BILLING_REALTIME_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
            
                        else:
                            fprefix = "TLOG_BILLING_REALTIME_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                        else:
                            fprefix = "TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_REQUEST_RESPONSE_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                        else:
                            fprefix = "TLOG_REQUEST_RESPONSE_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_TOMCAT_REQ_RESP":
                        fprefix = "TLOG_REQUEST_LOG_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
                        fprefix = "TLOG_CBCK-V2-REQ-RESPONSE_{}_" .format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_TOMCAT_PERF_LOG":
                        fprefix = "TLOG_PERF_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_DEAMON":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_BILLING_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
    
                        else:
                            fprefix = "TLOG_BILLING_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                        else:
                            fprefix = "TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            fprefix = "TLOG_REQUEST_RESPONSE_{0}_{1}_".format(self.validation_object.site_id, input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                        else:
                            fprefix = "TLOG_REQUEST_RESPONSE_{}_".format(input_date_formatted)
                            dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                            
                    elif pname == "PRISM_DAEMON_REQ_RESP":
                        fprefix = "TLOG_REQUEST_LOG_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                        fprefix = "TLOG_CBCK-V2-REQ-RESPONSE_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_DAEMON_PERF_LOG":
                        fprefix = "TLOG_PERF_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                    
                    elif pname == "GENERIC_SERVER":
                        fprefix = "TLOG_GENERIC_SERVER_REQUEST_BEAN_RESPONSE_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                    elif pname == "PRISM_SMSD":
                        fprefix = "TLOG_SMS_{}_".format(input_date_formatted)
                        dated_tlog_files = self.get_sorted_dated_tlog_files(path, fprefix, fsuffix)
                        
                except OSError as error:
                    logging.warning(error)
                
                if bool(dated_tlog_files):
                    for files in dated_tlog_files:
                        self.tlog_files.append(str(files))
                        
                else:
                    if pname == "PRISM_TOMCAT":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_BILLING_REALTIME_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_BILLING_REALTIME_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_REQUEST_RESPONSE_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_TOMCAT_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_TOMCAT_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file not present".format(input_date_formatted))
                    
                    elif pname == "PRISM_DEAMON":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_BILLING_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_BILLING_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
                        if self.validation_object.is_multitenant_system:
                            logging.info("TLOG_REQUEST_RESPONSE_{0}_{1}_*..log file not present".format(self.validation_object.site_id, input_date_formatted))
                        else:
                            logging.info("TLOG_REQUEST_RESPONSE_{}_*..log file not present".format(input_date_formatted))
                            
                    elif pname == "PRISM_DAEMON_REQ_RESP":
                        logging.info("TLOG_REQUEST_LOG_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                        logging.info("TLOG_CBCK-V2-REQ-RESPONSE_{}_*..log file not present".format(input_date_formatted))
                    elif pname == "PRISM_DAEMON_PERF_LOG":
                        logging.info("TLOG_PERF_{}_*..log file not present".format(input_date_formatted))
                    
                    elif pname == "GENERIC_SERVER":
                        logging.info("TLOG_GENERIC_SERVER_REQUEST_BEAN_RESPONSE_{}_*..log file not present".format(input_date_formatted))
                        
                    elif pname == "PRISM_SMSD":
                        logging.info("TLOG_SMS_{}_*..log file not present".format(input_date_formatted))
        
        #current tlog file
        if pname == "PRISM_TOMCAT":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_BILLING_REALTIME_{2}_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_BILLING_REALTIME_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_tlog_path"]))
                
        elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{2}_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_http_handler_req_resp_path"]))
                
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_REQUEST_RESPONSE_{2}_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_REQUEST_RESPONSE_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_generic_soap_handler_req_resp_path"]))
                
        elif pname == "PRISM_TOMCAT_REQ_RESP":
            self.tlog_files.append('{}/TLOG_REQUEST_LOG_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_req_resp_path"]))
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            self.tlog_files.append('{}/TLOG_CBCK-V2-REQ-RESPONSE_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_callbackV2_req_resp_path"]))                        
        elif pname == "PRISM_TOMCAT_PERF_LOG":
            self.tlog_files.append('{}/TLOG_PERF_*.tmp'.format(self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_perf_log_path"]))
            
        elif pname == "PRISM_DEAMON":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_BILLING_{2}_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_BILLING_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_tlog_path"]))
                
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_{2}_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_REQUEST_RESPONSE_GENERIC_HTTP_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_http_handler_req_resp_path"]))
                
        elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            if self.validation_object.is_multitenant_system:
                self.tlog_files.append('{0}_{1}/TLOG_REQUEST_RESPONSE_{2}_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"], self.validation_object.site_id, self.validation_object.site_id))
            else:
                self.tlog_files.append('{}/TLOG_REQUEST_RESPONSE_*.tmp'.format(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_generic_soap_handler_req_resp_path"]))
                
        elif pname == "PRISM_DAEMON_REQ_RESP":
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_req_resp_path"] + "/TLOG_REQUEST_LOG_*.tmp")    
        elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_callbackV2_req_resp_path"] + '/TLOG_CBCK-V2-REQ-RESPONSE_*.tmp')
        elif pname == "PRISM_DAEMON_PERF_LOG":
            self.tlog_files.append(self.initializedPath_object.prism_daemon_log_path_dict["prism_daemon_perf_log_path"] + "/TLOG_PERF_*.tmp")
        
        elif pname == "GENERIC_SERVER":
            self.tlog_files.append(self.initializedPath_object.prism_tomcat_log_path_dict["generic_server_request_bean_response"] + "/TLOG_GENERIC_SERVER_REQUEST_BEAN_RESPONSE_*.tmp")
            
        elif pname == "PRISM_SMSD":
            self.tlog_files.append(self.initializedPath_object.prism_smsd_log_path_dict["prism_smsd_tlog_path"] + '/TLOG_SMS_*.tmp')
            
        logging.info('TLOG_FILES_PRESENT: %s', self.tlog_files)
        
        return self.tlog_files
    
    def get_tomcat_access_files(self, pname):
        #re-initializing constructor parameters
        self.constructor_paramter_reinitialize()
        
        hostname = socket.gethostname()
        access_log_path = ""
        access_log_prefix = ""
        access_log_suffix = ""
        try:
            if pname == "PRISM_TOMCAT" or pname == "GENERIC_SERVER":
                for webService in self.initializedPath_object.web_services:
                    logging.info('web service: %s', webService)
                    access_log_prefix = self.config[hostname]["PRISM"]["PRISM_TOMCAT"][webService]['LOGS_PATH']["ACCESS_LOG_PREFIX"]
                    access_log_suffix = self.config[hostname]["PRISM"]["PRISM_TOMCAT"][webService]['LOGS_PATH']["ACCESS_LOG_SUFFIX"]
                    access_log_path = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]
        
        except KeyError as ex:
            logging.info('key error: %s', ex)
                
        path = os.path.abspath(access_log_path)
        
        #method call to date range list
        if not pname == "GENERIC_SERVER":
            self.input_date = self.date_range_list(self.s_date, self.e_date)
        else:
            s_date = datetime.strptime(self.validation_object.non_converted_start_date, "%Y-%m-%d")
            e_date = datetime.strptime(self.validation_object.non_converted_end_date, "%Y-%m-%d")
            self.input_date = self.date_range_list(s_date, e_date)
            
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

    def get_generated_cdr_files(self, file_prefix, file_datetime_fmt, file_suffix, file_local_directory):
        cdr_files = []
        
        # Define the pattern to match the substring
        pattern = r'\$.*?\$'

        # Replace the matched substring with an empty string
        file_prefix = re.sub(pattern, '*', file_prefix)
        logging.info("FILE_PREFIX: %s", file_prefix)
        
        self.input_date = self.date_range_list(self.start_date, self.end_date)
        
        for input_date in self.input_date:
            formatted_date = self.java_datetime_to_python_convertor(input_date, file_datetime_fmt)
            pattern = '{0}{1}*.{2}'.format(file_prefix, formatted_date, file_suffix)

            # Construct the full file path pattern
            file_pattern = os.path.join(file_local_directory, pattern)
            cdr_files.append(file_pattern)
        
        cdr_files.append('{0}/{1}*.tmp'.format(file_local_directory, file_prefix))
        logging.info("CDR_FILES: %s", cdr_files)
        
        return cdr_files

    def get_sorted_dated_tlog_files(self, path, fname_prefix, fname_suffix):
        return sorted(
                [ os.path.join(path, p) for p in os.listdir(path) 
                 if p.startswith(fname_prefix) and p.endswith(fname_suffix)
                ], key=extract_hour
            )
                    
    def date_range_list(self, start_date, end_date):
        # Return list of datetime.date objects between start_date and end_date (inclusive).
        date_list = []
        curr_date = start_date
        while curr_date <= end_date:
            date_list.append(curr_date)
            curr_date += timedelta(days=1)
        return date_list
        
    def java_datetime_to_python_convertor(self, date_string, java_datetime_fmt):
        # Define a mapping of format specifiers between Java and Python
        java_to_python_mapping = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S'
        }

        # Replace Java format specifiers with Python format specifiers
        python_format = java_datetime_fmt
        for java_specifier, python_specifier in java_to_python_mapping.items():
            python_format = python_format.replace(java_specifier, python_specifier)
        # logging.info('PYTHON_FORMAT: %s', python_format[0:6])

        # Parse the Java datetime string using the Python format
        try:
            parsed_datetime = datetime.strftime(date_string, python_format[0:6])
            # logging.info("PARSED_DATETIME: %s", parsed_datetime)
            return parsed_datetime
        except Exception as ex:
            logging.info(ex)

            

    def constructor_paramter_reinitialize(self):
        self.access_log_files = []
        self.tlog_files = []
        self.input_date = []
        self.tlog_dir = ""