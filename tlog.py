from datetime import datetime
import logging
import signal
import subprocess
from configManager import ConfigManager
from log_files import LogFileFinder
from collections import defaultdict

class Tlog:
    """
    tlog mapping class
    for creating tlog data mapping, 
    access log data included, cdr data included
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object,\
                    prism_data_dict_list, config
                ):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.prism_data_dict_list = prism_data_dict_list
        self.prism_data_dict = defaultdict(list)
        self.config = config
        self.global_site_id = -1
        self.thread_list = []
        self.is_tlog_processed = False
        self.access_data = []
    
    def get_tlog(self, pname):
        """
        calling path finder method
        """
        
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        
        if pname == "PRISM_TOMCAT":
            if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]:
                logging.debug('prism tomcat access path exists.')
                access_files = logfile_object.get_tomcat_access_files(pname)
            
            if access_files:
                self.accesslog_fetch(access_files)
                
        tlog_files = logfile_object.get_tlog_files(pname)
        
        if tlog_files:                    
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
                self.tlog_fetch(pname, tlog_files)
            elif pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG":
                self.perf_log_map(tlog_files)
        
        return self.is_tlog_processed
    
    def accesslog_fetch(self, files):
        msisdn = self.validation_object.fmsisdn
        operator_urls = self.get_operator_url()
        
        for file in files:
            self.msisdn_opurl_based_extract_access_data(file, msisdn)
        
        for url in operator_urls:
            for file in files:
                self.msisdn_opurl_based_extract_access_data(file, url)
        
        if self.access_data:
            self.prism_data_dict["TOMCAT_ACCESS_LOG"].append(self.access_data)
            self.prism_data_dict_list["PRISM_DATA"].append(self.prism_data_dict)
    
    def msisdn_opurl_based_extract_access_data(self, file, fetch_value):
        try:
                records = subprocess.check_output("cat {0} | grep -a {1}".format(file, fetch_value), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                if records:
                    logging.info("RECORDS: %s", records)
                    data_list = records.splitlines()
                    if data_list:
                        for data in data_list:
                            access_timestamp = datetime.strptime(data.split(']')[0].split("[")[1].split(" ")[0], "%d/%b/%Y:%H:%M:%S")
                            if self.is_between_timestamp_logs(access_timestamp):
                                self.access_data.append(data)
                else:
                    logging.info("No access record found in %s", file)
        except Exception as ex:
            logging.info(ex)
    
    def tlog_fetch(self, pname, files):
        tlog_data = []
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
            # temp_map = self.prism_ctid  //support not available for now
            msisdn = self.validation_object.fmsisdn
            for file in files:
                try:
                    records = subprocess.check_output("cat {0} | grep -a {1}".format(file, msisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    if records:
                        data_list = records.splitlines()
                        for data in data_list:
                            splitted_data = str(data).split("|")
                            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                                thread = str(data).split("|")[1]
                            logging.info("THREAD: %s AND TIMESTAMP: %s", thread, splitted_data[0].split(",")[0])
                            log_timestamp = datetime.strptime(splitted_data[0].split(",")[0], "%Y-%m-%d %H:%M:%S")
                            
                            if self.is_between_timestamp_logs(log_timestamp):
                                if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                                    self.thread_list.append(thread)
                                tlog_data.append(data)
                    else:
                        logging.info("No tlog data found in %s", file)
                except Exception as ex:
                    logging.info(ex)
            
            if tlog_data:
                if pname == "PRISM_TOMCAT":
                    self.prism_data_dict["PRISM_TOMCAT_LOG"].append(tlog_data)
                    self.is_tlog_processed = True
                elif pname == "PRISM_DEAMON":
                    self.prism_data_dict["PRISM_DEAMON_LOG"].append(tlog_data)
                    self.is_tlog_processed = True
                elif pname == "PRISM_SMSD":
                    self.prism_data_dict["PRISM_SMSD_LOG"].append(tlog_data)
                    self.is_tlog_processed = True
                    
                self.prism_data_dict_list["PRISM_DATA"].append(self.prism_data_dict)
            
    def is_between_timestamp_logs(self, log_timestamp):
        if self.validation_object.is_multitenant_system:
            start_timestamp = self.validation_object.start_date
            end_timestamp = self.validation_object.end_date
        else:
            start_timestamp = self.validation_object.non_converted_start_date
            end_timestamp = self.validation_object.non_converted_end_date
            
        if log_timestamp >= start_timestamp and log_timestamp <= end_timestamp:
            return True
        else:
            return False
    
    def url_operator_id_check(self, prism_url):
        #check for operator id in prism URL
        url_pre_param, url_param = str(prism_url).split("?")
        url_pre_param_splitted = url_pre_param.split("/")[-1]
        if str(url_pre_param_splitted).startswith("opid_"):
            operator_id = str(url_pre_param_splitted).split("_")[1]
            # logging.info("URL_OPERATOR_ID: %s", operator_id)
            if operator_id == self.validation_object.operator_id:
                return True
        else:
            url_param_splitted = url_param.split("&")
            for param in url_param_splitted:
                key, value = param.split("=")
                # logging.info("URL_OPERATOR_ID: %s", value)
                if key == "operatorId" and value == self.validation_object.operator_id:
                    return True
            else:
                return False 
        
    
    def perf_log_map(self, files):
        perf_data = []
        
        for thread in self.thread_list:
            for file in files:
                try:
                    records = subprocess.check_output("cat {0} | grep -a {1}".format(file, thread), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    if records:
                        data_list = [str(data).splitlines() for data in records]
                        for data in data_list:
                            perf_data.append(data)
                    
                    if not perf_data:
                        logging.info("No perf log data found in %s", file)
                except Exception as ex:
                    logging.info(ex)
    
    def get_operator_url(self):
        configManager_object = ConfigManager(self.validation_object)
        operator_url = []
        try:

            if self.validation_object.is_multitenant_system:
                operator_url.extend([url for url in configManager_object.get_operator_url_map(self.validation_object.operator_id) if str(url).startswith("/subscription/PrismServer/")])
            else:
                #actionBased
                operator_url.extend([url for url in configManager_object.get_operator_url_from_pcp("GENERIC_SERVLET", self.global_site_id)])
                #uriBased
                operator_url.extend([url for url in self.get_uri_based_url_map(configManager_object)])
            
            return operator_url
        except Exception as ex:
            logging.debug(ex)
    
            