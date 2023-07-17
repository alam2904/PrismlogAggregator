from datetime import datetime, timedelta
import logging
from re import sub
import signal
import subprocess
import time
from log_files import LogFileFinder
from collections import defaultdict
from tlog_accesslog_parser import TlogAccessLogParser
from collections import OrderedDict
from configManager import ConfigManager
from status_tags import PrismTasks
from subscriptions_events import SubscriptionEventController

class Tlog:
    """
    tlog mapping class
    for creating tlog data mapping, 
    access log data included, cdr data included
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object, log_mode,\
                    prism_data_dict_list, prism_data_dict, config,\
                    prism_ctid, prism_tomcat_tlog_dict, prism_daemon_tlog_dict, prism_daemon_tlog_thread_dict,\
                    prism_tomcat_tlog_thread_dict, prism_tomcat_handler_generic_http_req_resp_dict,\
                    prism_daemon_handler_generic_http_req_resp_dict, prism_tomcat_handler_generic_soap_req_resp_dict,\
                    prism_daemon_handler_generic_soap_req_resp_dict, prism_tomcat_request_log_dict,\
                    prism_daemon_request_log_dict, prism_tomcat_callbackV2_log_dict, prism_daemon_callbackV2_log_dict,\
                    prism_tomcat_perf_log_dict, prism_daemon_perf_log_dict, combined_perf_data, prism_handler_info_dict,\
                    issue_task_types, issue_handler_task_type_map, prism_smsd_tlog_dict, non_issue_sbn_thread_dict, oarm_uid):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        
        self.start_date = validation_object.start_date
        self.end_date = validation_object.end_date
        
        self.s_date = datetime.strptime(datetime.strftime(self.start_date, "%Y%m%d"), "%Y%m%d")
        self.e_date = datetime.strptime(datetime.strftime(self.end_date, "%Y%m%d"), "%Y%m%d")
        logging.info("tlog sdate: %s and edate: %s", self.start_date, self.e_date)

        self.log_mode = log_mode
        self.tlog_files = []
        self.backup_tlog_files = []
        self.access_files = []
        
        #ctid msisdn mapping for griff and packs
        self.ctid_msisdn_map_dict = {}
        # self.ctid_map_dict = {}
        
        self.tlog_record = []
        self.access_record = []
        self.is_backup_file = False
        
        #ctid containing files list
        self.tlog_files_with_ctid_msisdn = []
        
        #tlog header mapping parameters
        self.tlog_dict = defaultdict(list)
        # self.thread_data_dict = defaultdict(list)
        self.thread_data_dict = OrderedDict()
        self.msisdn_data_dict = OrderedDict()
        self.msisdn_access_data_dict = OrderedDict()
        self.msisdn_sms_data_list = []
        
        #header data mapped tlogs
        self.prism_access_log_dict = {}
        
        #log processor initialization parameter
        self.prism_data_dict_list = prism_data_dict_list
        self.prism_data_dict = prism_data_dict
        self.config = config
        self.issue_task_types = issue_task_types
        self.issue_handler_task_type_map = issue_handler_task_type_map
        
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
        self.combined_perf_data = combined_perf_data
        self.prism_handler_info_dict = prism_handler_info_dict
        
        self.prism_smsd_tlog_dict = prism_smsd_tlog_dict
        self.oarm_uid = oarm_uid
        self.is_success_access_hit = True
        
        #subscription reprocessing parameters
        self.is_record_reprocessed = False
        self.subscriptions_data = None
        self.reprocessed_tlog_record = []
        self.reprocessed_thread = []
        self.sbn_thread_dict = {}
        self.non_issue_sbn_thread_dict = non_issue_sbn_thread_dict
        
    
    def get_tlog(self, pname):
        """
        calling path finder method
        """
        
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        
        tlogAccessLogParser_object = TlogAccessLogParser(self.initializedPath_object, self.outputDirectory_object,\
                                        self.validation_object, self.log_mode, self.oarm_uid,\
                                        self.prism_daemon_tlog_thread_dict, self.prism_tomcat_tlog_thread_dict,\
                                        self.issue_task_types, self.sbn_thread_dict)
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            self.constructor_parameter_reinitialize()
            self.constructor_ctid_msisdn_paramter_reinitialization()
            self.reprocessed_constructor_parameter_reinitialize()
         
        elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
            or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP"\
            or pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG"\
            or pname == "PRISM_SMSD":
            
            self.constructor_parameter_reinitialize()
        
        self.tlog_files = logfile_object.get_tlog_files(pname)
        
        if self.tlog_files:                    
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                # function call
                self.msisdn_based_tlog_fetch(pname, self.tlog_files)
            
            elif pname == "PRISM_SMSD":
                # function call
                self.msisdn_based_sms_tlog_fetch(pname, self.tlog_files)
                
            
            elif pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG":
                # function call
                self.thread_based_prism_handler_req_resp_fetch(pname, self.tlog_files)
                
        if pname == "PRISM_TOMCAT":
            if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]:
                logging.debug('prism tomcat access path exists.')
                self.access_files = logfile_object.get_tomcat_access_files(pname)
            
            if self.access_files:
                self.msisdn_based_accesslog_fetch(tlogAccessLogParser_object, pname, self.access_files)
        
        logging.info('tlog record: %s', self.tlog_record)
        if self.tlog_record:
            data_list = []
            for data in self.tlog_record:
                logging.info('data in tlog: %s', data)
                for record in str(data).splitlines():
                    if record not in data_list:
                        data_list.append(record)
            # logging.info('data list: %s', data_list)
            
            if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                    
                    self.prism_handler_req_resp_header_map(pname, data_list)
                    
            elif pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG":
                self.perf_data_mapping(tlogAccessLogParser_object, pname, data_list)
            elif pname == "PRISM_SMSD":
                self.sms_data_header_mapping(tlogAccessLogParser_object, pname, data_list)
            else:
                self.tlog_record_header_mapping(tlogAccessLogParser_object, pname, data_list)
        
        if pname == "PRISM_TOMCAT":
            return self.prism_tomcat_tlog_dict
        
        elif pname == "PRISM_DEAMON":
            return self.prism_daemon_tlog_dict
    
    def msisdn_based_tlog_fetch(self, pname, files):
        try:                    
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
                # temp_map = self.prism_ctid  //support not available for now
                msisdn = self.validation_object.fmsisdn

                for file in files:
                    try:
                        data = subprocess.check_output("cat {0} | grep -a {1}".format(file, msisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                        self.tlog_record.append(data)
                    except Exception as ex:
                        logging.info(ex)
        except Exception as ex:
            logging.info(ex)
    
    def thread_based_prism_handler_req_resp_fetch(self, pname, files):
        threads = ""
        
        if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
            or pname == "PRISM_TOMCAT_PERF_LOG":
                
            threads = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
            
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP"\
            or pname == "PRISM_DAEMON_PERF_LOG":
                
            threads = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
        
        for thread in threads:
            try:
                for file in files:
                    try:
                        data = subprocess.check_output("cat {0} | grep -a {1}".format(file, thread), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                        self.tlog_record.append(data)
                    except Exception as ex:
                        logging.info(ex)
            except Exception as ex:
                logging.info(ex)
          
    def tlog_record_header_mapping(self, tlogAccessLogParser_object, pname, data_list, last_modified_time=None):
        #GRIFF tlog header mapping and call to tlog parser class
        header = []
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
        
            header = [
                        "TIMESTAMP","THREAD","SITE_ID","MSISDN","SUB_TYPE","SBN_OR_EVT_ID","SRV_KEYWORD",\
                        "CHARGE_TYPE","PARENT_KEYWORD","AMOUNT","MODE","USER","REQUEST_DATE","INVOICE_DATE",\
                        "EXPIRY_DATE","RETRY_COUNT","CYCLE_STATUS","GRACE_COUNT","GRACE_RETRY_COUNT",\
                        "NEW_SRV_KEYWORD","INFER_SUB_STATUS","CHARGE_KEYWORD","TRIGGER_ID","PACK_KEY",\
                        "PARENT_ID","APP_NAME","SITE_NAME","REN_COUNT","FLOW_TASKS","CONTENT_ID","CAMPAIGN_ID",\
                        "TOTAL_CHG_AMNT","RECO","TASK_TYPE","TASK_STATUS","PAYMENT_STATUS","CHARGE_SCHEDULE",\
                        "NEXT_BILL_DATE"
                    ]
        
        logging.info('process name: %s', pname)
        reprocessed_thread = []
           
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            for data in data_list:
                splitted_data = str(data).split("|")
                data_dict = OrderedDict()
                flow_tasks_element = []
                index_count = 28
                if last_modified_time:
                    reprocessed_thread = self.reprocessed_thread
                    self.reprocessed_constructor_parameter_reinitialize()
                    input_date_formatted = datetime.strftime(datetime.strptime(last_modified_time, "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d")
                    logging.info("last modified input date formated: %s", input_date_formatted)
                    self.tlog_map(header, splitted_data, data_dict, flow_tasks_element, index_count)
                else:
                    #method call to date range list
                    self.input_date = self.date_range_list(self.s_date, self.e_date)
            
                    for date in self.input_date:
                        input_date_formatted = datetime.strftime(date, "%Y%m%d")
                            
                        if input_date_formatted == datetime.strftime(datetime.strptime(splitted_data[0].split(" ")[0], "%Y-%m-%d"), "%Y%m%d"):
                            self.tlog_map(header, splitted_data, data_dict, flow_tasks_element, index_count)
                        else:
                            logging.info("tlog timestamp: %s did not match with input date: %s", datetime.strftime(datetime.strptime(splitted_data[0].split(" ")[0], "%Y-%m-%d"), "%Y%m%d"), input_date_formatted)

        if pname == "PRISM_TOMCAT":
            logging.info('msisdn data dict: %s', self.msisdn_data_dict)
            for thread, data in self.msisdn_data_dict.items():
                self.sbn_thread_map(dict(data))
                self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"].append(thread)
            logging.info('prism tomcat thread: %s', self.prism_tomcat_tlog_thread_dict)
                
        
        elif pname == "PRISM_DEAMON":
            if last_modified_time:
                for thread in reprocessed_thread:
                    self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"].append(thread)
            else:
                for thread, data in self.msisdn_data_dict.items():
                    self.sbn_thread_map(dict(data))
                    self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"].append(thread)
            logging.info('prism daemon thread: %s', self.prism_daemon_tlog_thread_dict)
                    
                        
        if pname == "PRISM_TOMCAT":
            self.prism_tomcat_tlog_dict = {"PRISM_TOMCAT_BILLING_TLOG": self.msisdn_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_tlog_dict)
            logging.info('prism realtime billing tlogs: %s', str(self.prism_tomcat_tlog_dict).replace("'", '"'))
        
        elif pname == "PRISM_DEAMON":
            if last_modified_time:
                for tlog_dict in self.prism_data_dict_list:
                    if "PRISM_DAEMON_TLOG" in tlog_dict:
                        tlog_dict["PRISM_DAEMON_TLOG"] = self.msisdn_data_dict
            else:
                self.prism_daemon_tlog_dict = {"PRISM_DAEMON_TLOG": self.msisdn_data_dict}
                self.prism_data_dict_list.append(self.prism_daemon_tlog_dict)
            logging.info('prism billing tlogs: %s', str(self.prism_daemon_tlog_dict).replace("'", '"'))
        
        # parse tlog for error
        if self.log_mode == "error":
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                if self.msisdn_data_dict:
                    logging.info("reached here: %s", self.is_record_reprocessed)
                    if not self.is_record_reprocessed:
                        self.subscriptions_data = tlogAccessLogParser_object.parse_tlog(pname, self.msisdn_data_dict, None, reprocessed_thread)
                    
                    if not tlogAccessLogParser_object.is_daemon_log and self.subscriptions_data:
                        self.is_record_reprocessed = True
                        logging.info('daemon log not present')
                        time.sleep(15)
                        if pname == "PRISM_DEAMON":
                            self.get_reprocessed_tlog(pname, tlogAccessLogParser_object)
    
    def tlog_map(self, header, splitted_data, data_dict, flow_tasks_element, index_count):
        for index, element in enumerate(splitted_data):                  
            if index <= 27:
                data_dict[header[index]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
            
            elif index <= len(splitted_data) - 6:
                flow_tasks_element.append(element.replace('"', '').replace("'", '"').strip())
                data_dict[header[index_count]] = flow_tasks_element
                
            elif index <= len(splitted_data) - 2:
                index_count += 1
                data_dict[header[index_count]] = element.replace('"', '').replace("'", '"').strip()
            
            elif index == len(splitted_data) - 1:
                # logging.info('index: %s', index)
                try:
                    for i, td in enumerate(element.split("=")[1].split("]")[0].split(",")):
                        index_count += 1
                        data_dict[header[index_count]] = td.replace('"', '').replace("'", '"').strip()
                    # data_dict[header[len(header) - 1]] = f"{ctid}"
                except IndexError as error:
                    logging.exception(error)

        # if ctid in data_dict["CTID"]: //support not available for now
        self.msisdn_data_dict[data_dict["THREAD"]] = data_dict
        
    def get_reprocessed_tlog(self, pname, tlogAccessLogParser_object):
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            logging.info('subscriptions data is: %s', self.subscriptions_data)
            
            for subscriptions in self.subscriptions_data:
                for subscription in subscriptions:
                    last_modified_time = subscription["last_modified_time"]
                    logging.info('last modified time: %s', last_modified_time)
                    
                    if self.validation_object.is_multitenant_system:
                        last_modified_time = str(self.validation_object.time_zone_conversion(last_modified_time))
                        logging.info("CONVERTED_LAST_MODIFIED_TIME: %s", last_modified_time)
                    
                    reprocessed_tlog_files = logfile_object.get_tlog_files(pname, last_modified_time)
                
                    logging.info('reprocessed tlog files: %s', reprocessed_tlog_files)
                    
                    self.lastModifiedTime_based_tlog_fetch(pname, reprocessed_tlog_files, last_modified_time)
            
            if self.reprocessed_tlog_record:
                data_list = []
                for data in self.reprocessed_tlog_record:
                    logging.info('data in latest tlog: %s', data)
                    for record in str(data).splitlines():
                        if record not in data_list:
                            data_list.append(record)
                self.tlog_record_header_mapping(tlogAccessLogParser_object, pname, data_list, last_modified_time)
                # logging.info('msisdn_data_dict: %s', self.msisdn_data_dict)
    
    def lastModifiedTime_based_tlog_fetch(self, pname, files, last_modified_time):
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            # temp_map = self.prism_ctid  //support not available for now
            msisdn = self.validation_object.fmsisdn
            temp = []
            for file in files:
                # logging.info('file: %s', file)
                try:
                    data = subprocess.check_output("cat {0} | grep -a {1}".format(file, msisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    temp = data.splitlines()  # Split the data into individual lines/records
                    for record in temp:
                        if str(record).split("|")[0].split(",")[0] >= str(last_modified_time):
                            self.reprocessed_thread.append(str(record).split("|")[1])
                            logging.info('latest tlog timestamp: %s and thread: %s', str(record).split("|")[0].split(",")[0], self.reprocessed_thread)
                            self.reprocessed_tlog_record.append(record)
                except Exception as ex:
                    logging.info(ex)
            logging.info("latest tlog record: %s", self.reprocessed_tlog_record)
        
    def prism_handler_req_resp_header_map(self, pname, data_list):
        # prism tomcat and daemon handler request response mapping
        header = []
        threads = ""
        
        if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            
            header = [
                        "TIMESTAMP","THREAD_ID","TASK_TYPE","URL","REQUEST","RESPONSE","TIME_TAKEN"
                    ]
        
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            
            header = [
                        "TIMESTAMP","THREAD_ID","TASK_TYPE","URL","REQUEST_XML","RESPONSE_XML","TIME_TAKEN"
                    ]
        elif pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_DAEMON_REQ_RESP":
            
            header = [
                        "TIMESTAMP","THREAD","REQTYPE","URL","HEADERS","ENTITY_DATA","PARAMTERS","RESPONSE_CODE",\
                        "RESPONSE_TIME(sec)","RESPONSE"
                    ]
            
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            
            header = [
                        "TIMESTAMP","THREAD","URL","RESPONSE","CODE","RETRY_COUNT"
                    ]
        
        if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            
            threads = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
            
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
            or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                
            threads = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
            
        for thread in threads:
            temp = []
            for data in data_list:
                splitted_data = str(data).split("|")
                if thread == splitted_data[1].replace('"', '').strip():
                    logging.info('splitted data: %s', splitted_data)
                    
                    data_dict = OrderedDict()
                    try:
                        for index, element in enumerate(splitted_data):
                                data_dict[header[index]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
                        temp.append(data_dict)
                    except IndexError as error:
                        logging.exception(error)
            if temp:
                self.thread_data_dict[thread] = temp
                logging.info('thread data dict: %s', self.thread_data_dict)
    
        if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
            self.prism_tomcat_handler_generic_http_req_resp_dict = {"PRISM_TOMCAT_GENERIC_HTTP_HANDLER_REQ_RESP": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_handler_generic_http_req_resp_dict)
            logging.info('prism tomcat generic http req-resp: %s', str(self.prism_tomcat_handler_generic_http_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
            self.prism_tomcat_handler_generic_soap_req_resp_dict = {"PRISM_TOMCAT_GENERIC_SOAP_HANDLER_REQ_RESP": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_handler_generic_soap_req_resp_dict)
            logging.info('prism tomcat generic soap req-resp: %s', str(self.prism_tomcat_handler_generic_soap_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_REQ_RESP":
            self.prism_tomcat_request_log_dict = {"PRISM_TOMCAT_REQUEST_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_request_log_dict)
            logging.info('prism tomcat request log: %s', str(self.prism_tomcat_request_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            self.prism_tomcat_callbackV2_log_dict = {"PRISM_TOMCAT_CALLBACK_V2_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_callbackV2_log_dict)
            logging.info('prism tomcat callbackV2 log: %s', str(self.prism_tomcat_callbackV2_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            self.prism_daemon_handler_generic_http_req_resp_dict = {"PRISM_DAEMON_GENERIC_HTTP_HANDLER_REQ_RESP": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_daemon_handler_generic_http_req_resp_dict)
            logging.info('prism daemon generic http req-resp: %s', str(self.prism_daemon_handler_generic_http_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            self.prism_daemon_handler_generic_soap_req_resp_dict = {"PRISM_DAEMON_GENERIC_SOAP_HANDLER_REQ_RESP": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_daemon_handler_generic_soap_req_resp_dict)
            logging.info('prism daemon generic soap req-resp: %s', str(self.prism_daemon_handler_generic_soap_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_REQ_RESP":
            self.prism_daemon_request_log_dict = {"PRISM_DAEMON_REQUEST_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_daemon_request_log_dict)
            logging.info('prism daemon request log: %s', str(self.prism_daemon_request_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            self.prism_daemon_callbackV2_log_dict = {"PRISM_DAEMON_CALLBACK_V2_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_daemon_callbackV2_log_dict)
            logging.info('prism daemon generic callbackV2 req-resp: %s', str(self.prism_daemon_callbackV2_log_dict).replace("'", '"'))
   
    def sms_data_header_mapping(self, tlogAccessLogParser_object, pname, data_list):
        #sms tlog header map
        # tlogParser_object = TlogAccessLogParser(self.initializedPath_object, self.outputDirectory_object,\
                                        # self.validation_object, self.log_mode, self.oarm_uid,\
                                        # self.prism_daemon_tlog_thread_dict, self.prism_tomcat_tlog_thread_dict)
        if pname == "PRISM_SMSD":
            
            header = [
                        "TIMESTAMP","THREAD","SITE_ID","MSISDN","SRNO","SRVCODE","MSG","HANDLER","STATUS","REMARKS","TIME_TAKEN","SMS_INFO"
                    ]
     
            for data in data_list:
                splitted_data = str(data).split("|")
                # splitted_data = re.split(r'\|(?=[^|])(?:[^|]*\([^)]+\))*[^|]*', data)
                # splitted_data = re.split(r'\|(?=(?:[^<]*<[^>]*>)*[^>]*$)', data)
                
                logging.info('splitted data: %s', splitted_data)
                
                data_dict = OrderedDict()
                
                try:
                    for index, element in enumerate(splitted_data):
                        
                            data_dict[header[index]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
                    # self.msisdn_sms_data_dict[f"{self.validation_object.fmsisdn}"].append(data_dict)
                    if data_dict not in self.msisdn_sms_data_list:
                        self.msisdn_sms_data_list.append(data_dict)
                except IndexError as error:
                    logging.exception(error)
            logging.info('sms tlog data dict: %s', self.msisdn_sms_data_list)
    
        if pname == "PRISM_SMSD":
            # self.prism_smsd_tlog_dict = {"PRISM_SMSD_TLOG": dict(self.msisdn_sms_data_dict)}
            self.prism_smsd_tlog_dict = {"PRISM_SMSD_TLOG": self.msisdn_sms_data_list}
            self.prism_data_dict_list.append(self.prism_smsd_tlog_dict)
            logging.info('prism sms tlog: %s', str(self.prism_smsd_tlog_dict).replace("'", '"'))
        
        # parse tlog for error
        if self.prism_smsd_tlog_dict:
            tlogAccessLogParser_object.parse_tlog(pname, self.prism_smsd_tlog_dict)
            
    def msisdn_based_accesslog_fetch(self, tlogAccessLogParser_object, pname, files):
        #keeping prism out of ctid flow
        mdn = ""
        
        header = [
                    "REMOTE_HOST", "TIMESTAMP", "TIME_ZONE", "THREAD", "SIGNATURE", "METHOD",\
                    "URL", "PROTOCOL", "HTTP_STATUS_CODE", "TIMETAKEN_MILLIS"
                ]
        
        if pname == "PRISM_TOMCAT":
            mdn = self.validation_object.fmsisdn

        for file in files:
            self.constructor_access_paramter_reinitialize()
            try:
                
                data = subprocess.check_output("cat {0} | grep -a {1}".format(file, mdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                self.access_record.append(data)
                
                if self.access_record:
                    logging.info('%s, %s, access record: %s', pname, file, self.access_record)
                    for data in self.access_record:
                        for record in str(data).splitlines():
                            if record:
                                data_dict = OrderedDict()
                                access_data_split = record.split(' ')
                    
                                for index, element in enumerate(access_data_split):
                                    # logging.info("index: %s and element: %s", index, element)
                                    if index == 1:
                                        element = element.split("[")[1]
                                    elif index == 2:
                                        element = element.split("]")[0]
            
                                    data_dict[header[index]] = element
                                
                                #IS_MULTITENANT_SYSTEM
                                if self.validation_object.is_multitenant_system:
                                    if self.url_operator_id_check(data_dict["URL"]):
                                        self.msisdn_access_data_dict[data_dict["THREAD"]] = data_dict
                                else:
                                    self.msisdn_access_data_dict[data_dict["THREAD"]] = data_dict
            except Exception as ex:
                logging.info(ex)
                        
        
        if self.msisdn_access_data_dict:
            self.access_data_mapping(tlogAccessLogParser_object, pname)
    
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
        

    def access_data_mapping(self, tlogAccessLogParser_object, pname):
        logging.info('msisdn based access data dict: %s', self.msisdn_access_data_dict)
        
        if pname == "PRISM_TOMCAT":
            self.prism_access_log_dict = {"PRISM_ACCESS_LOG": dict(self.msisdn_access_data_dict)}
            self.prism_data_dict_list.append(self.prism_access_log_dict)
            logging.info('prism access logs: %s', self.prism_access_log_dict)
        
        if self.log_mode == "error":
            if self.msisdn_access_data_dict:
                tlogAccessLogParser_object.parse_access_req_resp_Log(pname, self.msisdn_access_data_dict)
    
    def perf_data_mapping(self, tlogAccessLogParser_object, pname, data_list):
        #perf header log mapping
        header = [
                    "TIMESTAMP", "THREAD", "SITE_NAME", "SBNID", "MSISDN", "SRVKEY", "QUEUEI_D", "TRANSACTION_TYPE",\
                    "DELAY", "AMOUNT", "MESSAGE", "PERF_TASK", "TOTAL_TIME"
                ]
        threads = ""
        
        if pname == "PRISM_TOMCAT_PERF_LOG":
            threads = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
        elif pname == "PRISM_DAEMON_PERF_LOG":
            threads = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
        
        try:
            for thread in threads:
                for data in data_list:
                    splitted_data = str(data).split("|")
                    data_dict = OrderedDict()
                    flow_tasks_element = []
                    index_count = 11
                # for data in data_list:
                    if thread in data:
                        self.perf_map(header, thread, splitted_data, data_dict, flow_tasks_element, index_count)
                        # self.thread_data_dict[thread] = data
        except IndexError as e:
            logging.info('exception occured while perf header data mapping. Hence logging perf data without header')
            self.thread_data_dict[thread] = data
        
        self.combined_perf_data.append(self.thread_data_dict)
        
        if pname == "PRISM_TOMCAT_PERF_LOG":
            self.prism_tomcat_perf_log_dict = {"PRISM_TOMCAT_PERF_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_tomcat_perf_log_dict)
            logging.info('prism tomcat perf log: %s', self.prism_tomcat_perf_log_dict)
    
        elif pname == "PRISM_DAEMON_PERF_LOG":
            self.prism_daemon_perf_log_dict = {"PRISM_DAEMON_PERF_LOG": self.thread_data_dict}
            self.prism_data_dict_list.append(self.prism_daemon_perf_log_dict)
            logging.info('prism daemon perf log: %s', self.prism_daemon_perf_log_dict)
    
    def get_subscription_event_details(self):
        #fetch subscriptions
        logging.info("NON_ISSUE_SBN_THREAD_DICT: %s", self.non_issue_sbn_thread_dict)
        if self.non_issue_sbn_thread_dict:
            subscription_event_data = []
            subscription_event_object = SubscriptionEventController(None, self.validation_object, self.non_issue_sbn_thread_dict, True)
            try:
                subscriptions_data_dict = subscription_event_object.get_subscription_event("SUBSCRIPTIONS", False)
                if subscriptions_data_dict:
                    subscription_event_data.extend(subscriptions_data_dict)
                    prism_subscriptions_dict = {"PRISM_SUBSCRIPTIONS_ENTRY": subscriptions_data_dict}
                    self.prism_data_dict_list.append(prism_subscriptions_dict)
                else:
                    logging.info("Subscriptions record could not be found")
            except Exception as ex:
                logging.info(ex)
            
            try:
                events_data_dict = subscription_event_object.get_subscription_event("EVENTS", False)
                if events_data_dict:
                    subscription_event_data.extend(events_data_dict)
                    prism_events_dict = {"PRISM_EVENTS_ENTRY": events_data_dict}
                    self.prism_data_dict_list.append(prism_events_dict)
                else:
                    logging.info("Events record could not be found")
                
            except Exception as ex:
                logging.info(ex)
        
        logging.info("SUBSCRIPTIONS_EVENTS_DATA: %s", subscription_event_data)
        return subscription_event_data
        
    def get_issue_handler_details(self, subscription_event_data):
        #handler info and map    
        try:
            logging.info('combined perf data: %s', self.combined_perf_data)
            
            flow_id = []
            srv_id = []
            
            if subscription_event_data:
                for tRecords in subscription_event_data:
                    for record in tRecords:
                        flow_id.append(str(record["system_info"]).split("flowId:")[1].split("|")[0])
                        srv_id.append(record["srv_id"])
                        logging.info('srv_id: %s and sys_info flow_id: %s', srv_id, flow_id)
            
            if self.issue_task_types:
                self.task_perf_handler_mapping(tuple(srv_id), tuple(flow_id))
        except KeyError as error:
            logging.info(error)
        
        if self.issue_handler_task_type_map:
            handler_info_details = {}
            handler_map_details = {}
            handler_details = []
            
            configManager_object = ConfigManager(self.validation_object)
            configManager_object.get_handler_info(self.issue_handler_task_type_map)
            if configManager_object.handler_info:
                logging.info('handler info details: %s', configManager_object.handler_info)
                handler_info_details["HANDLER_INFO"] = configManager_object.handler_info
                handler_details.append(handler_info_details)
            
            configManager_object.get_handler_map(self.issue_handler_task_type_map)
            if configManager_object.handler_map:
                logging.info('handler map details: %s', configManager_object.handler_map)
                handler_map_details["HANDLER_MAP"] = configManager_object.handler_map
                handler_details.append(handler_map_details)
            
            if handler_details:
                self.prism_handler_info_dict = {"PRISM_ISSUE_HANDLER_DETAILS": handler_details}
                self.prism_data_dict_list.append(self.prism_handler_info_dict)
    
                return handler_info_details
        return None
    
    def processing_cdr_file(self):
        configManager_object = ConfigManager(self.validation_object)
        file_info = configManager_object.get_file_info()
        cdrs = []
        
        if file_info:
            logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
            for item in file_info:
                try:
                    cdr_dated_file_pattern = logfile_object.get_generated_cdr_files(
                                    item["FILE_PREFIX"], item["FILE_DATETIME_FMT"],
                                    item["FILE_SUFFIX"], item["FILE_LOCAL_DIR"]
                                )
                    if cdr_dated_file_pattern:
                        for dated_file_pattern in cdr_dated_file_pattern:
                            try:
                                output = subprocess.check_output("grep -a {0} {1}".format(self.validation_object.fmsisdn, dated_file_pattern), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                                cdrs.append(output.splitlines())
                            except subprocess.CalledProcessError as e:
                                logging.info("Error: %s", e)
                except KeyError as err:
                    logging.info(err)
        if cdrs:
            cdr_data = {}
            cdr_data["CDR_DATA"] = [record for data in cdrs for record in data]
            logging.info("CDRS: %s", cdr_data)
            self.prism_data_dict_list.append(cdr_data)
                
    
    def task_perf_handler_mapping(self, srv_id, flow_id):
        for perf_data in self.combined_perf_data: 
            for key, value in perf_data.items():
                for item in self.issue_task_types:
                    task_name, task_value, sub_type, charge_type = item
                    task = str(task_value).replace("=", ",")
                    # logging.info('perf task list: %s', value["PERF_TASK"])
                    for ptask in value["PERF_TASK"]:
                        if task in ptask:
                            handler_id = str(ptask).split(task)[1].split(",")[0]
                            
                            task_type = ""
                            for ptask_name, ptask_value in PrismTasks.__dict__.items():
                                if not ptask_name.startswith("__"):
                                    if ptask_name == task_name:
                                        task_type = ptask_value
                            
                            # if not flow_id:
                            #     for ch_type, f_id in PrismFlowId.__dict__.items():
                            #         if charge_type == str(ch_type):
                            #             flow_id = f_id
                            
                            #task type and handler id mapping
                            task_handler_map = (task_type, handler_id, sub_type, srv_id, flow_id)
                            
                            if task_handler_map not in self.issue_handler_task_type_map:
                                self.issue_handler_task_type_map.append(task_handler_map)
            logging.info('issue_handler_task_type_map: %s', self.issue_handler_task_type_map)
            
    def perf_map(self, header, thread, splitted_data, data_dict, flow_tasks_element, index_count):
        logging.info('length of perf splitted data: %s', len(splitted_data))
        try:
            for index, element in enumerate(splitted_data):                  
                if index <= 10:
                    data_dict[header[index]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
                
                elif index <= len(splitted_data) - 2:
                    flow_tasks_element.append(element.replace('"', '').replace("'", '"').strip())
                    data_dict[header[index_count]] = flow_tasks_element
                    
                elif index == len(splitted_data) - 1:
                    # logging.info('index: %s', index)
                    index_count = index_count + 1
                    data_dict[header[index_count]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
            self.thread_data_dict[thread] = data_dict
        except IndexError as error:
            raise IndexError(error)
            
    
    def msisdn_based_sms_tlog_fetch(self, pname, files):
        for file in files:
            try:
                data = subprocess.check_output("cat {0} | grep -a {1}".format(file, self.validation_object.fmsisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                self.tlog_record.append(data)
            except Exception as ex:
                logging.info(ex)
                
    def sbn_thread_map(self, tlog_dict):
        try:
            for key, value in self.non_issue_sbn_thread_dict.items(): 
                logging.info('non_issues:- tlog sbn id: %s and map sbn id: %s', tlog_dict["SBN_OR_EVT_ID"], key)
                if tlog_dict["SBN_OR_EVT_ID"] == key:
                    self.non_issue_sbn_thread_dict.pop(tlog_dict["SBN_OR_EVT_ID"])
            else:
                self.non_issue_sbn_thread_dict[tlog_dict["SBN_OR_EVT_ID"]] = tlog_dict["THREAD"]
        except KeyError as error:
            logging.info("non_issue:- sbn not present in map")
 
    def constructor_parameter_reinitialize(self):
        self.tlog_files = []
        self.backup_tlog_files = []
        self.access_files = []
        self.tlog_record = []
        self.access_record = []
        self.is_backup_file = False
        self.tlog_files_with_ctid_msisdn = []
        # self.tlog_dict = defaultdict()
        self.thread_data_dict = OrderedDict()
    
    def reprocessed_constructor_parameter_reinitialize(self):
        self.reprocessed_tlog_record = []
        self.reprocessed_thread = []
        self.is_record_reprocessed = False
        self.subscriptions_data = None
            
    def date_range_list(self, start_date, end_date):
        # Return list of datetime.date objects between start_date and end_date (inclusive).
        date_list = []
        curr_date = start_date
        while curr_date <= end_date:
            date_list.append(curr_date)
            curr_date += timedelta(days=1)
        return date_list
    
    def constructor_ctid_msisdn_paramter_reinitialization(self):
        self.ctid_msisdn_map_dict = {}
        
    def constructor_access_paramter_reinitialize(self):
        self.access_record = []
            