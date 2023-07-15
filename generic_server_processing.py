from collections import OrderedDict
import logging
import os
import signal
import socket
import subprocess
from configManager import ConfigManager
from outfile_writer import FileWriter
from subscriptions_events import SubscriptionEventController
from log_files import LogFileFinder
from tlog_accesslog_parser import TlogAccessLogParser

class GENERIC_SERVER_PROCESSOR:
    def __init__(self, initializedPath_object, outputDirectory_object, prism_data_dict_list,\
                validation_object, config, log_mode, oarm_uid):
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.prism_data_dict_list = prism_data_dict_list
        self.validation_object = validation_object
        self.config = config
        self.hostname = socket.gethostname()
        self.web_services = []
        self.prism_tomcat_conf_path = ""
        self.log_mode = log_mode
        self.oarm_uid = oarm_uid
        self.pname = "GENERIC_SERVER"
        self.global_site_id = -1
        self.site_id = ""
        self.msisdn = ""
        self.payment_status = ""
        self.task_type = ""
        self.timestamp = ""
        self.flow_tasks = ""
        self.sbn_event_id = ""
        self.srv_id = ""
        self.requester_ref_id = ""
        self.charging_ref_id = ""
        self.operator_url = []
        self.last_modified_time = ""
        self.is_sbn_processed = {}
        self.gs_tlog_files = []
        self.is_processed_by_generic_server = False
        self.gs_tlog_record = []
        self.gs_tlog_thread = []
        self.gs_req_resp_record = []
        self.thread_data_dict = OrderedDict()
        self.gs_access_data_dict = OrderedDict()
        self.gs_req_param_file = None
        self.gs_callback_response_file = None
        self.is_macro_required = False
        self.generic_server_files = []
        
    
    def process_generic_server_tlog(self, process_tlog):
        logging.info("GENERIC SERVER LOG PROCESSING STARTED")
        configManager_object = ConfigManager(self.validation_object)
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        tlogAccessLogParser_object = TlogAccessLogParser(self.initializedPath_object, self.outputDirectory_object,\
                                        self.validation_object, self.log_mode, self.oarm_uid,\
                                        None, None, None, None)
        
        try:
            for pthread, ptlog in process_tlog.items():
                self.is_processed_by_generic_server = False
                self.site_id = ptlog["SITE_ID"]
                self.msisdn = ptlog["MSISDN"]
                self.payment_status = ptlog["PAYMENT_STATUS"]
                self.task_type = ptlog["TASK_TYPE"]
                self.timestamp = ptlog["TIMESTAMP"]
                self.flow_tasks = ptlog["FLOW_TASKS"]
                self.sbn_event_id = ptlog["SBN_OR_EVT_ID"]
                
                for task in self.flow_tasks:
                    if "-#PUSH" in task.split(",") or "-#PROXY SRV-PRECHARGED" in task.split(","):
                        self.is_processed_by_generic_server = True
                        break
                
                if self.is_processed_by_generic_server:
                    if self.validation_object.is_multitenant_system:
                        site_id = self.site_id
                        self.operator_url.extend([url for url in configManager_object.get_operator_url_map(self.validation_object.operator_id) if str(url).startswith("/subscription/PrismServer/")])
                    else:
                        site_id = self.global_site_id
                        #actionBased
                        self.operator_url.extend([url for url in configManager_object.get_operator_url_from_pcp("GENERIC_SERVLET", self.global_site_id)])
                        #uriBased
                        self.operator_url.extend([url for url in self.get_uri_based_url_map(configManager_object)])
                        
                    logging.info("OPERATOR_URL: %s", self.operator_url)
                    if self.operator_url:
                        self.fetch_subscriptions_events_data_once(pthread)
                    
                        self.gs_tlog_files = logfile_object.get_tlog_files(self.pname)
                        logging.info("GS_REQUEST_BEAN_RESPONSE_TLOG_FILES: %s", self.gs_tlog_files)
                        
                        if self.gs_tlog_files:
                            self.condition_based_gs_tlog_fetch(site_id)
                            
                        if self.gs_tlog_record:
                            data_list = []
                            for data in self.gs_tlog_record:
                                for record in str(data).splitlines():
                                    if record not in data_list:
                                        self.gs_tlog_thread.append(record.split("|")[1])
                                        data_list.append(record)
                            logging.info("GENERIC_REQUEST_BEAN_RESPONSE_RECORD: %s", data_list)
                            logging.info("GENERIC_REQUEST_BEAN_RESPONSE_THREAD: %s", self.gs_tlog_thread)
                            
                if self.gs_tlog_thread:
                    self.generic_server_access_header_data_map(logfile_object, tlogAccessLogParser_object)
                    self.generic_server_request_response_header_map(configManager_object, tlogAccessLogParser_object)
                    self.get_generic_server_files(configManager_object)
                    logging.info("All the dated REQUEST_BEAN_RESPONSE have been already parsed so breaking the loop")
                    break              
        except KeyError as err:
            logging.info(err)
    
    def fetch_subscriptions_events_data_once(self, pthread):
        value = self.is_sbn_processed.get(self.sbn_event_id)
        if value is not None and value == "processed":
            pass
        else:
            thread_dict = {self.sbn_event_id: pthread}
            subscription_event_object = SubscriptionEventController(None, self.validation_object, thread_dict, True)
            transaction_table_data_dict = subscription_event_object.get_subscription_event("SUBSCRIPTIONS", False)
            if not transaction_table_data_dict:
                transaction_table_data_dict = subscription_event_object.get_subscription_event("EVENTS", False)
                
            if transaction_table_data_dict:
                subscriptionEventRecord = subscription_event_object.get_subscription_event_dict()
                if subscriptionEventRecord:
                    self.is_sbn_processed[self.sbn_event_id] = "processed"
                    # logging.info("SUBS_RECORD: %s", subscriptionRecord)
                    try:
                        self.charging_ref_id = subscriptionEventRecord["charging_ref_id"]
                        self.requester_ref_id = subscriptionEventRecord["requester_ref_id"]
                        self.srv_id = subscriptionEventRecord["srv_id"]
                        self.last_modified_time = subscriptionEventRecord["last_modified_time"]
                    except KeyError as err:
                        logging.info(err)
                        
            logging.info(
                "SITE_ID=%s, MSISDN=%s, PAYMENT_STATUS=%s, TASK_TYPE=%s, "
                "TIMESTAMP=%s, CHARGING_REF_ID=%s, "
                "REQUESTER_REF_ID=%s, SRV_ID=%s, FLOW_TASKS=%s",
                self.site_id, self.msisdn, self.payment_status, self.task_type,
                self.timestamp, self.charging_ref_id,
                self.requester_ref_id, self.srv_id, self.flow_tasks
            )
    
    def get_uri_based_url_map(self, configManager_object):
        if self.validation_object.is_multitenant_system:
            req_param_file_path = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'REQ_PARAM_FILE_PATH')
        else:
            req_param_file_path = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'REQ_PARAM_FILE_PATH')
            
        extract_string = "URI_BASED="
        return self.extract_gs_file(req_param_file_path, extract_string)
        
    def extract_gs_file(self, gs_file_path, extract_string):
        with open(gs_file_path, "r") as file:
            for line in file:
                if line.startswith(extract_string) and not line.startswith("#"):
                    if extract_string == "URI_BASED=" or extract_string == "ACTION_BASED=":
                        value = line[len(extract_string):].split(":")[0].strip()
                    elif extract_string == "FVstatusFilePath":
                        value = line.split(":")[1].strip()
                    elif extract_string == "file.resource.loader.path":
                        value = line.split("=")[1].strip()
                    elif extract_string == "velocimacro.library":
                        value = line.split("=")[1].strip()
                    elif extract_string == "$$RESP_FILE":
                        value = line.split("FILE@")[1].strip()
                        
                    yield value
        
    def condition_based_gs_tlog_fetch(self, site_id):
        for file in self.gs_tlog_files:
            for opUrl in self.operator_url:
                try:
                    data = subprocess.check_output("cat {0} | grep -a '|{1}|' | grep -a {2} | grep -a {3}".format(file, site_id, opUrl, self.msisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    self.gs_tlog_record.append(data)
                except Exception as ex:
                    logging.info(ex)
                
                if not self.gs_tlog_record:
                    try:
                        data = subprocess.check_output("cat {0} | grep -a '|{1}|' | grep -a {2} | grep -a {3}".format(file, site_id, opUrl, self.charging_ref_id), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                        self.gs_tlog_record.append(data)
                    except Exception as ex:
                        logging.info(ex)
    
    def generic_server_access_header_data_map(self, logfile_object, tlogAccessLogParser_object):
        access_files = []
        counter = 0
        if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]:
            access_files = logfile_object.get_tomcat_access_files(self.pname)
            
        if access_files:
            header = [
                        "REMOTE_HOST", "TIMESTAMP", "TIME_ZONE", "THREAD", "SIGNATURE", "METHOD",\
                        "URL", "PROTOCOL", "HTTP_STATUS_CODE", "TIMETAKEN_MILLIS"
                    ]
            for file in access_files:
                access_record = []
                try:
                    for opUrl in self.operator_url:
                        data = subprocess.check_output("cat {0} | grep -a {1}".format(file, opUrl), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                        
                        access_record.append(data)
    
                        for data in access_record:
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
                                    counter += 1
                                    self.gs_access_data_dict["{}_{}".format(data_dict["THREAD"], counter)] = data_dict
                except Exception as ex:
                    logging.info(ex)
            logging.info("ACCESS_COUNTER: %s", counter)
            logging.info("GS_ACCESS_DATA_DICT: %s", self.gs_access_data_dict)
            
            if self.gs_access_data_dict:
                gs_access_dict = {"GENERIC_SERVER_ACCESS_LOG": self.gs_access_data_dict}
                self.prism_data_dict_list.append(gs_access_dict)
            
            if self.log_mode == "error":
                if self.gs_access_data_dict:
                    tlogAccessLogParser_object.parse_access_req_resp_Log(self.pname, self.gs_access_data_dict)
    
    def generic_server_request_response_header_map(self, configManager_object, tlogAccessLogParser_object):
        header = [
                    "TIMESTAMP", "THREAD_ID", "ACTION", "REQUEST", "RESPONSE", "STATUS", "TIME_TAKEN"
                ]
        #check for site specific logger name and field & value separator else -1 site id
        if self.validation_object.is_multitenant_system:
            gs_req_resp_logger_name = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'reqResLoggerName')
        else:
            gs_req_resp_logger_name = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'reqResLoggerName')
            
        if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_{}_log".format(gs_req_resp_logger_name)]:
            gs_req_resp_log_path = self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_{}_log".format(gs_req_resp_logger_name)]
            logging.info("GS_REQ_RESP_LOG_PATH: %s", gs_req_resp_log_path)
            
            try:
                for gs_thread in self.gs_tlog_thread:
                    temp = []
    
                    data_bytes = subprocess.check_output("cat {0} | grep -a {1}".format(gs_req_resp_log_path, gs_thread), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                    data_str = data_bytes.decode("utf-8")
                    self.gs_req_resp_record.append(data_str)
                    
                    if self.gs_req_resp_record:
                        for data in self.gs_req_resp_record:
                            splitted_data = str(data).split("|")
                            if gs_thread == splitted_data[1].strip():
                                data_dict = OrderedDict()
                                try:
                                    for index, element in enumerate(splitted_data):
                                        data_dict[header[index]] = element.strip()
                                    temp.append(data_dict)
                                except IndexError as error:
                                    logging.exception(error)
                        if temp:
                            self.thread_data_dict[gs_thread] = temp
                            
                logging.info('GS_THREAD_DATA_DICT: %s', self.thread_data_dict)
            except Exception as ex:
                logging.info(ex)
        
        if self.thread_data_dict:
            gs_thread_req_resp_dict = {"GENERIC_SERVER_REQUEST_RESPONSE": self.thread_data_dict}
            self.prism_data_dict_list.append(gs_thread_req_resp_dict)
            
            if self.log_mode == "error":
                if self.thread_data_dict:
                    tlogAccessLogParser_object.parse_access_req_resp_Log("GENERIC_SERVER_REQ_RESP", self.thread_data_dict)
    
    def get_generic_server_files(self, configManager_object):
        is_macro_required = None
        velocity_file = None
        
        if self.validation_object.is_multitenant_system:
            self.gs_req_param_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'REQ_PARAM_FILE_PATH')
            self.gs_callback_response_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'CALLBACK_RESPONSE_FILE')        
        else:
            self.gs_req_param_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'REQ_PARAM_FILE_PATH')
            self.gs_callback_response_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'CALLBACK_RESPONSE_FILE')
        
        if self.validation_object.is_multitenant_system:
            is_macro_required = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'IS_MACRO_REQ')
        else:
            is_macro_required = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'IS_MACRO_REQ')
            
        if is_macro_required and self.is_boolean(is_macro_required):
            self.is_macro_required = is_macro_required.lower() == 'true'
            
        if self.is_macro_required:
            if self.validation_object.is_multitenant_system:
                velocity_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.site_id, 'VEL_PROPS_FILE_NAME')
            else:
                velocity_file = configManager_object.get_prism_config_param_value('GENERIC_SERVLET', self.global_site_id, 'VEL_PROPS_FILE_NAME')
        
        if velocity_file:
            self.get_velocity_and_macro_file_path(velocity_file)
        
        if self.gs_req_param_file: 
            self.generic_server_files.append(self.gs_req_param_file)
            self.generic_server_files.extend(set(file for file in self.extract_gs_file(self.gs_req_param_file, "FVstatusFilePath") if file.startswith("/")))
        
        if self.gs_callback_response_file:
            self.generic_server_files.append(self.gs_callback_response_file)
            self.generic_server_files.extend(set(file for file in self.extract_gs_file(self.gs_callback_response_file, "$$RESP_FILE") if file.startswith("/")))
        
        if self.generic_server_files:
            logging.info("GENERIC_SERVER_FILES: %s", self.generic_server_files)
            folder = os.path.join(self.outputDirectory_object, "{}_generic_server_files".format(self.hostname))
            self.create_folder(folder)
            
            fileWriter_object = FileWriter(self.outputDirectory_object, self.oarm_uid)
            fileWriter_object.write_files(self.generic_server_files, folder)
            
    def get_velocity_and_macro_file_path(self, velocity_file):
        try:
            for webService in self.config[self.hostname]["PRISM"]["PRISM_TOMCAT"]:
                self.web_services.append(webService)
                logging.info('tomcat web services: %s', self.web_services)
        
                try:
                    if self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['CONF_PATH'] != "":
                        self.prism_tomcat_conf_path = self.config[self.hostname]['PRISM']['PRISM_TOMCAT'][webService]['CONF_PATH']
                        logging.info("TOMCAT_CONF_PATH: %s", self.prism_tomcat_conf_path)
                        if self.prism_tomcat_conf_path:
                            path = os.path.join(self.prism_tomcat_conf_path, velocity_file)
                            if os.path.exists(path):
                                logging.info("GS_VELOCITY_PROP_FILE: %s", path)
                                self.generic_server_files.append(path)
                                value1 = self.extract_gs_file(path, "file.resource.loader.path")
                                value2 = self.extract_gs_file(path, "velocimacro.library")
                                macro_file = os.path.join(list(value1)[0], list(value2)[0])
                                
                                if os.path.exists(macro_file):
                                    logging.info("GS_MACRO_FILE: %s", macro_file)
                                    self.generic_server_files.append(macro_file)
                            break
                    else:
                        logging.info('%s conf path not available in %s.json file', webService, self.hostname)
                except OSError as error:
                    logging.exception(error)
        except KeyError as error:
            logging.error('Hence %s conf path will not be fetched.', webService)
            logging.exception(error)
        
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
    
    def create_folder(self, folder):
        try:
            if not os.path.exists(folder):
                os.mkdir(folder)
        except os.error as error:
            logging.info(error)
            os.mkdir(folder)
        
        
