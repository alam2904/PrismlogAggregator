import logging
import signal
import subprocess
from log_files import LogFileFinder
from collections import defaultdict
from tlog_parser import TlogParser
from collections import OrderedDict


class Tlog:
    """
    tlog mapping class
    for creating tlog data mapping based on ctid, 
    access log data included
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object, log_mode,\
                    prism_data_dict_list, prism_data_dict, config,\
                    prism_ctid, prism_tomcat_tlog_dict, prism_daemon_tlog_dict, prism_daemon_tlog_thread_dict,\
                    prism_tomcat_tlog_thread_dict, prism_tomcat_handler_generic_http_req_resp_dict,\
                    prism_daemon_handler_generic_http_req_resp_dict, prism_tomcat_handler_generic_soap_req_resp_dict,\
                    prism_daemon_handler_generic_soap_req_resp_dict, prism_tomcat_request_log_dict,\
                    prism_daemon_request_log_dict, prism_tomcat_callbackV2_log_dict, prism_daemon_callbackV2_log_dict,\
                    prism_tomcat_perf_log_dict, prism_daemon_perf_log_dict, prism_smsd_tlog_dict, oarm_uid):
        
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
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
        self.ctid_data_dict = defaultdict(list)
        self.msisdn_data_dict = {}
        self.msisdn_access_data_dict = defaultdict(list)
        # self.msisdn_sms_data_dict = defaultdict(list)
        self.msisdn_sms_data_list = []
        
        #header data mapped tlogs
        self.prism_access_log_dict = {}
        
        #log processor initialization parameter
        self.prism_data_dict_list = prism_data_dict_list
        self.prism_data_dict = prism_data_dict
        self.config = config
        
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
        self.is_success_access_hit = True
    
    def get_tlog(self, pname):
        """
        calling path finder method
        """
        
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            self.constructor_parameter_reinitialize()
            self.constructor_ctid_msisdn_paramter_reinitialization()
         
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
            
        # if self.backup_tlog_files:
        #     self.is_backup_file = True
            
        #     for file in self.backup_tlog_files:
        #         #function call
        #         self.msisdn_ctid_map(pname, file, self.is_backup_file)
                
        if pname == "PRISM_TOMCAT":
            if self.initializedPath_object.prism_tomcat_log_path_dict["prism_tomcat_access_path"]:
                logging.debug('prism tomcat access path exists.')
                self.access_files = logfile_object.get_tomcat_access_files(pname)
            
            if self.access_files:
                self.msisdn_based_accesslog_fetch(pname, self.access_files)
        
        logging.info('tlog record: %s', self.tlog_record)
        if self.tlog_record:
            data_list = []
            for data in self.tlog_record:
                logging.info('data in tlog: %s', data)
                for record in str(data).splitlines():
                    if record not in data_list:
                        data_list.append(record)
            
            if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP" or pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP"\
                    or pname == "PRISM_TOMCAT_REQ_RESP" or pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP"\
                    or pname == "PRISM_DAEMON_REQ_RESP" or pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
                    
                    self.prism_handler_req_resp_header_map(pname, data_list)
                    
            elif pname == "PRISM_TOMCAT_PERF_LOG" or pname == "PRISM_DAEMON_PERF_LOG":
                self.perf_data_mapping(pname, data_list)
            elif pname == "PRISM_SMSD":
                self.sms_data_header_mapping(pname, data_list)
            else:
                self.tlog_record_header_mapping(pname, data_list)
        
        if pname == "PRISM_TOMCAT":
            return self.prism_tomcat_tlog_dict
        
        elif pname == "PRISM_DEAMON":
            return self.prism_daemon_tlog_dict
    
    # def msisdn_based_tlog_fetch(self, pname, files, is_backup_files):
    def msisdn_based_tlog_fetch(self, pname, files):
        try:                    
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON" or pname == "PRISM_SMSD":
                # temp_map = self.prism_ctid  //support not available for now
                msisdn = self.validation_object.fmsisdn

                # if is_backup_files:
                #     for file in files:
                #         try:
                #             data = subprocess.check_output("zcat {0} | grep -a {1}".format(file, msisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                #             self.tlog_record.append(data)
                #         except Exception as ex:
                #             logging.info(ex)
                # else:
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
          
    def tlog_record_header_mapping(self, pname, data_list):
        #GRIFF tlog header mapping and call to tlog parser class
        tlogParser_object = TlogParser(self.initializedPath_object, self.outputDirectory_object,\
                                        self.validation_object, self.log_mode, self.oarm_uid,\
                                        self.prism_daemon_tlog_thread_dict, self.prism_tomcat_tlog_thread_dict)
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
        
        elif pname == "PRISM_SMSD":
            header = [
                        "TIMESTAMP","THREAD","SITE_ID","MSISDN","SRNO","SRVCODE","MSG","HANDLER",\
                        "STATUS","REMARKS","TIME_TAKEN","SMS_INFO"
                    ]
        
        logging.info('process name: %s', pname)
           
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            for data in data_list:
                splitted_data = str(data).split("|")
                data_dict = OrderedDict()
                flow_tasks_element = []
                index_count = 28
                # logging.info('splitted data length: %s', len(splitted_data))
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
                    
                    # logging.info('index_: %s', index)
                    # elif index == len(splitted_data):
                
                # if ctid in data_dict["CTID"]: //support not available for now
                self.msisdn_data_dict[data_dict["THREAD"]] = data_dict
        
        
        if pname == "PRISM_TOMCAT":
            logging.info('msisdn data dict: %s', self.msisdn_data_dict)
            for thread, data in self.msisdn_data_dict.items():
                self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"].append(thread)
                logging.info('prism tomcat thread: %s', self.prism_tomcat_tlog_thread_dict)
                
        
        elif pname == "PRISM_DEAMON":
            for thread, data in self.msisdn_data_dict.items():
                self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"].append(thread)
                logging.info('prism daemon thread: %s', self.prism_daemon_tlog_thread_dict)
                    
                        
        if pname == "PRISM_TOMCAT":
            # self.prism_tomcat_tlog_dict = {"PRISM_REALTIME_BILLING_TLOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_tlog_dict = {"PRISM_TOMCAT_BILLING_TLOG": dict(self.msisdn_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_tlog_dict)
            logging.info('prism realtime billing tlogs: %s', str(self.prism_tomcat_tlog_dict).replace("'", '"'))
        
        elif pname == "PRISM_DEAMON":
            self.prism_daemon_tlog_dict = {"PRISM_DAEMON_TLOG": dict(self.msisdn_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_tlog_dict)
            logging.info('prism billing tlogs: %s', str(self.prism_daemon_tlog_dict).replace("'", '"'))
        
        # parse tlog for error
        if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
            if self.msisdn_data_dict:
                tlogParser_object.parse_tlog(pname, self.msisdn_data_dict)
        
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
                        "TIMESTAMP","THREAD","REQTYPE","URL","ENTITY_DATA","PARAMTERS","RESPONSE_CODE",\
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
            for data in data_list:
                splitted_data = str(data).split("|")
                # splitted_data = re.split(r'\|(?=[^|])(?:[^|]*\([^)]+\))*[^|]*', data)
                # splitted_data = re.split(r'\|(?=(?:[^<]*<[^>]*>)*[^>]*$)', data)
                if thread == splitted_data[1].replace('"', '').strip():
                    logging.info('splitted data: %s', splitted_data)
                    
                    data_dict = OrderedDict()
                    
                    try:
                        for index, element in enumerate(splitted_data):
                            
                                data_dict[header[index]] = element.replace('"', '').replace("'", '"').strip().rstrip(":")
                        self.ctid_data_dict[thread].append(data_dict)
                    except IndexError as error:
                        logging.exception(error)
            logging.info('prism gen req resp data dict: %s', self.ctid_data_dict)
    
        if pname == "PRISM_TOMCAT_GENERIC_HTTP_REQ_RESP":
            # self.prism_tomcat_handler_generic_http_req_resp_dict = {"PRISM_TOMCAT_GENERIC_HTTP_HANDLER_REQ_RESP": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_handler_generic_http_req_resp_dict = {"PRISM_TOMCAT_GENERIC_HTTP_HANDLER_REQ_RESP": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_handler_generic_http_req_resp_dict)
            logging.info('prism tomcat generic http req-resp: %s', str(self.prism_tomcat_handler_generic_http_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_GENERIC_SOAP_REQ_RESP":
            # self.prism_tomcat_handler_generic_soap_req_resp_dict = {"PRISM_TOMCAT_GENERIC_SOAP_HANDLER_REQ_RESP": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_handler_generic_soap_req_resp_dict = {"PRISM_TOMCAT_GENERIC_SOAP_HANDLER_REQ_RESP": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_handler_generic_soap_req_resp_dict)
            logging.info('prism tomcat generic soap req-resp: %s', str(self.prism_tomcat_handler_generic_soap_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_REQ_RESP":
            # self.prism_tomcat_request_log_dict = {"PRISM_TOMCAT_REQUEST_LOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_request_log_dict = {"PRISM_TOMCAT_REQUEST_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_request_log_dict)
            logging.info('prism tomcat request log: %s', str(self.prism_tomcat_request_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_TOMCAT_CALLBACK_V2_REQ_RESP":
            # self.prism_tomcat_callbackV2_log_dict = {"PRISM_TOMCAT_CALLBACK_V2_LOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_callbackV2_log_dict = {"PRISM_TOMCAT_CALLBACK_V2_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_callbackV2_log_dict)
            logging.info('prism tomcat callbackV2 log: %s', str(self.prism_tomcat_callbackV2_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_GENERIC_HTTP_REQ_RESP":
            # self.prism_daemon_handler_generic_http_req_resp_dict = {"PRISM_DAEMON_GENERIC_HTTP_HANDLER_REQ_RESP": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_daemon_handler_generic_http_req_resp_dict = {"PRISM_DAEMON_GENERIC_HTTP_HANDLER_REQ_RESP": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_handler_generic_http_req_resp_dict)
            logging.info('prism daemon generic http req-resp: %s', str(self.prism_daemon_handler_generic_http_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_GENERIC_SOAP_REQ_RESP":
            # self.prism_daemon_handler_generic_soap_req_resp_dict = {"PRISM_DAEMON_GENERIC_SOAP_HANDLER_REQ_RESP": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_daemon_handler_generic_soap_req_resp_dict = {"PRISM_DAEMON_GENERIC_SOAP_HANDLER_REQ_RESP": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_handler_generic_soap_req_resp_dict)
            logging.info('prism daemon generic soap req-resp: %s', str(self.prism_daemon_handler_generic_soap_req_resp_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_REQ_RESP":
            # self.prism_daemon_request_log_dict = {"PRISM_DAEMON_REQUEST_LOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_daemon_request_log_dict = {"PRISM_DAEMON_REQUEST_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_request_log_dict)
            logging.info('prism daemon request log: %s', str(self.prism_daemon_request_log_dict).replace("'", '"'))
        
        elif pname == "PRISM_DAEMON_CALLBACK_V2_REQ_RESP":
            # self.prism_daemon_callbackV2_log_dict = {"PRISM_DAEMON_CALLBACK_V2_LOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_daemon_callbackV2_log_dict = {"PRISM_DAEMON_CALLBACK_V2_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_callbackV2_log_dict)
            logging.info('prism daemon generic soap req-resp: %s', str(self.prism_daemon_callbackV2_log_dict).replace("'", '"'))
   
   
    def sms_data_header_mapping(self, pname, data_list):
        #sms tlog header map
        tlogParser_object = TlogParser(self.initializedPath_object, self.outputDirectory_object,\
                                        self.validation_object, self.log_mode, self.oarm_uid,\
                                        self.prism_daemon_tlog_thread_dict, self.prism_tomcat_tlog_thread_dict)
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
            tlogParser_object.parse_tlog(pname, self.prism_smsd_tlog_dict)
            
    def msisdn_based_accesslog_fetch(self, pname, files):
        #keeping prism out of ctid flow
        mdn = ""
        
        if pname == "PRISM_TOMCAT":
            mdn = self.validation_object.fmsisdn

        self.constructor_access_paramter_reinitialize()
        for file in files:
            try:
                data = subprocess.check_output("cat {0} | grep -a {1}".format(file, mdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                self.access_record.append(data)
                
                data_list = []
                if self.access_record:
                    logging.info('%s access record: %s', pname, self.access_record)
                    for data in self.access_record:
                        logging.info('data in access log: %s', data)
                        for record in str(data).splitlines():
                            if record:
                                logging.info('access rec: %s', record)
                                data_list.append(record)
                self.msisdn_access_data_dict["{0}".format(mdn)].append(data_list)
            except Exception as ex:
                logging.info(ex)
                        
        
        if self.msisdn_access_data_dict:
            self.access_data_mapping(pname)

    def access_data_mapping(self, pname):
        
        logging.info('msisdn based access data dict: %s', self.msisdn_access_data_dict)
        
        if pname == "PRISM_TOMCAT":
            self.prism_access_log_dict = {"PRISM_ACCESS_LOG": dict(self.msisdn_access_data_dict)}
            self.prism_data_dict_list.append(self.prism_access_log_dict)
            logging.info('prism access logs: %s', self.prism_access_log_dict)
    
    def perf_data_mapping(self, pname, data_list):
        #perf log mapping
        threads = ""
        
        if pname == "PRISM_TOMCAT_PERF_LOG":
            threads = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
        elif pname == "PRISM_DAEMON_PERF_LOG":
            threads = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
        
        for thread in threads:
            for data in data_list:
                if thread in data:
                    self.ctid_data_dict[thread].append(data)
                
        if pname == "PRISM_TOMCAT_PERF_LOG":
            # self.prism_tomcat_perf_log_dict = {"PRISM_TOMCAT_PERF_LOG": {f"{self.validation_object.fmsisdn}": dict(self.ctid_data_dict)}}
            self.prism_tomcat_perf_log_dict = {"PRISM_TOMCAT_PERF_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_tomcat_perf_log_dict)
            logging.info('prism tomcat perf log: %s', self.prism_tomcat_perf_log_dict)
    
        elif pname == "PRISM_DAEMON_PERF_LOG":
            self.prism_daemon_perf_log_dict = {"PRISM_DAEMON_PERF_LOG": dict(self.ctid_data_dict)}
            self.prism_data_dict_list.append(self.prism_daemon_perf_log_dict)
            logging.info('prism daemon perf log: %s', self.prism_daemon_perf_log_dict)
    
    def msisdn_based_sms_tlog_fetch(self, pname, files):
        for file in files:
            try:
                data = subprocess.check_output("cat {0} | grep -a {1}".format(file, self.validation_object.fmsisdn), shell=True, preexec_fn=lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))
                self.tlog_record.append(data)
            except Exception as ex:
                logging.info(ex)

                            
    def constructor_parameter_reinitialize(self):
        self.tlog_files = []
        self.backup_tlog_files = []
        self.access_files = []
        self.tlog_record = []
        self.access_record = []
        self.is_backup_file = False
        self.tlog_files_with_ctid_msisdn = []
        # self.tlog_dict = defaultdict()
        self.ctid_data_dict = defaultdict(list)
        
    def constructor_ctid_msisdn_paramter_reinitialization(self):
        self.ctid_msisdn_map_dict = {}
        
    def constructor_access_paramter_reinitialize(self):
        self.access_record = []
            