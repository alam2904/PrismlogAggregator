import logging
import os
import socket
from process_daemon_log import DaemonLogProcessor
from status_tags import PrismTlogIssueTag, HttpErrorCodes, GsErrorCodes, PrismTlogSmsTag, PrismTasks, PrismGeneralIssueTage
from subscriptions_events import SubscriptionEventController

class TlogAccessLogParser:
    """
        Tlog parser class
        for parsing tlog for any issue
    """
    def __init__(self, config, initializedPath_object, outputDirectory_object, validation_object, log_mode, oarm_uid,\
                    prism_daemon_tlog_thread_dict, prism_tomcat_tlog_thread_dict, issue_task_types,\
                    sbn_thread_dict, non_issue_sbn_thread_dict):
        
        self.config = config
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.validation_object = validation_object
        self.log_mode = log_mode
        self.oarm_uid = oarm_uid
        self.prism_daemon_tlog_thread_dict = prism_daemon_tlog_thread_dict
        self.prism_tomcat_tlog_thread_dict = prism_tomcat_tlog_thread_dict
        
        self.hostname = socket.gethostname()
        
        #out folder parameters
        self.prism_tomcat_out_folder = False
        self.prism_daemon_out_folder = False
        self.prism_smsd_out_folder = False
        self.prism_tomcat_access_out_folder = False
        self.prism_gs_access_out_folder = False
        self.prism_gs_req_resp_out_folder = False
        
        #prism required parameters
        self.issue_task_types = issue_task_types
        self.task_types = []
        self.stck_sub_type = ""
        self.input_tags = []
        self.issue_access_req_resp_threads = []
        self.is_daemon_log = False
        self.sbn_thread_dict = sbn_thread_dict
        self.non_issue_sbn_thread_dict = non_issue_sbn_thread_dict
        self.process_subs_data = True
        self.subscriptions_data = None
    
    def parse_tlog(self, pname, tlog_header_data_dict, ctid_map=None, reprocessed_thread=None):
        """
            tlog parser method
        """
        self.reinitialize_constructor_parameters()
        folder = ""
        
        if pname == "PRISM_TOMCAT":
            folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_tomcat".format(self.hostname))
        elif pname == "PRISM_DEAMON":
            folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_daemon".format(self.hostname))
        elif pname == "PRISM_SMSD":
            folder = os.path.join(self.outputDirectory_object, "{}_issue_prism_smsd".format(self.hostname))
        
        #Daemon log processor object
        daemonLogProcessor_object = DaemonLogProcessor(self.initializedPath_object, self.outputDirectory_object,\
                                                        self.validation_object, self.oarm_uid)
        #processing tlog based on different key in the tlog
        latest_thread = "" 
        try:
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                if not reprocessed_thread:
                    thread_list = []
                    if pname == "PRISM_TOMCAT":
                        thread_list = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
                    elif pname == "PRISM_DEAMON":
                        thread_list = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
                else:
                    thread_list = reprocessed_thread
                    
                for thread in thread_list:
                    #re-initializing self.task_types for each threads
                    self.reinitialize_constructor_parameters()
                    for key, value in dict(tlog_header_data_dict).items():
                        # logging.info('tlog key: %s value: %s', key, value['THREAD'])
        
                        if self.log_mode == "error" and thread == value['THREAD']:
                            if self.check_for_issue_in_prism_tlog(
                                                                    pname, folder, value,
                                                                    PrismTasks, PrismTlogIssueTag
                                                                ):
                                
                                if self.stck_sub_type:
                                    latest_thread = thread
                                    self.is_daemon_log = daemonLogProcessor_object.process_daemon_log_init(pname, thread, None, self.task_types, self.stck_sub_type, self.input_tags)
                                    
                                    self.is_query_reprocessing_required(self.is_daemon_log, value)
                                else:
                                    latest_thread = thread
                                    logging.info('reached thread: %s', latest_thread)
                                    self.is_daemon_log = daemonLogProcessor_object.process_daemon_log_init(pname, thread, None, self.task_types, tlog_header_data_dict[thread]["SUB_TYPE"], self.input_tags)
                                    
                                    self.is_query_reprocessing_required(self.is_daemon_log, value)
                
                if self.sbn_thread_dict and self.validation_object.is_sub_reprocess_required:    
                    logging.info('IS_SUB_REPROCESS_REQUIRED: %s', self.validation_object.is_sub_reprocess_required)
                    logging.info('SBN-THREAD DICT: %s', self.sbn_thread_dict)
                    
                    subscription_object = SubscriptionEventController(self.config, pname, self.validation_object, self.sbn_thread_dict, self.process_subs_data)
                    self.subscriptions_data = subscription_object.get_subscription_event("SUBSCRIPTIONS", self.validation_object.is_sub_reprocess_required)
                    self.validation_object.is_sub_reprocess_required = False
                
            elif pname == "PRISM_SMSD":
                if self.log_mode == "error":
                    for sms_tlog in tlog_header_data_dict["PRISM_SMSD_TLOG"]:
                        # logging.info('sms tlog list: %s', sms_tlog)
                        for var_name, var_value in PrismTlogSmsTag.__dict__.items():
                            if not var_name.startswith("__"):
                                if var_value == sms_tlog["STATUS"]:
                        # for status in PrismTlogSmsTag:
                            # if status.value == sms_tlog["STATUS"]:
                                    if not self.prism_smsd_out_folder:
                                        self.create_process_folder(pname, folder)
                                    daemonLogProcessor_object.process_daemon_log_init(pname, sms_tlog["THREAD"], None, None, None, None)
            return self.subscriptions_data        
        except KeyError as error:
            logging.exception(error)
    
    def parse_access_req_resp_Log(self, pname, header_data_dict):
        folder = ""
        
        if pname == "GENERIC_SERVER":
            folder = os.path.join(self.outputDirectory_object, "{}_issue_generic_server_access".format(self.hostname))
        elif pname == "GENERIC_SERVER_REQ_RESP":
            folder = os.path.join(self.outputDirectory_object, "{}_issue_generic_server_log".format(self.hostname))
        else:
            folder = os.path.join(self.outputDirectory_object, "{}_issue_tomcat_access".format(self.hostname))
        
        self.reinitialize_constructor_parameters()
        
        for key, value in dict(header_data_dict).items():
            self.check_for_issue_in_access_req_resp_log(pname, folder, value, HttpErrorCodes, GsErrorCodes)
            
            if self.issue_access_req_resp_threads:
                #Daemon log processor object
                daemonLogProcessor_object = DaemonLogProcessor(self.initializedPath_object, self.outputDirectory_object,\
                                                                self.validation_object, self.oarm_uid)
                if not pname == "GENERIC_SERVER_REQ_RESP":
                    daemonLogProcessor_object.process_tomcat_http_req_resp_log(pname, folder, value, self.issue_access_req_resp_threads)
                else:
                    daemonLogProcessor_object.process_tomcat_http_req_resp_log("GENERIC_SERVER_REQ_RESP", folder, value, self.issue_access_req_resp_threads)
                    daemonLogProcessor_object.process_tomcat_http_req_resp_log("GENERIC_SERVER_REQ_RESP_GS", folder, value, self.issue_access_req_resp_threads)
                    
    def check_for_issue_in_access_req_resp_log(self, pname, folder, data_dict, error_code, Gs_ErrorCodes):
        self.issue_access_req_resp_threads = []
        #issue validation against http error codes
        
        if not pname == "GENERIC_SERVER_REQ_RESP":
            for error_msg, err_code in error_code.__dict__.items():
                try:
                    if not error_msg.startswith("__"):
                        if err_code == data_dict["HTTP_STATUS_CODE"]:
                            if not pname == "GENERIC_SERVER":
                                self.issue_access_req_resp_threads.append(data_dict["THREAD"])
                            elif pname == "GENERIC_SERVER":
                                logging.info("ISSUE_STATUS_THREAD: %s", str(data_dict["THREAD"]).split("_")[0])
                                self.issue_access_req_resp_threads.append(str(data_dict["THREAD"]).split("_")[0])
                except KeyError as err:
                    logging.info(err)
        else:
            for gs_error_msg, gs_err_code in Gs_ErrorCodes.__dict__.items():
                try:
                    if not gs_error_msg.startswith("__"):
                        for item in data_dict:
                            logging.info("GS_ERR_CODE: %s AND DATA_DICT_STATUS: %s", gs_err_code, item["STATUS"])
                            if gs_err_code == item["STATUS"]:
                                self.issue_access_req_resp_threads.append(item["THREAD_ID"])
                                break
                        if self.issue_access_req_resp_threads:
                            break
                except KeyError as err:
                    logging.info(err)
            else:
                for error_msg, err_code in error_code.__dict__.items():
                    try:
                        if not error_msg.startswith("__"):
                            for item in data_dict:
                                if err_code == item["STATUS"]:
                                    self.issue_access_req_resp_threads.append(item["THREAD_ID"])
                                    break
                            break
                    except KeyError as err:
                        logging.info(err)
                
                    
        if self.issue_access_req_resp_threads:
            #issue thread found hence going to create tomcat access folder
            if not pname == "GENERIC_SERVER":
                if not self.prism_tomcat_access_out_folder:
                    self.create_process_folder(pname, folder)
                return True
            elif pname == "GENERIC_SERVER":
                if not self.prism_gs_access_out_folder:
                    self.create_process_folder(pname, folder)
                return True
            elif pname == "GENERIC_SERVER_REQ_RESP":
                if not self.prism_gs_req_resp_out_folder:
                    self.create_process_folder(pname, folder)
                return True
                
        return False
        
    
    def check_for_issue_in_prism_tlog(self, pname, folder, tlog_dict, prism_tasks, prism_status_tag):
        #issue validation against input_tags
        for var_name, var_value in prism_status_tag.__dict__.items():
            if not var_name.startswith("__"):
                for task in tlog_dict["FLOW_TASKS"]:
                    for status in var_value:
                        # logging.info('STATUS_TAG: %s', task)
                        if status in task:
                            if var_name == "GENERAL_FAILURE":
                                equals_index = task.find("[")
                                comma_index = task.find("=")
                                gtask_type = task[equals_index + 1:comma_index]
                                
                                logging.info('GENERAL_TASK_TYPE: %s', gtask_type)
                                for gvar_name, gvar_value in PrismGeneralIssueTage.__dict__.items():
                                    if not gvar_name.startswith("__"):
                                        if gtask_type == gvar_value:
                                            var_name = gvar_name
                                            status = gvar_value

                            if "#PUSH" in task:
                                logging.info('prism flow tasks: %s', task)
                            
                            if var_name == "SUB_TYPE_CHECK":
                                sub_type = 'A'
                            else:
                                sub_type = tlog_dict["SUB_TYPE"]
                            
                            charge_type = tlog_dict["CHARGE_TYPE"]
                            
                            #list of tasks for which handler details will be fetched later
                            if [var_name, status, sub_type, charge_type] not in self.issue_task_types:
                                task_name_type = [var_name, status, sub_type, charge_type]
                                self.issue_task_types.append(task_name_type)
                                logging.info("TASK_TYPES: %s", self.issue_task_types)
                            
                            #substitution parameters
                            self.input_tags.append(status)
                            for ptask_name, ptask_value in prism_tasks.__dict__.items():
                                if not ptask_name.startswith("__"):
                            # for ptask in prism_tasks:
                                    if ptask_name == var_name:
                                        if var_name == "SUB_TYPE_CHECK":
                                            self.stck_sub_type = 'A'
                                        # self.task_type = ptask_value
                                        self.task_types.append(ptask_value)
                            break
        if self.task_types:
            #issue thread found hence going to create prism process folder for the 1st time
            if pname == "PRISM_TOMCAT":
                if not self.prism_tomcat_out_folder:
                    self.create_process_folder(pname, folder)
            elif pname == "PRISM_DEAMON":
                if not self.prism_daemon_out_folder:
                    self.create_process_folder(pname, folder)
            logging.info('task types: %s', self.task_types)
            return True
        return False
    
    def is_query_reprocessing_required(self, is_daemon_log, tlog_dict):
        #check if daemon log returned True/False and accordingly maintain the sbn-thread dict
        if is_daemon_log:
            try:
                for key, value in self.sbn_thread_dict.items(): 
                    logging.info('tlog sbn id: %s and map sbn id: %s', tlog_dict["SBN_OR_EVT_ID"], key)
                    if tlog_dict["SBN_OR_EVT_ID"] == key:
                        self.sbn_thread_dict.pop(tlog_dict["SBN_OR_EVT_ID"])
                else:
                    logging.info("sbn not present in map")
            except KeyError as error:
                logging.info("sbn not present in map")
        else:
            self.sbn_thread_dict[tlog_dict["SBN_OR_EVT_ID"]] = tlog_dict["THREAD"]
        
        self.non_issue_sbn_thread_dict = self.sbn_thread_dict
        
                               
    def create_process_folder(self, pname, folder):
        """
            creating process folder
        """
        try:
            # folder.mkdir(parents=True, exist_ok=False)
            if not os.path.exists(folder):
                os.mkdir(folder)
            self.set_process_out_folder(pname, True)
        except os.error as error:
            logging.info(error)
            os.mkdir(folder)
            # folder.mkdir(parents=True)
            
            self.set_process_out_folder(pname, True)
    
    def set_process_out_folder(self, pname, is_true):
        if pname == "PRISM_TOMCAT":
            self.prism_tomcat_out_folder = is_true
        elif pname == "PRISM_TOMCAT_ACCESS":
            self.prism_tomcat_access_out_folder = is_true
        elif pname == "PRISM_DEAMON":
            self.prism_daemon_out_folder = is_true
        elif pname == "PRISM_SMSD":
            self.prism_smsd_out_folder = is_true
        elif pname == "GENERIC_SERVER":
            self.prism_gs_access_out_folder = is_true
        elif pname == "GENERIC_SERVER_REQ_RESP":
            self.prism_gs_req_resp_out_folder = is_true

    def reinitialize_constructor_parameters(self):
        self.task_types = []
        self.input_tags = []
        self.issue_access_req_resp_threads = []
        self.process_subs_data = True