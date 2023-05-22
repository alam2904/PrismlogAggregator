import logging
import os
import socket
from process_daemon_log import DaemonLogProcessor
from input_tags import PrismTlogErrorTag, PrismTlogLowBalTag, PrismTlogRetryTag,\
    PrismTlogHandlerExp, PrismTlogNHFTag, PrismTlogAwaitPushTag, PrismTlogAwaitPushTimeOutTag,\
    HttpErrorCodes, PrismTlogSmsTag, PrismTasks

class TlogAccessLogParser:
    """
        Tlog parser class
        for parsing tlog for any issue
    """
    def __init__(self, initializedPath_object, outputDirectory_object, validation_object, log_mode, oarm_uid,\
                    prism_daemon_tlog_thread_dict, prism_tomcat_tlog_thread_dict):
        
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
        
        #prism required parameters
        self.task_types = []
        self.stck_sub_type = ""
        self.input_tags = []
        self.issue_access_threads = []
    
    def parse_tlog(self, pname, tlog_header_data_dict, ctid_map=None):
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
        try:
            if pname == "PRISM_TOMCAT" or pname == "PRISM_DEAMON":
                thread_list = []
                if pname == "PRISM_TOMCAT":
                    thread_list = self.prism_tomcat_tlog_thread_dict["PRISM_TOMCAT_THREAD"]
                elif pname == "PRISM_DEAMON":
                    thread_list = self.prism_daemon_tlog_thread_dict["PRISM_DEAMON_THREAD"]
                for thread in thread_list:
                    #re-initializing self.task_types for each threads
                    self.reinitialize_constructor_parameters()
                    for key, value in dict(tlog_header_data_dict).items():       
                        # logging.info('tlog key: %s value: %s', key, value['THREAD'])
        
                        if self.log_mode == "error" and thread == value['THREAD']:
                            if self.check_for_issue_in_prism_tlog(
                                                                    pname, folder, value,
                                                                    PrismTasks, PrismTlogErrorTag,
                                                                    PrismTlogLowBalTag, PrismTlogRetryTag,
                                                                    PrismTlogNHFTag, PrismTlogHandlerExp,
                                                                    PrismTlogAwaitPushTag, PrismTlogAwaitPushTimeOutTag
                                                                ):
                                if self.stck_sub_type:
                                    daemonLogProcessor_object.process_daemon_log(pname, thread, None, self.task_types, self.stck_sub_type, self.input_tags)
                                else:
                                    logging.info('reached thread: %s', thread)
                                    daemonLogProcessor_object.process_daemon_log(pname, thread, None, self.task_types, tlog_header_data_dict[thread]["SUB_TYPE"], self.input_tags)
            
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
                                    daemonLogProcessor_object.process_daemon_log(pname, sms_tlog["THREAD"], None, None, None, None)
                            
                     
        except KeyError as error:
            logging.exception(error)
    
    def parse_accessLog(self, pname, accesslog_header_data_dict):
        folder = os.path.join(self.outputDirectory_object, "{}_issue_tomcat_access".format(self.hostname))
        for key, value in dict(accesslog_header_data_dict).items():
            logging.info('access value is: %s', value["HTTP_STATUS_CODE"])
            self.check_for_issue_in_accesslog(pname, folder, value, HttpErrorCodes)       
            
    def check_for_issue_in_accesslog(self, pname, folder, access_dict, error_code):
        #issue validation against http error codes
        for error_msg, err_code in error_code.__dict__.items():
            if not error_msg.startswith("__"):
                if err_code == access_dict["HTTP_STATUS_CODE"]:
                    # logging.info("http_code: %s", access_dict["HTTP_STATUS_CODE"])
                    self.issue_access_threads.append(access_dict["THREAD"])
        
        if self.issue_access_threads:
            #issue thread found hence going to create tomcat access folder
            if not self.prism_tomcat_access_out_folder:
                self.create_process_folder(pname, folder)
            return True
        return False
        
    
    def check_for_issue_in_prism_tlog(self, pname, folder, tlog_dict, prism_tasks, *args):
        #issue validation against input_tags
        
        for prism_input_tags in args:
            for var_name, var_value in prism_input_tags.__dict__.items():
                if not var_name.startswith("__"):
            # for status in prism_input_tags:
                    for task in tlog_dict["FLOW_TASKS"]:
                        if var_value in task:
                            if "#PUSH" in task:
                                logging.info('prism flow tasks: %s', task)
                            
                            #substitution parameters
                            self.input_tags.append(var_value)
                            logging.info("input tags: %s", self.input_tags)
                            for ptask_name, ptask_value in prism_tasks.__dict__.items():
                                if not ptask_name.startswith("__"):
                            # for ptask in prism_tasks:
                                    if ptask_name == var_name:
                                        if var_name == "SUB_TYPE_CHECK":
                                            self.stck_sub_type = 'A'
                                        # self.task_type = ptask_value
                                        self.task_types.append(ptask_value)
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
            
    def reinitialize_constructor_parameters(self):
        self.task_types = []
        self.input_tags = []