
import logging
from configManager import ConfigManager
from subscriptions import SubscriptionController
from log_files import LogFileFinder

class GENERIC_SERVER_PROCESSOR:
    def __init__(self, initializedPath_object, outputDirectory_object, prism_data_dict_list, validation_object, config):
        self.initializedPath_object = initializedPath_object
        self.outputDirectory_object = outputDirectory_object
        self.prism_data_dict_list = prism_data_dict_list
        self.validation_object = validation_object
        self.config = config
        self.global_site_id = -1
        self.site_id = ""
        self.msisdn = ""
        self.payment_status = ""
        self.task_type = ""
        self.timestamp = ""
        self.sbn_id = ""
        self.srv_id = ""
        self.requester_ref_id = ""
        self.internal_ref_id = ""
        self.charging_ref_id = ""
        self.operator_url = ""
        self.last_modified_time = ""
        self.is_sbn_processed = {}
        self.gs_tlog_files = []
        
    
    def process_generic_server_tlog(self, process_tlog):
        logging.info("GENERIC SERVER LOG PROCESSING STARTED")
        configManager_object = ConfigManager(self.validation_object)
        logfile_object = LogFileFinder(self.initializedPath_object, self.validation_object, self.config)
        
        try:
            if self.validation_object.is_multitenant_system:
                self.operator_url = configManager_object.get_operator_url_map(self.validation_object.operator_id)
            else:
                self.operator_url = configManager_object.get_operator_url_from_pcp("GENERIC_SERVLET", self.global_site_id)
            
            logging.info("OPERATOR_URL: %s", self.operator_url)
            
            self.gs_tlog_files = logfile_object.get_tlog_files("GENERIC_SERVER")
            logging.info("GS_REQUEST_BEAN_RESPONSE_TLOG_FILES: %s", self.gs_tlog_files)
            
            for pthread, ptlog in process_tlog.items():
                self.site_id = ptlog["SITE_ID"]
                self.msisdn = ptlog["MSISDN"]
                self.payment_status = ptlog["PAYMENT_STATUS"]
                self.task_type = ptlog["TASK_TYPE"]
                self.timestamp = ptlog["TIMESTAMP"]
                self.sbn_id = ptlog["SBN_OR_EVT_ID"]
                value = self.is_sbn_processed.get(self.sbn_id)
                if value is not None and value == "processed":
                    pass
                else:
                    thread_dict = {self.sbn_id: pthread}
                    subscription_object = SubscriptionController(None, self.validation_object, thread_dict, True)
                    subscriptions_data_dict = subscription_object.get_subscription(False)
                    if subscriptions_data_dict:
                        subscriptionRecord = subscription_object.get_subscription_dict()
                        if subscriptionRecord:
                            self.is_sbn_processed[self.sbn_id] = "processed"
                            # logging.info("SUBS_RECORD: %s", subscriptionRecord)
                            self.charging_ref_id = subscriptionRecord["charging_ref_id"]
                            self.internal_ref_id = subscriptionRecord["internal_ref_id"]
                            self.requester_ref_id = subscriptionRecord["requester_ref_id"]
                            self.srv_id = subscriptionRecord["srv_id"]
                            self.last_modified_time = subscriptionRecord["last_modified_time"]
                            
                logging.info(
                    "SITE_ID=%s, MSISDN=%s, PAYMENT_STATUS=%s, TASK_TYPE=%s, "
                    "TIMESTAMP=%s, CHARGING_REF_ID=%s, INTERNAL_REF_ID=%s, "
                    "REQUESTER_REF_ID=%s, SRV_ID=%s",
                    self.site_id, self.msisdn, self.payment_status, self.task_type,
                    self.timestamp, self.charging_ref_id, self.internal_ref_id,
                    self.requester_ref_id, self.srv_id
                )
        except KeyError as err:
            logging.info(err)