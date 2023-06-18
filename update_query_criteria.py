import logging
from configManager import ConfigManager
from status_tags import PrismFlowId

class UpdateQueryCriteria:
    def __init__(self, subscriptionRecord):
        self.subscriptionRecord = subscriptionRecord
        self.forced_subtype_check = None
        self.renewal_subtype_check = None
        self.resume_subtype_flow = None
        self.deact_subtype_check = None
        self.subscriptions_task_type = ""
        self.subscriptions_flow_id = ""
        self.subscriptions_srv_id = ""
        self.subscriptions_pmt_status = ""
        self.charge_type = ""
        self.update_query = ""
    
    def get_nested_dictionaries(self, nested_list):
        dictionaries = []

        for item in nested_list:
            if isinstance(item, dict):
                dictionaries.append(item)
            elif isinstance(item, list):
                dictionaries.extend(self.get_nested_dictionaries(item))

        return dictionaries
        
    def update_query_formatter(self):
        configManager_object = ConfigManager()
        logging.info("HANDLER_PROCESSOR_INFO: %s", configManager_object.handler_processor_info)
        logging.info("HANDLER_PROCESSOR_MAP: %s", configManager_object.handler_processor_map)
        logging.info("SUBTYPE_BOOLEAN_PARAM: %s", configManager_object.subtype_parameter)
        
        try:
            nested_dictionaries = self.get_nested_dictionaries(configManager_object.subtype_parameter)
            if nested_dictionaries:
                for dictionary in nested_dictionaries:
                    if dictionary["PARAM_NAME"] == "FORCED_SUBTYPE_CHECK":
                        if self.is_boolean(dictionary["PARAM_VALUE"]):
                            self.forced_subtype_check = dictionary["PARAM_VALUE"].lower() == 'true'
                    elif dictionary["PARAM_NAME"] == "RENEWAL_SUBTYPE_CHECK":
                        if self.is_boolean(dictionary["PARAM_VALUE"]):
                            self.renewal_subtype_check = dictionary["PARAM_VALUE"].lower() == 'true'
                    elif dictionary["PARAM_NAME"] == "RESUME_SUBTYPE_FLOW":
                        if self.is_boolean(dictionary["PARAM_VALUE"]):
                            self.resume_subtype_flow = dictionary["PARAM_VALUE"].lower() == 'true'
                    elif dictionary["PARAM_NAME"] == "DEACT_SUBTYPE_CHECK":
                        if self.is_boolean(dictionary["PARAM_VALUE"]):
                            self.deact_subtype_check = dictionary["PARAM_VALUE"].lower() == 'true'
                        
            logging.info("FORCED_SUBTYPE_CHECK: %s", self.forced_subtype_check)
            logging.info("RENEWAL_SUBTYPE_CHECK: %s", self.renewal_subtype_check)
            logging.info("RESUME_SUBTYPE_FLOW: %s", self.resume_subtype_flow)
            logging.info("DEACT_SUBTYPE_CHECK: %s", self.deact_subtype_check)

        except KeyError as err:
            logging.info(err)
        
        if self.subscriptionRecord:
            self.subscriptions_flow_id = str(self.subscriptionRecord["system_info"]).split("flowId:")[1].split("|")[0]
            self.subscriptions_srv_id = self.subscriptionRecord["srv_id"]
            self.subscriptions_task_type = self.subscriptionRecord["task_type"]
            self.subscriptions_pmt_status = self.subscriptionRecord["pmt_status"]
            logging.info('SUBSCRIPTIONS_TASK_TYPE: %s', self.subscriptions_task_type)
            
            self.get_charge_type()
            self.set_update_query()
              
    def get_charge_type(self):
        for status_chargeType, status_flowId in PrismFlowId.__dict__.items():
            if not status_chargeType.startswith("__"):
                logging.info("STATUS_FLOW_ID: %s", status_flowId)
                for sflowId in status_flowId:
                    if self.subscriptions_flow_id in sflowId:
                        self.charge_type = status_chargeType
    
    def set_update_query(self):
        if self.subscriptions_task_type in ('CN', 'S', 'L', 'I', 'C'):
            self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now() WHERE sbn_id = %s"
        
        elif self.subscriptions_task_type in ('V', 'Q', 'B', 'R'):
            if self.charge_type == 'A':
                if not self.forced_subtype_check:
                    
                    if self.subscriptions_task_type == 'B':
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'Q', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0, system_info = replace(system_info,'StepChgId:','StepChgId_:') where sbn_id = %s"
                    
                    elif self.subscriptions_task_type == "R":
                        if self.subscriptions_pmt_status != 1:
                            self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type='Q',task_status=0,remote_status=127,cycle_status='R',charge_schedule=now() WHERE sbn_id = %s"
                        else:
                            self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now() where sbn_id = %s"
                    
                    else:
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
                
                else:
                    
                    if self.subscriptions_task_type == 'B':
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0, system_info = replace(system_info,'StepChgId:','StepChgId_:') where sbn_id = %s"
                    
                    elif self.subscriptions_task_type == "R":
                        if self.subscriptions_pmt_status != 1:
                            self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type='Q',task_status=0,remote_status=127,cycle_status='R',charge_schedule=now() WHERE sbn_id = %s"
                        else:
                            self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now() where sbn_id = %s"
                    
                    else:
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s" 
            
            elif self.charge_type == 'R':
                if not self.forced_subtype_check:
                    if self.subscriptions_task_type == 'B':
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'Q', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0, system_info = replace(system_info,'StepChgId:','StepChgId_:') where sbn_id = %s"
                    else:
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
                
                else:
                    if self.subscriptions_task_type == 'B':
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0, system_info = replace(system_info,'StepChgId:','StepChgId_:') where sbn_id = %s"
                    else:
                        self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s" 
                
            elif self.charge_type == 'D':
                if not self.deact_subtype_check:
                    self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
                
                else:
                    self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s" 
        
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
                  
    