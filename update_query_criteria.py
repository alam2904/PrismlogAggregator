import logging
from configManager import ConfigManager
from status_tags import PrismFlowId
import xml.etree.ElementTree as ET

class UpdateQueryCriteria:
    def __init__(self, validation_object, subscriptionRecord):
        self.validation_object = validation_object
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
        self.generic_flow_handler_files = []
        self.next_task_type = ""
    
    def get_nested_dictionaries(self, nested_list):
        dictionaries = []

        for item in nested_list:
            if isinstance(item, dict):
                dictionaries.append(item)
            elif isinstance(item, list):
                dictionaries.extend(self.get_nested_dictionaries(item))

        return dictionaries
        
    def update_query_formatter(self):
        configManager_object = ConfigManager(self.validation_object)
        configManager_object.initialize_subtype_parameter()
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
            self.set_update_query(configManager_object)
              
    def get_charge_type(self):
        for status_chargeType, status_flowId in PrismFlowId.__dict__.items():
            if not status_chargeType.startswith("__"):
                for sflowId in status_flowId:
                    if self.subscriptions_flow_id == sflowId:
                        self.charge_type = status_chargeType
                        break
    
    def set_update_query(self, configManager_object):
        #check for generic flow handler for retry next task type
        logging.info("NEXT_TASK_TYPE: %s", self.next_task_type)
        self.get_next_task_type(configManager_object)
        
        if self.next_task_type:
            if self.subscriptions_pmt_status != 1:
                self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = %s, task_status = 0, remote_status = 127, pmt_status = 0, cycle_status = 'R', charge_schedule = now() WHERE sbn_id = %s"
            else:
                self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = %s, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now() WHERE sbn_id = %s"
                
        elif self.subscriptions_task_type in ('CN', 'S', 'L', 'I', 'C'):
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
                
        elif self.subscriptions_task_type in ('D', 'H'):
            if self.subscriptions_task_type == 'D':
                if not self.deact_subtype_check:
                    self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
                else:
                    self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S', task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s" 
            else:
                self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
    
    def get_next_task_type(self, configManager_object):
        #generic flow handler case
        flow_handler_params = []
        
        logging.info("FLOW_ID: %s AND CHARGE_TYPE: %s", self.subscriptions_flow_id, self.charge_type)
        flow_handler_map = configManager_object.flow_handler_mapping()
        if flow_handler_map:
            for data in flow_handler_map:
                if self.charge_type == data["transaction_type"]:
                    flow_handler_params.append(data["params"])
                    
        if flow_handler_params:
            try:
                for param in flow_handler_params:
                    root = ET.fromstring(param)
                    if root.get('flowConfigFile') != None:
                        self.generic_flow_handler_files.append((root.get('flowConfigFile')))
            except ET.ParseError as ex:
                logging.debug(ex)
        
        if self.generic_flow_handler_files:
            # logging.info("XML_FILES: %s", self.generic_flow_handler_files)
            for xml_file in self.generic_flow_handler_files:
                tree = ET.parse(xml_file)
                root = tree.getroot()
                
                if self.subscriptions_pmt_status != 1:
                    # Find the initialTask element
                    initial_task_element = root.find('initialTask')

                    # Get the value of the value attribute
                    self.next_task_type = initial_task_element.get('value')
                else:
                    retry_element = root.find(".//retry")
                    task_case_elements = retry_element.findall("taskCase")

                    # Find the nextTask value based on the current task
                    next_task = None
                    
                    for task_case_element in task_case_elements:
                        task_value = task_case_element.get("currentTask")
                        if task_value == self.subscriptions_task_type:
                            next_task = task_case_element.get("nextTask")
                            self.next_task_type = next_task

                if self.next_task_type:
                    break
                    
        logging.info("CURRENT_TASK_TYPE: %s AND NEXT_TASK_TYPE: %s", self.subscriptions_task_type, self.next_task_type)
        
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
                  
    