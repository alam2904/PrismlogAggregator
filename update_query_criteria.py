import logging
from configManager import ConfigManager
from status_tags import PrismFlowId
import xml.etree.ElementTree as ET

class UpdateQueryCriteria:
    def __init__(self, config, validation_object, transaction_record, sbn_Id):
        self.config = config
        self.validation_object = validation_object
        self.transaction_record = transaction_record
        self.sbn_Id = sbn_Id
        self.forced_subtype_check = None
        self.renewal_subtype_check = None
        self.resume_subtype_flow = None
        self.deact_subtype_check = None
        self.transaction_task_type = ""
        self.transaction_flow_id = ""
        self.transaction_srv_id = ""
        self.transaction_pmt_status = ""
        self.charge_type = ""
        self.update_query = ""
        self.generic_flow_handler_files = []
    
    # def get_nested_dictionaries(self, nested_list):
    #     dictionaries = []

    #     for item in nested_list:
    #         if isinstance(item, dict):
    #             dictionaries.append(item)
    #         elif isinstance(item, list):
    #             dictionaries.extend(self.get_nested_dictionaries(item))

    #     return dictionaries
        
    def update_query_formatter(self):
        configManager_object = ConfigManager(self.config, self.validation_object)
        subtype_parameter = configManager_object.get_enabled_subtype_parameter()
        logging.info("SUBTYPE_BOOLEAN_PARAM: %s", subtype_parameter)
        
        try:
            # nested_dictionaries = self.get_nested_dictionaries(configManager_object.subtype_parameter)
            # if nested_dictionaries:
            if subtype_parameter:
                for subType_enable_param_dict in subtype_parameter:
                    if subType_enable_param_dict["PARAM_NAME"] == "FORCED_SUBTYPE_CHECK":
                        if self.is_boolean(subType_enable_param_dict["PARAM_VALUE"]):
                            self.forced_subtype_check = subType_enable_param_dict["PARAM_VALUE"].lower() == 'true'
                    elif subType_enable_param_dict["PARAM_NAME"] == "RENEWAL_SUBTYPE_CHECK":
                        if self.is_boolean(subType_enable_param_dict["PARAM_VALUE"]):
                            self.renewal_subtype_check = subType_enable_param_dict["PARAM_VALUE"].lower() == 'true'
                    elif subType_enable_param_dict["PARAM_NAME"] == "RESUME_SUBTYPE_FLOW":
                        if self.is_boolean(subType_enable_param_dict["PARAM_VALUE"]):
                            self.resume_subtype_flow = subType_enable_param_dict["PARAM_VALUE"].lower() == 'true'
                    elif subType_enable_param_dict["PARAM_NAME"] == "DEACT_SUBTYPE_CHECK":
                        if self.is_boolean(subType_enable_param_dict["PARAM_VALUE"]):
                            self.deact_subtype_check = subType_enable_param_dict["PARAM_VALUE"].lower() == 'true'
                        
            logging.info("FORCED_SUBTYPE_CHECK: %s", self.forced_subtype_check)
            logging.info("RENEWAL_SUBTYPE_CHECK: %s", self.renewal_subtype_check)
            logging.info("RESUME_SUBTYPE_FLOW: %s", self.resume_subtype_flow)
            logging.info("DEACT_SUBTYPE_CHECK: %s", self.deact_subtype_check)

        except KeyError as err:
            logging.info(err)
        
        if self.transaction_record:
            self.transaction_flow_id = str(self.transaction_record["system_info"]).split("flowId:")[1].split("|")[0]
            self.transaction_srv_id = self.transaction_record["srv_id"]
            self.transaction_task_type = self.transaction_record["task_type"]
            self.transaction_pmt_status = self.transaction_record["pmt_status"]
            logging.info('TRANSACTION_TASK_TYPE: %s', self.transaction_task_type)
            
            self.get_charge_type()
            self.set_update_query(configManager_object)
              
    def get_charge_type(self):
        for status_chargeType, status_flowId in PrismFlowId.__dict__.items():
            if not status_chargeType.startswith("__"):
                for sflowId in status_flowId:
                    if self.transaction_flow_id == sflowId:
                        self.charge_type = status_chargeType
                        break
    
    def set_update_query(self, configManager_object):
        #check for generic flow handler for retry next task type
        logging.info("NEXT_TASK_TYPE: %s", self.next_task_type)
        next_task_type = self.get_next_task_type(configManager_object)
        
        if next_task_type:
            if self.transaction_pmt_status != 1:
                self.update_query = """
                                        UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = %s,
                                        task_status = 0, remote_status = 127, pmt_status = 0, cycle_status = 'R',
                                        charge_schedule = now() WHERE sbn_id = %s
                                    """ % (next_task_type, self.sbn_Id)
            else:
                self.update_query = """
                                        UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = %s,
                                        task_status = 0, remote_status = 127, cycle_status = 'R',
                                        charge_schedule = now() WHERE sbn_id = %s
                                    """ % (next_task_type, self.sbn_Id)
                
        elif self.transaction_task_type in ('CN', 'S', 'L', 'I', 'C'):
            self.update_query = """
                                    UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                    remote_status = 127, cycle_status = 'R', charge_schedule = now()
                                    WHERE sbn_id = %s
                                """ % (self.sbn_Id)
        
        elif self.transaction_task_type in ('V', 'Q', 'B', 'R'):
            if self.charge_type == 'A':
                if not self.forced_subtype_check:
                    
                    if self.transaction_task_type == 'B':
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'Q',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0,
                                                system_info = replace(system_info,'StepChgId:','StepChgId_:')
                                                where sbn_id = %s
                                            """ % (self.sbn_Id)
                    
                    elif self.transaction_task_type == "R":
                        if self.transaction_pmt_status != 1:
                            self.update_query = """
                                                    UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type='Q',
                                                    task_status=0, remote_status=127, cycle_status='R',
                                                    charge_schedule=now() WHERE sbn_id = %s
                                                """ % (self.sbn_Id)
                        else:
                            self.update_query = """
                                                    UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                                    remote_status = 127, cycle_status = 'R',
                                                    charge_schedule = now() where sbn_id = %s
                                                """ % (self.sbn_Id)
                    
                    else:
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                                remote_status = 127, cycle_status = 'R', charge_schedule = now(),
                                                pmt_status = 0 WHERE sbn_id = %s
                                            """ % (self.sbn_Id)
                
                else:
                    
                    if self.transaction_task_type == 'B':
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0,
                                                system_info = replace(system_info,'StepChgId:','StepChgId_:')
                                                WHERE sbn_id = %s
                                            """ % (self.sbn_Id)
                    
                    elif self.transaction_task_type == "R":
                        if self.transaction_pmt_status != 1:
                            self.update_query = """
                                                    UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type='Q',
                                                    task_status=0, remote_status=127, cycle_status='R',
                                                    charge_schedule=now() WHERE sbn_id = %s
                                                """ % (self.sbn_Id)
                        else:
                            self.update_query = """
                                                    UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                                    remote_status = 127, cycle_status = 'R',
                                                    charge_schedule = now() WHERE sbn_id = %s
                                                """ % (self.sbn_Id)
                    
                    else:
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s
                                            """ % (self.sbn_Id)
            
            elif self.charge_type == 'R':
                if not self.forced_subtype_check:
                    if self.transaction_task_type == 'B':
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'Q',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0,
                                                system_info = replace(system_info,'StepChgId:','StepChgId_:')
                                                WHERE sbn_id = %s
                                            """ % (self.sbn_Id)
                    else:
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                                remote_status = 127, cycle_status = 'R', charge_schedule = now(),
                                                pmt_status = 0 WHERE sbn_id = %s
                                            """ % (self.sbn_Id)
                
                else:
                    if self.transaction_task_type == 'B':
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0,
                                                system_info = replace(system_info,'StepChgId:','StepChgId_:')
                                                where sbn_id = %s
                                            """ % (self.sbn_Id)
                    else:
                        self.update_query = """
                                                UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S',
                                                task_status = 0, remote_status = 127, cycle_status = 'R',
                                                charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s
                                            """ % (self.sbn_Id) 
                
        elif self.transaction_task_type in ('D', 'H'):
            if self.transaction_task_type == 'D':
                if not self.deact_subtype_check:
                    self.update_query = """
                                            UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0,
                                            remote_status = 127, cycle_status = 'R', charge_schedule = now(),
                                            pmt_status = 0 WHERE sbn_id = %s
                                        """ % (self.sbn_Id)
                else:
                    self.update_query = """
                                            UPDATE SUBSCRIPTIONS SET queue_id = 99, task_type = 'S',
                                            task_status = 0, remote_status = 127, cycle_status = 'R',
                                            charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s
                                        """ % (self.sbn_Id) 
            else:
                self.update_query = "UPDATE SUBSCRIPTIONS SET queue_id = 99, task_status = 0, remote_status = 127, cycle_status = 'R', charge_schedule = now(), pmt_status = 0 WHERE sbn_id = %s"
    
    def get_next_task_type(self, configManager_object):
        #generic flow handler case
        flow_handler_params = []
        next_task_type = None
        
        logging.info("FLOW_ID: %s AND CHARGE_TYPE: %s", self.transaction_flow_id, self.charge_type)
        flow_handler_map = configManager_object.get_flow_handler_mapping()
        
        if flow_handler_map:
            for data in flow_handler_map:
                
                if self.charge_type == data["TRANSACTION_TYPE"]:
                    flow_handler_params.append(data["PARAMS"])
                    
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
                
                if self.transaction_pmt_status != 1:
                    # Find the initialTask element
                    initial_task_element = root.find('initialTask')

                    # Get the value of the value attribute
                    next_task_type = initial_task_element.get('value')
                else:
                    retry_element = root.find(".//retry")
                    task_case_elements = retry_element.findall("taskCase")

                    # Find the nextTask value based on the current task
                    next_task = None
                    
                    for task_case_element in task_case_elements:
                        task_value = task_case_element.get("currentTask")
                        if task_value == self.transaction_task_type:
                            next_task = task_case_element.get("nextTask")
                            next_task_type = next_task
                            
        logging.info("CURRENT_TASK_TYPE: %s AND NEXT_TASK_TYPE: %s", self.transaction_task_type, self.next_task_type)
        return next_task_type
                        
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
                  
    