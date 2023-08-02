from collections import OrderedDict
from datetime import datetime, timedelta
import json
import socket
import traceback
from status_tags import logMode
import logging
from configManager import ConfigManager
from status_tags import TimeZoneGmtOffsetValue

class InputValidation:
    """
    input data validation class
    """
    
    def __init__(self, num_argv, msisdn, operator_id, start_date, end_date, input_mode, reprocess, config):
        self.num_argv = num_argv
        self.msisdn = msisdn
        self.operator_id = operator_id
        self.fmsisdn = ""
        self.non_converted_start_date = start_date
        self.non_converted_end_date = end_date 
        self.start_date = start_date
        self.end_date = end_date
        self.input_mode = input_mode 
        self.log_mode = "txn"
        self.is_sub_reprocess_required = reprocess
        self.config = config
        self.is_input_valid = False
        self.site_id = ""
        self.time_zone = ""
        self.file_ids = []
        self.is_multitenant_system = False


    def validate_argument(self):
        #Input argument validation
        logging.debug('Number of arguments passed is: %s', self.num_argv - 2)
        if self.num_argv == 3:
            if self.validate_msisdn():
                if self.validate_operator_site_map():
                    if self.validate_date():
                        logging.info("IS_INPUT_DATE_VALID: %s", self.is_input_valid)
                        if self.validate_log_mode():
                            logging.info("IS_LOG_MODE_VALID: %s", self.is_input_valid)
                            if self.validate_reprocessing():
                                pass
            logging.info("IS_INPUT_VALID: %s", self.is_input_valid)
            logging.info("IS_MULTITENANT_SYSTEM: %s", self.is_multitenant_system)
        
        logging.debug('Arguments passed are :- msisdn:%s, operator_id: %s, start_date:%s, end_date:%s and log_mode:%s and reprocess_sub:%s', self.msisdn, self.operator_id, self.start_date, self.end_date, self.log_mode, self.is_sub_reprocess_required)
        logging.info("OPERATOR_ID: %s AND SITE_ID: %s AND TIME_ZONE: %s", self.operator_id, self.site_id, self.time_zone)
    
    def validate_msisdn(self):
        """
        Validate msisdn.
        """
        try:
            msisdn = self.msisdn
            special_characters = ['/', '#', '$', '*', '&', '@']
            self.fmsisdn = "".join(filter(lambda char: char not in special_characters , msisdn))
            logging.info('msisdn:%s and formatted msisdn after removal of special character just for creating out file:%s', self.msisdn, self.fmsisdn)
            return True
        except Exception as error:
            logging.error(error)
        
    def validate_operator_site_map(self):
        #operator site map validation
        configManager_object = ConfigManager(self.config)
        try:
            if self.is_digit():
                is_global_instance = configManager_object.is_multitenant_system()
                logging.info("OPERATOR_ID: %s AND IS_GLOBAL_INSTANCE: %s", self.operator_id, is_global_instance)
                
                if not is_global_instance and self.operator_id == '-1':
                    self.site_id = -1
                    self.is_input_valid = True
                
                elif is_global_instance and self.operator_id != '-1':
                    self.is_multitenant_system = True
                    self.site_id, self.time_zone, self.file_ids = configManager_object.get_operator_site_map(self.operator_id)
                    self.start_date = datetime.strftime(self.time_zone_conversion(self.start_date), "%Y-%m-%d")
                    self.end_date = datetime.strftime(self.time_zone_conversion(self.end_date), "%Y-%m-%d")
                    
                    logging.info("CONVERTED_START_DATE: %s AND CONVERTED_END_DATE: %s", self.start_date, self.end_date)
            
                    
                    if self.site_id and self.time_zone:
                        self.is_input_valid = True    
                    else:
                        self.is_input_valid = False
                else:
                    logging.error("Operator-site not matching")
                    self.is_input_valid = False
            else:
                self.is_input_valid = False
            return self.is_input_valid
        except Exception as ex:
            logging.error(traceback.format_exc())
            
    def validate_date(self):
        """
        Validate date.
        """
        try:
            self.start_date = datetime.strptime(str(self.start_date), "%Y-%m-%d")
            self.end_date = datetime.strptime(str(self.end_date), "%Y-%m-%d")
            self.is_input_valid = True
            logging.debug('start date: %s and end date: %s entered is valid', datetime.strftime(self.start_date, "%d-%m-%Y"), datetime.strftime(self.end_date, "%d-%m-%Y"))
            return self.is_input_valid
        except Exception as error:
            logging.error(traceback.format_exc())
            self.is_input_valid = False
            return self.is_input_valid
        
    def validate_log_mode(self):
        for var_name, var_value in logMode.__dict__.items():
            if not var_name.startswith("__"):
                if var_value == self.input_mode:
                    self.log_mode = var_value
                    self.is_input_valid = True
                    break
        else:
            self.is_input_valid = True
            logging.error('%s passed can eigther be "txn/error", default value is %s', self.input_mode, self.log_mode)
        return self.is_input_valid
    
    def validate_reprocessing(self):
        #is reprocessing required check
        arg = self.is_sub_reprocess_required
        if self.is_boolean(arg):
            self.is_sub_reprocess_required = arg.lower() == 'true'
        return True
    
    def time_zone_conversion(self, input_date):
        hostname = socket.gethostname()
        file_path = "{}.json".format(hostname)

        # read the file contents
        with open(file_path, 'r') as f:
            data = f.read()

        config = json.loads(data, object_pairs_hook=OrderedDict)
        
        op_time_zone_offset = ""
    
        try:
            tz_offset = config[hostname]["server_facts"]["tz_offset"]
            
            op_time_zone = self.time_zone.replace("/", "_")
            
            for var_name, var_value in TimeZoneGmtOffsetValue.__dict__.items():
                    if not var_name.startswith("__"):
                        if op_time_zone == var_name:
                            op_time_zone_offset = var_value
                            break
            else:
                logging.info("operator time zone not resolved")
            
            try:
                start_date = datetime.strptime(input_date, "%Y-%m-%d")
            except ValueError as err:
                start_date = datetime.strptime(input_date, "%Y-%m-%d %H:%M:%S")
            
            converted_date = start_date - (timedelta(hours=int(tz_offset[0:3]), minutes=int(tz_offset[3:6])) -\
                        timedelta(hours=int(op_time_zone_offset[0:3]), minutes=int(op_time_zone_offset[3:6])))
            
            return converted_date
        except KeyError as err:
            logging.error(err)
        
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']
    
    def is_digit(self):
        # Check if the operator id argument is an integer
            if self.operator_id[0] == '-' and self.operator_id[1:].isdigit():
                return True
            elif self.operator_id.isdigit():
                return True
            else:
                return False
        