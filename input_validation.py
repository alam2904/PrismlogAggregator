from collections import OrderedDict
from datetime import datetime, timedelta
import json
import socket
import traceback
from status_tags import TimeZoneGmtOffsetValue
import logging
from configManager import ConfigManager
from status_tags import TimeZoneGmtOffsetValue

class InputValidation:
    """
    input data validation class
    """
    
    def __init__(self, num_argv, msisdn, operator_id):
        self.num_argv = num_argv
        self.msisdn = msisdn
        self.operator_id = operator_id
        self.fmsisdn = ""
        self.non_converted_start_date = None
        self.non_converted_end_date = None 
        self.start_date = None
        self.end_date = None
        self.is_input_valid = False
        self.site_id = ""
        self.time_zone = ""
        self.file_ids = []
        self.is_multitenant_system = False


    def validate_argument(self, time_delta):
        #Input argument validation
        logging.debug('Number of arguments passed is: %s', self.num_argv - 2)
        if self.num_argv == 3:
            if self.validate_msisdn():
                if self.validate_operator_site_map(time_delta):
                    logging.info("input validation successfully done")
                        
            logging.info("IS_INPUT_VALID: %s", self.is_input_valid)
            logging.info("IS_MULTITENANT_SYSTEM: %s", self.is_multitenant_system)
        
        logging.debug('Arguments passed are :- msisdn:%s, operator_id: %s, time_delta_in_mins:%s', self.msisdn, self.operator_id, time_delta)
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
        
    def validate_operator_site_map(self, time_delta):
        #operator site map validation
        configManager_object = ConfigManager()
        
        td = int(time_delta)
        cur_date_time = datetime.now()
        end_date = datetime.strptime(datetime.strftime(cur_date_time, "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        
        try:
            if self.is_digit():
                is_global_instance = configManager_object.is_multitenant_system()
                logging.info("OPERATOR_ID: %s AND IS_GLOBAL_INSTANCE: %s", self.operator_id, is_global_instance)
                
                if not is_global_instance and self.operator_id == '-1':
                    self.non_converted_start_date = self.form_start_date(end_date)
                    self.non_converted_end_date = end_date
                    logging.info('NON_CONVERTED_START_DATE: %s - TIME_DELTA: %s = NON_CONVERTED_END_DATE: %s ', self.non_converted_start_date, td, self.non_converted_end_date)
                    self.site_id = -1
                    self.is_input_valid = True
                
                elif is_global_instance and self.operator_id != '-1':
                    self.is_multitenant_system = True
                    self.site_id, self.time_zone, self.file_ids = configManager_object.get_operator_site_map(self.operator_id)
                    self.end_date = self.time_zone_conversion(end_date)
                    self.start_date = self.form_start_date(self.end_date, td)
                    
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
            
    def form_start_date(self, end_date, td):
        """
        Validate date.
        """
        return end_date - timedelta(minutes=td)
        
    def time_zone_conversion(self, end_date):
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

            converted_date = end_date - (timedelta(hours=int(tz_offset[0:3]), minutes=int(tz_offset[3:6])) -\
                        timedelta(hours=int(op_time_zone_offset[0:3]), minutes=int(op_time_zone_offset[3:6])))
            
            return converted_date
        except KeyError as err:
            logging.error(err)
    
    def is_digit(self):
        # Check if the operator id argument is an integer
            if self.operator_id[0] == '-' and self.operator_id[1:].isdigit():
                return True
            elif self.operator_id.isdigit():
                return True
            else:
                return False
        