from datetime import datetime
from status_tags import logMode
import logging


class InputValidation:
    """
    input data validation class
    """
    
    def __init__(self, num_argv, msisdn, start_date, end_date, input_mode, reprocess):
        self.num_argv = num_argv
        self.msisdn = msisdn
        self.fmsisdn = ""
        self.start_date = start_date
        self.end_date = end_date
        self.input_mode = input_mode 
        self.log_mode = "txn"
        self.is_sub_reprocess_required = reprocess
        self.is_input_valid = False


    def validate_argument(self):
        #Input argument validation
        logging.debug('Number of arguments passed is: %s', self.num_argv - 2)
        if self.num_argv == 3:
            for var_name, var_value in logMode.__dict__.items():
                if not var_name.startswith("__"):
                    if var_value == self.input_mode:
                        self.log_mode = var_value
                        self.is_input_valid = True
                        break
            else:
                self.is_input_valid = True
                logging.error('%s passed can eigther be "txn/error", default value is %s', self.input_mode, self.log_mode)

            arg = self.is_sub_reprocess_required
            
            if self.is_boolean(arg):
                self.is_sub_reprocess_required = arg.lower() == 'true'
            
            logging.debug('Arguments passed are :- msisdn:%s, start_date:%s, end_date:%s and log_mode:%s and reprocess_sub:%s', self.msisdn, self.start_date, self.end_date, self.log_mode, self.is_sub_reprocess_required)
            
    def is_boolean(self, arg):
        return arg.lower() in ['true', 'false']

    def validate_msisdn(self):
        """
        Validate msisdn.
        """
        try:
            msisdn = self.msisdn
            special_characters = ['/', '#', '$', '*', '&', '@']
            self.fmsisdn = "".join(filter(lambda char: char not in special_characters , msisdn))
            logging.info('msisdn:%s and formatted msisdn after removal of special character just for creating out file:%s', self.msisdn, self.fmsisdn)
        except Exception as error:
            logging.error('Invalid msisdn')
            raise

    def validate_date(self):
        """
        Validate date.
        """
        try:
            self.start_date = datetime.strptime(str(self.start_date), "%Y-%m-%d")
            self.end_date = datetime.strptime(str(self.end_date), "%Y-%m-%d")
            self.is_input_valid = True
            logging.debug('start date: %s and end date: %s entered is valid', datetime.strftime(self.start_date, "%d-%m-%Y"), datetime.strftime(self.end_date, "%d-%m-%Y"))
        except Exception as error:
            logging.error('start date: %s or/and end date: %s entered is of invalid format. The format should be "ddmmyyyy".', self.start_date, self.end_date)
            self.is_input_valid = False
            raise
        