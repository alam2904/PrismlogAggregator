"""
input data validation module
"""
from datetime import datetime
import logging


class InputValidation:
    """
    input data validation class
    """
    
    def __init__(self, msisdn, input_date):
        self.msisdn = msisdn
        self.input_date = input_date
        self.is_input_valid = False

    def validate_msisdn(self):
        """
        Validate msisdn.
        """
        try:
            msisdn = int(self.msisdn)
            logging.debug('msisdn entered is valid : %s', msisdn)
            self.is_input_valid = True
            return msisdn
        except Exception as error:
            logging.error('Invalid msisdn')
            raise

    def validate_date(self):
        """
        Validate date.
        """
        # is_valid_date = self.validate(self.input_date)
        try:
            datetime.strptime(self.input_date, "%Y%m%d")
            self.is_input_valid = True
            logging.debug('Transaction date entered is valid : %s', self.input_date)
            return self.input_date

        except Exception as error:
            logging.error('Transaction date %s entered is of invalid format.', self.input_date)
            self.is_input_valid = False
            raise
