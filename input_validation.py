"""
input data validation module
"""
from datetime import datetime
import logging


class InputValidation:
    """
input data validation class
"""

    def validate_msisdn(self):
        """
        Validate msisdn.
        """
        while True:
            try:
                msisdn = int(input("Enter msisdn without country code: ").strip())
                logging.debug('msisdn entered is %s', msisdn)
                break
            except ValueError:
                logging.error('Invalid msisdn')
        return msisdn

    def validate_date(self):
        """
        Validate date.
        """
        while True:
            input_date = input("Enter transaction date in a format yyyymmdd: ").strip()
            is_valid_date = self.validate(input_date)
            if is_valid_date:
                break
        return input_date

    def validate(self, date):
        """
        function to validate input date for day
        """
        try:
            datetime.strptime(date, "%Y%m%d")
            logging.debug('Transaction date entered is %s', date)
            return True
        except ValueError as ex:
            logging.debug('date entered is %s', date)
            logging.debug(ex)
            return False