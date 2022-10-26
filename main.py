"""
importing modules
"""
import time
from tlog_parser import PrismTlogParser
from input_validation import InputValidation
from daemonlog_parser import PrismDaemonLogParser
import logging

class Main:

    def process(self):
        logging.basicConfig(filename='log_aggregator.log', filemode='w', format='[%(asctime)s,%(msecs)d]%(pathname)s:(%(lineno)d)-%(levelname)s - %(message)s', datefmt='%y-%m-%d %H:%M:%S', level=logging.DEBUG)

        logging.info('Log aggregation started')
        logging.info("******************************")

        dictionary_of_tlogs = {}
        tlog_record_list = []
        worker_log_recod_list = []
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : ""}

        start = time.time()
        validation = InputValidation()
        msisdn = validation.validate_msisdn()
        input_date = validation.validate_date()


        tlogParser = PrismTlogParser(msisdn, input_date, dictionary_of_tlogs, tlog_record_list)

        if tlogParser.parse():
            logging.debug('Prism tlog parsed successfully')
            daemonLogParser = PrismDaemonLogParser(tlogParser.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list)
            daemonLogParser.parse()
        end = time.time()
        duration = end - start
        print(duration)
        logging.info('Log aggregation finished')
        logging.info("**********************************")

if __name__ == '__main__':
    main = Main()
    main.process()