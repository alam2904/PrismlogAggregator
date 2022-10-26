from tlog_parser import PrismTlogParser
from daemonlog_parser import PrismDaemonLogParser
import logging

class PROCESSOR:
    """
    Processor class
    """
    def __init__(self, msisdn, input_date):
        self.msisdn = msisdn
        self.input_date = input_date

    def process(self):
        dictionary_of_tlogs = {}
        tlog_record_list = []
        worker_log_recod_list = []
        dictionary_of_search_value = {"TIMESTAMP" : "","THREAD" : "","MSISDN" : "","SUB_TYPE" : ""}

        tlogParser = PrismTlogParser(self.msisdn, self.input_date, dictionary_of_tlogs, tlog_record_list)

        if tlogParser.parse():
            logging.debug('Prism tlog parsed successfully')
            daemonLogParser = PrismDaemonLogParser(tlogParser.dictionary_of_tlogs, dictionary_of_search_value, worker_log_recod_list)
            daemonLogParser.parse()