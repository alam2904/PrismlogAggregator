"""
tlog parser module
"""
import logging
from tlog import Tlog
from input_validation import InputValidation

class TlogParser:
    """
    Parse the tlog for various conditions
    """
    def __init__(self, msisdn, input_date, dictionary_of_tlogs, tlog_record_list, initializedPath_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.dictionary_of_tlogs = dictionary_of_tlogs
        self.tlog_record_list = tlog_record_list
        self.initializedPath_object = initializedPath_object

    def parse_prism(self):
        """
        Call to retreive tlog files and parse.
        """
        is_parsed = False
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list, self.initializedPath_object)
    
        if tlog_object.get_prism_billing_tlog():
            logging.debug('Tlog record found for %s', self.msisdn )
            filtered_prism_tlog = tlog_object.tlog_record_list
            logging.debug('Parsing tlog file')
            
            if filtered_prism_tlog:
                tlog_data = [data.split("|") for data in filtered_prism_tlog[0]]
                for cnt, data in enumerate(tlog_data):
                    tlog_key_value = {"TIMESTAMP" : "","THREAD" : "","SITE_ID" : "","MSISDN" : "","SUB_TYPE" : "","SBN_ID/EVT_ID" : "","SRV_KEYWORD" : "","CHARGE_TYPE" : "","PARENT_KEYWORD" : "","AMOUNT" : "","MODE" : "","USER" : "","REQUEST_DATE" : "","INVOICE_DATE" : "","EXPIRY_DATE" : "","RETRY_COUNT" : "","CYCLE_STATUS" : "","GRACE_COUNT" : "","GRACE_RETRY_COUNT" : "","NEW_SRV_KEYWORD" : "","INFER_SUB_STATUS" : "","CHARGE_KEYWORD" : "","TRIGGER_ID" : "","PACK_KEY" : "","PARENT_ID" : "","APP_NAME" : "","SITE_NAME" : "","[STCK=NEW_TYPE,MESSAGE]" : "","[CBAL=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]" : "","[RSRV=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]" : "","[CHG=PMT_STATUS,BILL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,RCHG_FILE_CHG,BAL]" : "","[REMT=REMOTE_STATUS,RETCODE,RETMSG]" : "","[CBCK=STATUS,RETCODE,RETMSG]" : "","[CONTENT_ID=[ContentInfo]]" : "","[CAMPAIGN_ID=[campaignId]]" : "","[TOTAL_CHG_AMNT=[totalChgAmnt]]" : "","[RECO:[ReconciliationData]]" : "","[TSK = TASK_TYPE,TASK_STATUS,PAYMENT STATUS,CHARGE_SCHEDULE,NEXT_BILL_DATE]" : ""}
                    self.dictionary_of_tlogs[f"dict_{cnt}"] = tlog_key_value
                    header_data = self.dictionary_of_tlogs[f"dict_{cnt}"]
                    temp = list(header_data)
                    for counter, tlog_header in enumerate(temp):
                        header_data[tlog_header] = self.data_in_tlog(data, counter)
                    self.dictionary_of_tlogs[f"dict_{cnt}"] = header_data
                is_parsed = True
            else:
                logging.error('No tlog found for given msisdn: %s', self.msisdn)
        else:
            logging.error('No tlog found for given msisdn: %s', self.msisdn)
        return is_parsed

    def data_in_tlog(self, data, index):
        """
        Returns data in tlog.
        """
        return data[index]