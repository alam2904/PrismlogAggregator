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
    def __init__(self, msisdn, input_date, dictionary_of_tlogs, tlog_record_list_prism, tlog_record_list_tomcat, tlog_record_list_sms, plog_record_list_prism, plog_record_list_tomcat, initializedPath_object):
        self.msisdn = msisdn
        self.input_date = input_date
        self.dictionary_of_tlogs = dictionary_of_tlogs
        self.tlog_record_list_prism = tlog_record_list_prism
        self.tlog_record_list_tomcat = tlog_record_list_tomcat
        self.tlog_record_list_sms = tlog_record_list_sms
        self.plog_record_list_prism = plog_record_list_prism
        self.plog_record_list_tomcat = plog_record_list_tomcat
        self.initializedPath_object = initializedPath_object
        self.filtered_prism_tlog = ""
        self.filtered_tomcat_tlog = ""
        self.filtered_sms_tlog = ""
        self.filtered_tomcat_plog = ""
        self.filtered_prism_plog = ""

    def parse_prism_automation(self, validation_object, data_automation_outfile, keyword):
        """
        Call to retreive prism tlog/plog files for automation.
        """
        is_parsed = False
        
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.tlog_record_list_sms, self.plog_record_list_prism, self.plog_record_list_tomcat, self.initializedPath_object)
        # tlog_object = Tlog(self.msisdn, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.initializedPath_object)
        if keyword == "tlog":
            if tlog_object.get_prism_billing_tlog_automation(validation_object, data_automation_outfile):
                logging.info('prism tlog data found')
                
                self.filtered_prism_tlog = tlog_object.tlog_record_list_prism
                
                if self.filtered_prism_tlog:
                    is_parsed = True
                else:
                    logging.error('No prism tlog found for given msisdn: %s', self.msisdn)
        
        elif keyword == "plog":
            if tlog_object.get_prism_perf_log_automation(validation_object, data_automation_outfile):
                logging.info('prism plog data found')
                
                self.filtered_prism_plog = tlog_object.plog_record_list_prism
                
                if self.filtered_prism_plog:
                    is_parsed = True
                else:
                    logging.error('No prism plog found for given msisdn: %s', self.msisdn)
        else:
            logging.info('No prism tlog/plog data found')
            
        return is_parsed
    
    def parse_prism(self, validation_object):
        """
        Call to retreive prism tlog files and parse.
        """
        is_parsed = False
        
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.tlog_record_list_sms, self.plog_record_list_prism, self.plog_record_list_tomcat, self.initializedPath_object)
    
        if tlog_object.get_prism_billing_tlog(validation_object):
            self.filtered_prism_tlog = tlog_object.tlog_record_list_prism
            
            if self.filtered_prism_tlog: 
                tlog_data = self.filtered_prism_tlog[-1].split("|")
                logging.info('tlog data : %s', tlog_data)
                tlog_key_value = ["TIMESTAMP","THREAD","SITE_ID","MSISDN","SUB_TYPE","SBN_ID/EVT_ID","SRV_KEYWORD","CHARGE_TYPE","PARENT_KEYWORD","AMOUNT","MODE","USER","REQUEST_DATE","INVOICE_DATE","EXPIRY_DATE","RETRY_COUNT","CYCLE_STATUS","GRACE_COUNT","GRACE_RETRY_COUNT","NEW_SRV_KEYWORD","INFER_SUB_STATUS","CHARGE_KEYWORD","TRIGGER_ID","PACK_KEY","PARENT_ID","APP_NAME","SITE_NAME","[STCK=NEW_TYPE,MESSAGE]","[CBAL=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]","[RSRV=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]","[CHG=PMT_STATUS,BILL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,RCHG_FILE_CHG,BAL]","[REMT=REMOTE_STATUS,RETCODE,RETMSG]","[CBCK=STATUS,RETCODE,RETMSG]","[CONTENT_ID=[ContentInfo]]","[CAMPAIGN_ID=[campaignId]]","[TOTAL_CHG_AMNT=[totalChgAmnt]]","[RECO:[ReconciliationData]]","[TSK = TASK_TYPE,TASK_STATUS,PAYMENT STATUS,CHARGE_SCHEDULE,NEXT_BILL_DATE]"]
                        
                for counter, tlog_header in enumerate(tlog_key_value):
                    try:
                        self.dictionary_of_tlogs[tlog_header] = self.data_in_tlog(tlog_data, counter)
                    except IndexError as ex:
                        logging.info('Header data did not match')
                is_parsed = True
                
                if tlog_object.get_prism_perf_log():
                    self.filtered_prism_plog = tlog_object.plog_record_list_prism
                    logging.info('perf log data : %s', self.filtered_prism_plog[-1].split("|"))
                
                else:
                    logging.error('No prism perf log found for given msisdn: %s', self.msisdn)
                    
            else:
                logging.error('No prism tlog found for given msisdn: %s', self.msisdn)
        else:
            logging.info('No prism tlog data found')
            
        return is_parsed
    
    def parse_tomcat_automation(self, validation_object, data_automation_outfile, keyword):
        """
        Call to retreive tomcat tlog files for automation.
        """
        is_parsed = False
        # tlog_object = Tlog(self.msisdn, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.initializedPath_object)
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.tlog_record_list_sms, self.plog_record_list_prism, self.plog_record_list_tomcat, self.initializedPath_object)
        if keyword == "tlog":
            if tlog_object.get_tomcat_billing_tlog_automation(validation_object, data_automation_outfile):
                logging.info('tomcat tlog data found')
                
                self.filtered_tomcat_tlog = tlog_object.tlog_record_list_tomcat   
                if self.filtered_tomcat_tlog:
                    is_parsed = True
                else:
                    logging.error('No tomcat tlog found for given msisdn: %s', self.msisdn)
        
        elif keyword == "plog":        
            if tlog_object.get_tomcat_perf_log_automation(validation_object, data_automation_outfile):
                logging.info('tomcat plog data found')
                
                self.filtered_tomcat_plog = tlog_object.plog_record_list_tomcat   
                if self.filtered_tomcat_plog:
                    is_parsed = True
                else:
                    logging.error('No tomcat plog found for given msisdn: %s', self.msisdn)
        else:
            logging.info('No tomcat tlog/plog data found')
        return is_parsed
    
    def parse_tomcat(self, validation_object):
        """
        Call to retreive tomcat tlog files and parse.
        """
        is_parsed = False
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.tlog_record_list_sms, self.plog_record_list_prism, self.plog_record_list_tomcat, self.initializedPath_object)
    
        if tlog_object.get_tomcat_billing_tlog(validation_object):
            self.filtered_tomcat_tlog = tlog_object.tlog_record_list_tomcat
                
            if self.filtered_tomcat_tlog:        
                tlog_data = self.filtered_tomcat_tlog[-1].split("|")
                logging.info('tlog data : %s', tlog_data)
                tlog_key_value = ["TIMESTAMP","THREAD","SITE_ID","MSISDN","SUB_TYPE","SBN_ID/EVT_ID","SRV_KEYWORD","CHARGE_TYPE","PARENT_KEYWORD","AMOUNT","MODE","USER","REQUEST_DATE","INVOICE_DATE","EXPIRY_DATE","RETRY_COUNT","CYCLE_STATUS","GRACE_COUNT","GRACE_RETRY_COUNT","NEW_SRV_KEYWORD","INFER_SUB_STATUS","CHARGE_KEYWORD","TRIGGER_ID","PACK_KEY","PARENT_ID","APP_NAME","SITE_NAME","[STCK=NEW_TYPE,MESSAGE]","[CBAL=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]","[RSRV=STATUS,BAL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,BAL]","[CHG=PMT_STATUS,BILL_AMOUNT,CHGMODE,BILLING_REFID,RETCODE,RETMSG,RCHG_FILE_CHG,BAL]","[REMT=REMOTE_STATUS,RETCODE,RETMSG]","[CBCK=STATUS,RETCODE,RETMSG]","[CONTENT_ID=[ContentInfo]]","[CAMPAIGN_ID=[campaignId]]","[TOTAL_CHG_AMNT=[totalChgAmnt]]","[RECO:[ReconciliationData]]","[TSK = TASK_TYPE,TASK_STATUS,PAYMENT STATUS,CHARGE_SCHEDULE,NEXT_BILL_DATE]"]
                    
                for counter, tlog_header in enumerate(tlog_key_value):
                    try:
                        self.dictionary_of_tlogs[tlog_header] = self.data_in_tlog(tlog_data, counter)
                    except IndexError as ex:
                        logging.info('Ignoring header index mapping')

                is_parsed = True
                
                if tlog_object.get_tomcat_perf_log():
                    self.filtered_tomcat_plog = tlog_object.plog_record_list_tomcat
                    logging.info('perf log data : %s', self.filtered_tomcat_plog[-1].split("|"))
                else:
                    logging.error('No tomcat perf log found for given msisdn: %s', self.msisdn)
                    
            else:
                logging.error('No tomcat tlog found for given msisdn: %s', self.msisdn)
        else:
            logging.info('No tomcat tlog data found')
        return is_parsed

    def parse_sms(self, validation_object):
        """
        Call to retreive sms tlog files and parse.
        """
        is_parsed = False
        tlog_object = Tlog(self.msisdn, self.input_date, self.tlog_record_list_prism, self.tlog_record_list_tomcat, self.tlog_record_list_sms, None, None, self.initializedPath_object)
    
        if tlog_object.get_sms_tlog(validation_object):
            self.filtered_sms_tlog = tlog_object.tlog_record_list_sms
                
            if self.filtered_sms_tlog:    
                tlog_data = self.filtered_sms_tlog[-1].split("|")
                logging.info('tlog data : %s', tlog_data)
                tlog_key_value = ["TIMESTAMP","THREAD","SITE_ID","MSISDN","SRNO","SRVCODE","MSG","HANDLER","STATUS","REMARKS","TIME TAKEN","SMS_INFO"]
                        
                for counter, tlog_header in enumerate(tlog_key_value):
                    try:
                        self.dictionary_of_tlogs[tlog_header] = self.data_in_tlog(tlog_data, counter)
                    except IndexError as ex:
                        logging.info('Header data did not match')
                
                is_parsed = True
            else:
                logging.error('No sms tlog found for given msisdn: %s', self.msisdn)
        else:
            logging.info('No sms tlog data found')
            
        return is_parsed
    
    def data_in_tlog(self, data, index):
        """
        Returns data in tlog.
        """
        try:
            return data[index]
        except IndexError as ex:
            raise