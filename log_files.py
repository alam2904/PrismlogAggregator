"""
path finder class
"""
import logging
from datetime import datetime
from pathlib import Path


class LogFileFinder():
    """
    transaction and daemon log path finder class
    """
    def __init__(self, input_date, initializedPath_object):
        self.input_date = input_date
        self.initializedPath_object = initializedPath_object
        self.is_prism_billing_tlog_path = False
        self.is_tomcat_billing_tlog_path = False
        self.is_sms_tlog_path = False
    
    def prism_billing_tlog_path(self):
        
        log_path = self.initializedPath_object

        prism_tlog_path = f"{log_path.prism_log_path_dict[log_path.prism_tlog_log_path]}/BILLING"
        path = Path(rf"{prism_tlog_path}")

        if path.exists():
            logging.debug('Prism BILLING tlog path exists.')
            self.set_prism_billing_path(True)
        else:
            self.set_prism_billing_path(False)
            logging.debug('Prism BILLING tlog path does not exists')
    
    def tomcat_billing_tlog_path(self):
        
        log_path = self.initializedPath_object
        tomcat_tlog_path = f"{log_path.tomcat_log_path_dict[log_path.tomcat_tlog_log_path]}BILLING_REALTIME"
        path = Path(rf"{tomcat_tlog_path}")

        if path.exists():
            logging.debug('Tomcat BILLING tlog path exists.')
            self.set_tomcat_billing_path(True)
        else:
            self.set_tomcat_billing_path(False)
            logging.debug('Tomcat BILLING tlog path does not exists')
    
    def sms_tlog_path(self):
        
        log_path = self.initializedPath_object

        sms_tlog_path = f"{log_path.sms_log_path_dict[log_path.sms_tlog_log_path]}/SMS"
        path = Path(rf"{sms_tlog_path}")

        if path.exists():
            logging.debug('Sms tlog path exists.')
            self.set_sms_path(True)
        else:
            self.set_sms_path(False)
            logging.debug('Sms tlog path does not exists')

    def prism_billing_tlog_files(self, input_trans_date):
        """
        function to find prism tlog file path
        """
        tlog_files = []
        
        logPath_object = self.initializedPath_object

        prism_tlog_path = f"{logPath_object.prism_log_path_dict[logPath_object.prism_tlog_log_path]}/BILLING"
        path = Path(rf"{prism_tlog_path}")

        try:
            billing_tlog_files_tmp = [p for p in path.glob(f"TLOG_BILLING_{input_trans_date}*.tmp")]
            billing_tlog_files_log = [p for p in path.glob(f"TLOG_BILLING_{input_trans_date}*.log")]
            
            if bool(billing_tlog_files_log):
                for prism_billing_files in billing_tlog_files_log:
                    tlog_files.append(prism_billing_files)
            if bool(billing_tlog_files_tmp):
                for prism_billing_files in billing_tlog_files_tmp:
                    tlog_files.append(prism_billing_files)
                    
            else:
                logging.debug('Prism billing tlog directory does not have %s dated files', input_trans_date)
            
            return tlog_files

        except ValueError as error:
            logging.exception(error)
        except Exception as error:
            logging.exception(error)
        
        return None
    
    def tomcat_billing_tlog_files(self, input_trans_date):
        """
        function to find tomcat tlog file path
        """
        tlog_files = []
        
        logPath_object = self.initializedPath_object

        tomcat_tlog_path = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_tlog_log_path]}BILLING_REALTIME"
        path = Path(rf"{tomcat_tlog_path}")

        try:
            billing_tlog_files_tmp = [p for p in path.glob(f"TLOG_BILLING_REALTIME_{input_trans_date}*.tmp")]
            billing_tlog_files_log = [p for p in path.glob(f"TLOG_BILLING_REALTIME_{input_trans_date}*.log")]
            if bool(billing_tlog_files_log):
                for tomcat_billing_files in billing_tlog_files_log:
                    tlog_files.append(tomcat_billing_files)
                
            if bool(billing_tlog_files_tmp):
                for tomcat_billing_files in billing_tlog_files_tmp:
                    tlog_files.append(tomcat_billing_files)
                
            else:
                logging.debug('Tomcat billing tlog directory does not have %s dated files', input_trans_date)
                
            return tlog_files

        except ValueError as error:
            logging.exception(error)
        except Exception as error:
            logging.exception(error)
        
        return None

    def sms_tlog_files(self, input_trans_date):
        """
        function to find sms tlog file path
        """
        tlog_files = []
        
        logPath_object = self.initializedPath_object

        prism_tlog_path = f"{logPath_object.sms_log_path_dict[logPath_object.sms_tlog_log_path]}/SMS"
        path = Path(rf"{prism_tlog_path}")

        try:
            sms_tlog_files_tmp = [p for p in path.glob(f"TLOG_SMS_{input_trans_date}*.tmp")]
            sms_tlog_files_log = [p for p in path.glob(f"TLOG_SMS_{input_trans_date}*.log")]
            
            if bool(sms_tlog_files_log):
                for sms_files in sms_tlog_files_log:
                    tlog_files.append(sms_files)
            if bool(sms_tlog_files_tmp):
                for sms_files in sms_tlog_files_tmp:
                    tlog_files.append(sms_files)
                    
            else:
                logging.debug('Sms tlog directory does not have %s dated files', input_trans_date)
            
            return tlog_files

        except ValueError as error:
            logging.exception(error)
        except Exception as error:
            logging.exception(error)
        
        return None
    
    def prism_daemonlog_file(self):
        """
        function to find prism daemon log file path
        """
        logPath_object = self.initializedPath_object
        try:
            prism_daemon_log_path = f"{logPath_object.prism_log_path_dict[logPath_object.prism_daemon_log_path]}"
            if prism_daemon_log_path:
                return prism_daemon_log_path
            else:
                logging.debug('Prism daemon log path does not exists')
            return None
        except KeyError as ex:
            logging.debug('Prism daemon log path does not exists')
    
    def tomcat_daemonlog_file(self):
        """
        function to find tomcat daemon log file path
        """
        logPath_object = self.initializedPath_object
        try:
            tomcat_daemon_log_path = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_daemon_log_path]}"
            if tomcat_daemon_log_path:
                return tomcat_daemon_log_path
            else:
                logging.debug('Tomcat daemon log path does not exists')
            return None
        except KeyError as ex:
            logging.debug('Tomcat daemon log path does not exists')
    
    def prism_rootlog_file(self):
        """
        function to find prism root log file path
        """
        logPath_object = self.initializedPath_object
        try:
            prism_root_log_path = f"{logPath_object.prism_log_path_dict[logPath_object.prism_root_log_path]}"
            if prism_root_log_path:
                return prism_root_log_path
            else:
                logging.debug('Prism root log path does not exists')
            return None
        except KeyError as ex:
            logging.debug('Prism root log path does not exists')
            
    
    def tomcat_rootlog_file(self):
        """
        function to find tomcat root log file path
        """
        logPath_object = self.initializedPath_object
        try:
            tomcat_root_log_path = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_root_log_path]}"
            if tomcat_root_log_path:
                return tomcat_root_log_path
            else:
                logging.debug('Tomcat root log path does not exists')
            return None
        except KeyError as ex:
            logging.debug('Tomcat root log path does not exists')
            

    def prism_daemonlog_backup_file(self):
        """
        function to find prism daemon log backup file path
        """
        logPath_object = self.initializedPath_object
        try:
            prism_daemon_log_backup = f"{logPath_object.prism_log_path_dict[logPath_object.prism_daemon_log_backup_path]}"
            
            backup_date = self.input_date
            date = self.get_backup_date(backup_date)

            prism_daemon_log_backup_path = f"{prism_daemon_log_backup}{date[0]}-{date[1]}/prismD-{date[0]}-{date[1]}-{date[2]}*.gz"
            
            if prism_daemon_log_backup_path:
                return prism_daemon_log_backup_path
            else:
                logging.debug('Prism daemon backup log path does not exists')
            return None
        except KeyError as ex:
                logging.debug('Prism daemon backup log path does not exists')
            
    
    def tomcat_daemonlog_backup_file(self):
        """
        function to find tomcat daemon log backup file path
        """
        logPath_object = self.initializedPath_object
        try:
            tomcat_daemon_log_backup = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_daemon_log_backup_path]}"
            
            backup_date = self.input_date
            date = self.get_backup_date(backup_date)

            tomcat_daemon_log_backup_path = f"{tomcat_daemon_log_backup}{date[0]}-{date[1]}/tomcat-{date[0]}-{date[1]}-{date[2]}*.gz"
            
            if tomcat_daemon_log_backup_path:
                return tomcat_daemon_log_backup_path
            else:
                logging.debug('Tomcat daemon backup log path does not exists')
            return None
        except KeyError as ex:
                logging.debug('Tomcat daemon backup log path does not exists')
            

    def prism_rootlog_backup_file(self):
        """
        function to find prism root log backup file path
        """
        logPath_object = self.initializedPath_object
        try:
            prism_root_log_backup = f"{logPath_object.prism_log_path_dict[logPath_object.prism_root_log_backup_path]}"
            
            backup_date = self.input_date
            date = self.get_backup_date(backup_date)
            
            prism_root_log_backup_path = f"{prism_root_log_backup}{date[0]}-{date[1]}/root-{date[0]}-{date[1]}-{date[2]}*.gz"
            
            if prism_root_log_backup_path:
                return prism_root_log_backup_path
            else:
                logging.debug('Prism root backup log path does not exists')
            return None
        except KeyError as ex:
                logging.debug('Prism root backup log path does not exists')
    
    def tomcat_rootlog_backup_file(self):
        """
        function to find tomcat root log backup file path
        """
        logPath_object = self.initializedPath_object
        try:
            tomcat_root_log_backup = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_root_log_backup_path]}"
            
            backup_date = self.input_date
            date = self.get_backup_date(backup_date)
            
            tomcat_root_log_backup_path = f"{tomcat_root_log_backup}{date[0]}-{date[1]}/root-{date[0]}-{date[1]}-{date[2]}*.gz"
            
            if tomcat_root_log_backup_path:
                return tomcat_root_log_backup_path
            else:
                logging.debug('Tomcat root backup log path does not exists')
            return None
        except KeyError as ex:
                logging.debug('Tomcat root backup log path does not exists')
            
    
    def tomcat_queue_id_99_log_file(self):
        """
        function to find tomcat queue_id_99 log file path
        """
        logPath_object = self.initializedPath_object
        try:
            queue_id_99_log_path = f"{logPath_object.tomcat_log_path_dict[logPath_object.tomcat_queue_id_processor_99_log_path]}"
            
            if queue_id_99_log_path:
                return queue_id_99_log_path
            else:
                logging.debug('tomcat queue_id_99 log path does not exists')
            return None
        except KeyError as ex:
            logging.debug('tomcat queue_id_99 log path does not exists')
            
    
    def prism_queue_id_99_log_file(self):
        """
        function to find prism queue_id_99 log file path
        """
        logPath_object = self.initializedPath_object
        try:
            queue_id_99_log_path = f"{logPath_object.prism_log_path_dict[logPath_object.prism_queue_id_processor_99_log_path]}"
            
            if queue_id_99_log_path:
                return queue_id_99_log_path
            else:
                logging.debug('prism queue_id_99 log path does not exists')
            return None
        except KeyError as ex:
                logging.debug('prism queue_id_99 log path does not exists')
            

    def get_backup_date(self, input_date):
        dts = datetime.strptime(input_date, "%Y%m%d")
        dtf = dts.strftime("%Y-%m-%d")
        date_formated = dtf.split("-")
        return date_formated

    def set_prism_billing_path(self, is_prism_billing_tlog_path):
        self.is_prism_billing_tlog_path = is_prism_billing_tlog_path
    
    def set_tomcat_billing_path(self, is_tomcat_billing_tlog_path):
        self.is_tomcat_billing_tlog_path = is_tomcat_billing_tlog_path

    def set_sms_path(self, is_sms_tlog_path):
        self.is_sms_tlog_path = is_sms_tlog_path
    
    def get_prism_path(self):
        return self.is_prism_billing_tlog_path
    
    def get_tomcat_path(self):
        return self.is_prism_billing_tlog_path
    
    def get_sms_path(self):
        return self.is_sms_tlog_path