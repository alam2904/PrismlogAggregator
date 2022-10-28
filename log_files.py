"""
path finder class
"""
import logging
from pathlib import Path
from path_initializer import LogPathFinder


class LogFileFinder():
    """
    transaction and daemon log path finder class
    """
    def __init__(self):
        self.is_prism_billing_tlog_path = False
        # self.is_tomcat_path
    
    def prism_billing_tlog_path(self):
        
        log_path = LogPathFinder()
        log_path.initialize_prism_path()

        prism_tlog_path = f"{log_path.prism_log_path_dict[log_path.prism_tlog_log_path]}/BILLING"
        path = Path(rf"{prism_tlog_path}")

        if path.exists():
            logging.debug('Prism BILLING tlog path exists.')
            self.set_prism_billing_path(True)
        else:
            self.set_prism_billing_path(False)
            logging.error('Prism BILLING tlog path does not exists')
    

    def prism_billing_tlog_files(self, input_trans_date):
        """
        function to find prism tlog file path
        """
        tlog_files = []
        
        log_path = LogPathFinder()
        log_path.initialize_prism_path()

        prism_tlog_path = f"{log_path.prism_log_path_dict[log_path.prism_tlog_log_path]}/BILLING"
        path = Path(rf"{prism_tlog_path}")

        try:
            billing_tlog_files = [p for p in path.glob(f"TLOG_BILLING_{input_trans_date}*.*")]
            if bool(billing_tlog_files):
                for prism_billing_files in billing_tlog_files:
                    tlog_files.append(prism_billing_files)
                    return tlog_files
            else:
                logging.error('Prism billing tlog directory does not have %s dated files', input_trans_date)

        except ValueError as error:
            logging.exception(error)
        except Exception as error:
            logging.exception(error)
        
        return None

    def prism_daemonlog_files(self):
        """
        function to find prism daemon log file path
        """
        log_path = LogPathFinder()
        log_path.initialize_prism_path()
        
        # if self.get_prism_path():
        prism_daemon_log_path = f"{log_path.prism_log_path_dict[log_path.prism_daemon_log_path]}"

        if prism_daemon_log_path:
            return prism_daemon_log_path
        else:
            logging.error('Prism daemon log path does not exists')
        # else:
        #     logging.debug('Prism tlog path does not exists')
        
        return None

    def set_prism_billing_path(self, is_prism_billing_tlog_path):
        self.is_prism_billing_tlog_path = is_prism_billing_tlog_path

    def get_prism_path(self):
        return self.is_prism_billing_tlog_path