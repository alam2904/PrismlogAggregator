"""
importing modules
"""
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import shutil
import os
from input_validation import InputValidation
from path_initializer import LogPathFinder
from log_processor import PROCESSOR
from configparser import ConfigParser
class Main:

    def init(self):
        
        logging.basicConfig(filename='log_aggregator.log', filemode='w', format='[%(asctime)s,%(msecs)d]%(pathname)s:(%(lineno)d)-%(levelname)s - %(message)s', datefmt='%y-%m-%d %H:%M:%S', level=logging.DEBUG)
        
        start = time.time()
        logging.debug(start)
    
        bdt = datetime.today() - timedelta(days=3)
        back_date = datetime.strftime(bdt, "%Y-%m-%d")
        logging.info('back date: %s', back_date)
        
        outputDirectory_object = Path('out')
        try:
            outputDirectory_object.mkdir(exist_ok=False)
        except FileExistsError as error:
            logging.info('out directory already exists. Going to fetch 3 days old files from today and remove.')
            outfiles = [p for p in outputDirectory_object.glob(f"*_{back_date}_*.*")]
            if bool(outfiles):
                for file in outfiles:
                    if os.path.isfile(file):
                        os.remove(file)
                        logging.info('3 day back dated files removed: %s', file)
                    else:
                        logging.info('back dated file does not exists: %s', file)
            else:
                logging.info('back dated file does not exists')

        logging.info('Log aggregation started')
        logging.info("******************************")

        num_argv = len(sys.argv)
        logging.debug('Number of arguments passed is %s', num_argv - 1)

        if num_argv == 3:
            logging.debug('Arguments passed are : msisdn=%s and search_date=%s', sys.argv[1], sys.argv[2])
            validation_object = InputValidation(sys.argv[1], sys.argv[2])

            try:
                # msisdn = validation_object.validate_msisdn()
                msisdn = sys.argv[1]
                input_date = validation_object.validate_date()
            except Exception as error:
                logging.exception(error)

            logging.info('\n')
            
            if validation_object.is_input_valid:
                file = Path('config.properties')
                if file.exists():
                    initializedPath_object = LogPathFinder(file)
                    config = ConfigParser()
                    config.read(file)
            
                    if config.has_option('tomcat', 'TRANS_BASE_DIR'):
                        try:
                            initializedPath_object.initialize_tomcat_path('tomcat')
                            logging.info('TOMCAT PATH INITIALIZED')
                            formatter = "#" * 100
                            logging.info('%s', formatter)
                            for key, value in initializedPath_object.tomcat_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                        except ValueError as error:
                            logging.warning('Tomcat path not initialized. %s', error)
                        except Exception as error:
                                logging.warning(error)
                    else:
                        logging.error('tomcat TRANS_BASE_DIR path not present in config.properties')
                    
                    logging.info('\n')
                    
                    if config.has_option('prismd', 'TRANS_BASE_DIR'):
                        try:
                            initializedPath_object.initialize_prism_path('prismd')
                            logging.info('PRISM PATH INITIALIZED')
                            formatter = "#" * 100
                            logging.info('%s', formatter)
                            for key, value in initializedPath_object.prism_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                        except ValueError as error:
                            logging.warning('Prism path not initialized. %s', error)
                        except Exception as error:
                            logging.warning(error)
                    else:
                        logging.error('prismd TRANS_BASE_DIR path not present in config.properties')
                        
                    logging.info('\n')
                    
                    if config.has_option('smsd', 'TRANS_BASE_DIR'):
                        try:
                            initializedPath_object.initialize_sms_path('smsd')
                            logging.info('SMS PATH INITIALIZED')
                            formatter = "#" * 100
                            logging.info('%s', formatter)
                            for key, value in initializedPath_object.sms_log_path_dict.items():
                                logging.info('%s : %s', key, value)
                        except ValueError as error:
                            logging.warning('SMS path not initialized. %s', error)
                        except Exception as error:
                            logging.warning(error)
                    else:
                        logging.error('prismd TRANS_BASE_DIR path not present in config.properties')
                        
                    logging.info('\n')

                    if initializedPath_object.is_tomcat_tlog_path or initializedPath_object.is_prism_tlog_path or initializedPath_object.is_sms_tlog_path:
                        is_tomcat_tlog_path = initializedPath_object.is_tomcat_tlog_path
                        is_prism_tlog_path = initializedPath_object.is_prism_tlog_path
                        is_sms_tlog_path = initializedPath_object.is_sms_tlog_path
                        
                        processor_object = PROCESSOR(msisdn, input_date, outputDirectory_object, file)
                        processor_object.process(is_tomcat_tlog_path, is_prism_tlog_path, is_sms_tlog_path, initializedPath_object)
                    else:
                        logging.error('Transaction log path initialization failed.')

                    logging.info('Log aggregation finished')
                    logging.info("**********************************")
                else:
                    logging.error('config.properties file does not exists. Hence process failed')
                
            else:
                logging.error('Invalid input. Log aggregation failed to process')
                logging.info("**********************************")

        else:
            logging.error('Invalid number of arguments passed')
            logging.debug('Log aggregation failed to process')
            logging.info("**********************************")

        end = time.time()
        logging.debug(end)
        
        duration = end - start
        logging.debug('Total time taken %s', duration)
        
        if Path('log_aggregator.log').exists():
            shutil.move('log_aggregator.log', 'out/log_aggregator.log')
            


if __name__ == '__main__':
    main_object = Main()
    main_object.init()