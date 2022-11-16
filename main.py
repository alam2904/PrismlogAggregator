"""
importing modules
"""
import sys
import time
import logging
from pathlib import Path
import shutil
from input_validation import InputValidation
from path_initializer import LogPathFinder
from log_processor import PROCESSOR

class Main:

    def init(self):
        logging.basicConfig(filename='log_aggregator.log', filemode='w', format='[%(asctime)s,%(msecs)d]%(pathname)s:(%(lineno)d)-%(levelname)s - %(message)s', datefmt='%y-%m-%d %H:%M:%S', level=logging.DEBUG)
        
        start = time.time()
        logging.debug(start)

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
                initializedPath_object = LogPathFinder()
                try:
                    initializedPath_object.initialize_tomcat_path()
                    logging.info('TOMCAT PATH INITIALIZED')
                    formatter = "#" * 100
                    logging.info('%s', formatter)
                    for key, value in initializedPath_object.tomcat_log_path_dict.items():
                        logging.info('%s : %s', key, value)
                except ValueError as error:
                    logging.warning('Tomcat path not initialized. %s', error)
                except Exception as error:
                        logging.warning(error)
                
                logging.info('\n')
                try:
                    initializedPath_object.initialize_prism_path()
                    logging.info('PRISM PATH INITIALIZED')
                    formatter = "#" * 100
                    logging.info('%s', formatter)
                    for key, value in initializedPath_object.prism_log_path_dict.items():
                        logging.info('%s : %s', key, value)
                except ValueError as error:
                    logging.warning('Prism path not initialized. %s', error)
                except Exception as error:
                    logging.warning(error)
                    
                logging.info('\n')
                
                try:
                    initializedPath_object.initialize_sms_path()
                    logging.info('SMS PATH INITIALIZED')
                    formatter = "#" * 100
                    logging.info('%s', formatter)
                    for key, value in initializedPath_object.sms_log_path_dict.items():
                        logging.info('%s : %s', key, value)
                except ValueError as error:
                    logging.warning('SMS path not initialized. %s', error)
                except Exception as error:
                    logging.warning(error)
                    
                logging.info('\n')

                if initializedPath_object.is_tomcat or initializedPath_object.is_prsim or initializedPath_object.is_sms:
                    is_tomcat = initializedPath_object.is_tomcat
                    is_tomcat_tlog_path = initializedPath_object.is_tomcat_tlog_path
                    
                    is_prism = initializedPath_object.is_prsim
                    is_prism_tlog_path = initializedPath_object.is_prism_tlog_path
                    
                    is_sms = initializedPath_object.is_sms
                    is_sms_tlog_path = initializedPath_object.is_sms_tlog_path
                    
                    outputDirectory_object = Path('out')
                    try:
                        outputDirectory_object.mkdir(exist_ok=False)
                    except FileExistsError as error:
                        logging.info('out directory already exists. Hence flushing and recreating the same.')
                        shutil.rmtree(outputDirectory_object)
                        outputDirectory_object.mkdir()

                    processor_object = PROCESSOR(msisdn, input_date, outputDirectory_object)
                    processor_object.process(is_tomcat, is_prism, is_tomcat_tlog_path, is_prism_tlog_path, initializedPath_object)
                else:
                    logging.error('Since none of the process running. Process failed to aggregate log.')

                logging.info('Log aggregation finished')
                logging.info("**********************************")
                
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


if __name__ == '__main__':
    main_object = Main()
    main_object.init()