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
        logging.debug('Number of arguments passed is %s', num_argv)

        if num_argv == 3:
            logging.debug('Arguments passed are : %s and %s', sys.argv[1], sys.argv[2])
            validation_object = InputValidation(sys.argv[1], sys.argv[2])

            try:
                msisdn = validation_object.validate_msisdn()
                input_date = validation_object.validate_date()
            except Exception as error:
                logging.exception(error)

            if validation_object.is_input_valid:
                initializedPath_object = LogPathFinder()
                try:
                    initializedPath_object.initialize_tomcat_path()
                    logging.debug("Tomcat path initialized")
                except ValueError as error:
                    logging.warning('Tomcat path not initialized. %s', error)
                except Exception as error:
                        logging.warning(error)

                try:
                    initializedPath_object.initialize_prism_path()
                    logging.debug("Prism path initialized")
                except ValueError as error:
                    logging.warning('Prism path not initialized. %s', error)
                except Exception as error:
                    logging.warning(error)
                    

                if initializedPath_object.is_tomcat or initializedPath_object.is_prsim:
                    is_tomcat = initializedPath_object.is_tomcat
                    is_tomcat_tlog_path = initializedPath_object.is_tomcat_tlog_path
                    is_prism = initializedPath_object.is_prsim
                    is_prism_tlog_path = initializedPath_object.is_prism_tlog_path
                     
                    
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