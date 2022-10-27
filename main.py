"""
importing modules
"""
import sys
import time
import logging
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
            validation = InputValidation(sys.argv[1], sys.argv[2])

            try:
                msisdn = validation.validate_msisdn()
                input_date = validation.validate_date()
            except Exception as error:
                logging.exception(error)

            if validation.is_input_valid:
                initializePath = LogPathFinder()
                try:
                    initializePath.initialize_tomcat_path()
                    logging.debug("Tomcat path initialized")
                except ValueError as error:
                    logging.warning('Tomcat path not initialized.Tomcat daemon not running. %s', error)
                except Exception as error:
                        logging.warning(error)

                try:
                    initializePath.initialize_prism_path()
                    logging.debug("Prism path initialized")
                except ValueError as error:
                    logging.warning('Prism path not initialized.PrismD daemon not running. %s', error)
                except Exception as error:
                    logging.warning(error)

                proc = PROCESSOR(msisdn, input_date)
                proc.process()
                logging.info('Log aggregation finished')
                logging.info("**********************************")
                
            else:
                logging.debug('Invalid input. Log aggregation failed to process')
                logging.info("**********************************")

        else:
            logging.debug('Invalid number of arguments passed')
            logging.debug('Log aggregation failed to process')
            logging.info("**********************************")

        end = time.time()
        logging.debug(end)
        
        duration = end - start
        logging.debug('Total time taken %s', duration)


if __name__ == '__main__':
    main = Main()
    main.init()