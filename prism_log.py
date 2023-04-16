import sys
sys.dont_write_bytecode = True
from datetime import datetime
from collections import OrderedDict
import json
import logging
import os
import socket
from input_validation import InputValidation
from process_initializer import Initializer
from outfile_writer import FileWriter

class Main:

    def init(self):
        logging.basicConfig(filename='aggregator.log', filemode='w', format='[%(asctime)s,%(msecs)d]%(pathname)s:(%(lineno)d)-%(levelname)s - %(message)s', datefmt='%y-%m-%d %H:%M:%S', level=logging.DEBUG)
        
        start = datetime.now()
        logging.debug('start of execution time: %s', start)
        
        num_argv = len(sys.argv)
        uid = sys.argv[len(sys.argv) - 1]
        hostname = socket.gethostname()

        # set the output directory path
        output_directory_path = os.path.join(os.getcwd(), 'out')

        # create the output directory if it doesn't exist
        if not os.path.exists(output_directory_path):
            os.makedirs(output_directory_path)

        # set the output directory object
        outputDirectory_object = output_directory_path
        
        validation_object = InputValidation(num_argv, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        validation_object.validate_argument()
        
        fileWriter_object = FileWriter(outputDirectory_object, uid)
        
        if os.path.exists('modified_log4j2.xml'):
            logging.info('removing old modified_log4j2.xml')
            os.remove('modified_log4j2.xml')
        
        if os.path.exists('modified_nlog.config'):
            logging.info('removing old modified_nlog.config')
            os.remove('modified_nlog.config')
        
        if os.path.exists('out/{}_paymentTransactionData.json'.format(hostname)):
            logging.info('out/{}_paymentTransactionData.json'.format(hostname))
            os.remove('out/{}_paymentTransactionData.json'.format(hostname))
        
        if validation_object.is_input_valid:
            file_path = "{}.json".format(hostname)

            # read the file contents
            with open(file_path, 'r') as f:
                data = f.read()
            
            # data = os.path("{}.json".format(hostname)).read_text()
            config = json.loads(data, object_pairs_hook=OrderedDict)
            
            if config:
                logging.info('\n')
                logging.info('Log aggregation for automation started')
                logging.info("*******************************************")
                
                try:
                    validation_object.validate_msisdn()
                    validation_object.validate_date()
                except Exception as error:
                    logging.exception(error)
                
                logging.info('\n')
                
                if validation_object.is_input_valid:
                    initializer_object = Initializer(hostname, outputDirectory_object, config, validation_object, validation_object.log_mode, uid)
                    initializer_object.initialize_process()
            
        else:
            logging.error('Invalid number of argument passed, should be "4" Please refer to the syntax.')
            logging.error('Hence log fetch could not happen.')
            
            
        logging.info('Log aggregation finished.')
        logging.info("**********************************")
        
        end = datetime.now()
        logging.debug('end of execution time: %s', end)
            
        duration = end - start
        logging.debug('Total time taken %s', duration)
        
        #move log to out folder and zip the out folder
        fileWriter_object.log_mover()
        fileWriter_object.zipped_outfile()
        
        
if __name__ == '__main__':
    main_object = Main()
    main_object.init()