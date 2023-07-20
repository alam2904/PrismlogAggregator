import shutil
import sys
sys.dont_write_bytecode = True
from datetime import datetime
import traceback
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
        logging.debug('start of execution time for QA: %s', start)
        
        num_argv = len(sys.argv)
        uid = sys.argv[len(sys.argv) - 1]
        hostname = socket.gethostname()

        # set the output directory path
        output_directory_path = os.path.join(os.getcwd(), 'out')

        # create the output directory if it doesn't exist
        if not os.path.exists(output_directory_path):
            os.makedirs(output_directory_path)
        else:
            # Iterate over the directory and its subdirectories
            for root, dirs, files in os.walk(output_directory_path, topdown=False):
                # Remove all files in the current directory
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    os.remove(file_path)
                # Remove all subdirectories
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    shutil.rmtree(dir_path)


        # set the output directory object
        outputDirectory_object = output_directory_path
        fileWriter_object = FileWriter(outputDirectory_object, uid)
        
        try:
            msisdn, operator_id, start_date, end_date, log_mode = sys.argv[1].split("|")
            
            validation_object = InputValidation(num_argv, msisdn, operator_id, start_date, end_date, log_mode)
            validation_object.validate_argument()        
            
            if validation_object.is_input_valid:
                if os.path.exists('modified_log4j2.xml'):
                    logging.info('removing old modified_log4j2.xml')
                    os.remove('modified_log4j2.xml')
                
                if os.path.exists('modified_nlog.config'):
                    logging.info('removing old modified_nlog.config')
                    os.remove('modified_nlog.config')
                
                if os.path.exists('out/{}_prismTransactionData.json'.format(hostname)):
                    logging.info('out/{}_prismTransactionData.json'.format(hostname))
                    os.remove('out/{}_prismTransactionData.json'.format(hostname))
                
                file_path = "{}.json".format(hostname)

                # read the file contents
                with open(file_path, 'r') as f:
                    data = f.read()
                
                config = json.loads(data, object_pairs_hook=OrderedDict)
                
                if config:
                    logging.info('\n')
                    logging.info('Log aggregation for automation started')
                    logging.info("*******************************************")                    
                    logging.info('\n')
                    
                    initializer_object = Initializer(hostname, outputDirectory_object, config, validation_object, validation_object.log_mode, uid)
                    initializer_object.initialize_process()
            else:
                logging.error('input validation failed. Hence log fetch could not happen.')
                logging.error('check aggregator.log for the reason.')
            
        except Exception as error:
            logging.error(traceback.format_exc())
            
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