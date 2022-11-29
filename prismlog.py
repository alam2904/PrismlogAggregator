#!/usr/local/bin/python3.6
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
        
        num_argv = len(sys.argv)
        logging.debug('Number of arguments passed is %s', num_argv - 1)
        
        if num_argv == 3 or num_argv == 4:
            if num_argv == 4:
                logging.debug('Arguments passed are : msisdn=%s, search_date=%s and automation=%s', sys.argv[1], sys.argv[2], sys.argv[3])
            else:
                logging.debug('Arguments passed are : msisdn=%s and search_date=%s', sys.argv[1], sys.argv[2])
            
            #default retention period.
            r_period = 3
            file = Path('config.properties')
            if file.exists():
                config = ConfigParser()
                config.read(file)
                if config.has_option('outdata_retention_period', 'retention'):
                    r_period = int(config['outdata_retention_period']['retention'])
                    logging.info('out file retention period: %s', r_period)
                    bdt = datetime.today() - timedelta(days=r_period)
                    back_date = str(datetime.strftime(bdt, "%Y-%m-%d")).replace("-", "")
                    logging.info('back date: %s', back_date)
                else:
                    logging.info('data rentention in config.properties not defined. Default is 3 days.')
                    bdt = datetime.today() - timedelta(days=r_period)
                    back_date = datetime.strftime(bdt, "%Y-%m-%d")
                    logging.info('back date: %s', back_date)
                
                if num_argv == 4:
                    outputDirectory_object = Path('out')
                    try:
                        outputDirectory_object.mkdir(exist_ok=False)
                    except FileExistsError as error:
                        logging.info('out directory already exists.')
                    
                    outputDirectory_object = Path('out/prism_auto_log')
                    try:
                        outputDirectory_object.mkdir(exist_ok=False)
                    except FileExistsError as error:
                        logging.info('out/automation directory already exists. Going to fetch %s dated files and remove.', back_date)
                    
                    logging.info('TLog aggregation for automation started')
                    logging.info("*******************************************")
                    
                else:
                    outputDirectory_object = Path('out')
                    try:
                        outputDirectory_object.mkdir(exist_ok=False)
                    except FileExistsError as error:
                        logging.info('out directory already exists.')
                    
                    outputDirectory_object = Path('out/prism_agg_log')
                    try:
                        outputDirectory_object.mkdir(exist_ok=False)
                    except FileExistsError as error:
                        logging.info('out directory already exists. Going to fetch %s dated files and remove.', back_date)
                
                    logging.info('Log aggregation started')
                    logging.info("******************************")
                    
                self.remove_backdated_files(outputDirectory_object, back_date)

                validation_object = InputValidation(sys.argv[1], sys.argv[2])
                
                try:
                    if num_argv == 4:
                        fmsisdn = self.validate_input(validation_object, num_argv)
                    else:
                        fmsisdn, input_date = self.validate_input(validation_object, num_argv)
                except Exception as error:
                    logging.exception(error)
                msisdn = sys.argv[1]
                
                logging.info('\n')
                
                if validation_object.is_input_valid:
                    initializedPath_object = LogPathFinder(config)
                    
                    if config.has_option('tomcat', 'TRANS_BASE_DIR'):
                        if config['tomcat']['TRANS_BASE_DIR']:
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
                    else:
                        logging.error('tomcat TRANS_BASE_DIR path not present in config.properties')
                            
                    logging.info('\n')
                            
                    if config.has_option('prismd', 'TRANS_BASE_DIR'):
                        if config['prismd']['TRANS_BASE_DIR']:
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
                    else:
                        logging.error('prismd TRANS_BASE_DIR path not present in config.properties')
                                
                    logging.info('\n')
                    if not num_argv == 4:
                        if config.has_option('smsd', 'TRANS_BASE_DIR'):
                            if config['smsd']['TRANS_BASE_DIR']:
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
                        else:
                            logging.error('prismd TRANS_BASE_DIR path not present in config.properties')
                                
                    logging.info('\n')

                    if initializedPath_object.is_tomcat_tlog_path or initializedPath_object.is_prism_tlog_path or initializedPath_object.is_sms_tlog_path:
                        is_tomcat_tlog_path = initializedPath_object.is_tomcat_tlog_path
                        is_prism_tlog_path = initializedPath_object.is_prism_tlog_path
                        is_sms_tlog_path = initializedPath_object.is_sms_tlog_path
                    
                        if num_argv == 4:          
                            processor_object = PROCESSOR(msisdn, fmsisdn, None, outputDirectory_object, file, validation_object)
                            processor_object.process_automation(is_tomcat_tlog_path, is_prism_tlog_path, initializedPath_object)
                        else:
                            processor_object = PROCESSOR(msisdn, fmsisdn, input_date, outputDirectory_object, file, validation_object)
                            processor_object.process(is_tomcat_tlog_path, is_prism_tlog_path, is_sms_tlog_path, initializedPath_object)
                    else:
                        logging.error('Transaction log path initialization failed.')

                        logging.info('Log aggregation finished.')
                        logging.info("**********************************")
                else:
                    logging.error('Invalid input. Log aggregation failed to process.')
                    logging.info("****************************************************")
            else:
                logging.error('config.properties file does not exists. Hence process failed')
        else:
            logging.error('Invalid number of arguments passed.')
            logging.debug('Log aggregation failed to process.')
            logging.info("******************************************")

        end = time.time()
        logging.debug(end)
            
        duration = end - start
        logging.debug('Total time taken %s', duration)
            
        if Path('log_aggregator.log').exists():
            shutil.move('log_aggregator.log', 'out/log_aggregator.log')
    
    def remove_backdated_files(self, outputDirectory_object, back_date):
        outfiles = [p for p in outputDirectory_object.glob(f"*_{back_date}_*.*")]
        if bool(outfiles):
            for file in outfiles:
                if os.path.isfile(file):
                    os.remove(file)
                    logging.info('back dated files removed: %s', file)
                else:
                    logging.info('back dated file does not exists: %s', file)
        else:
            logging.info('back dated file does not exists')
    
    def validate_input(self, validation_object, cmd_argv):
        try:
            fmsisdn = validation_object.validate_msisdn()
            if cmd_argv == 3:
                input_date = validation_object.validate_date()
                return (fmsisdn, input_date)
            else:
                try:
                    validation_object.validate_timedtdata(sys.argv[2], sys.argv[3])
                    return (fmsisdn)
                except Exception as error:
                    raise
        except Exception as error:
            raise
                            
if __name__ == '__main__':
    main_object = Main()
    main_object.init()