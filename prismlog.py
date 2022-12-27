#!/usr/local/bin/python3.6
"""
importing modules
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import shutil
import os
import zipfile
from input_validation import InputValidation
from path_initializer import LogPathFinder
from log_processor import PROCESSOR
from configparser import ConfigParser
from zipfile import ZipFile

class Main:

    def init(self):
        logging.basicConfig(filename='log_aggregator.log', filemode='w', format='[%(asctime)s,%(msecs)d]%(pathname)s:(%(lineno)d)-%(levelname)s - %(message)s', datefmt='%y-%m-%d %H:%M:%S', level=logging.DEBUG)
        
        start = datetime.now()
        logging.debug('start of execution time: %s', start)
        
        num_argv = len(sys.argv)
        logging.debug('Number of arguments passed is %s', num_argv - 1)
        
        if num_argv == 4 or num_argv == 5:
            if num_argv == 4:
                logging.debug('Arguments passed are - msisdn:%s and automattion_log:%s', sys.argv[1], sys.argv[2])
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
                            processor_object = PROCESSOR(msisdn, fmsisdn, None, outputDirectory_object, file, validation_object, sys.argv[3])
                            processor_object.process_automation(is_tomcat_tlog_path, is_prism_tlog_path, initializedPath_object)
                        else:
                            processor_object = PROCESSOR(msisdn, fmsisdn, input_date, outputDirectory_object, file, validation_object, sys.argv[4])
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
            
        #move log_aggregator.log from current directory to respective directory.
        log = outputDirectory_object/"log_aggregator.log"
        if Path('log_aggregator.log').exists():
            try:
                for path in Path(outputDirectory_object).rglob(f"*_log_aggregator.log"):
                    if path:
                        logging.info('*_log_aggregator.log file already exists. Hence removing the old log files and copying the new to the path.')
                        os.remove(path)
                if num_argv == 5:
                    shutil.move('log_aggregator.log', f'{outputDirectory_object}/{sys.argv[4]}_log_aggregator.log')
                elif num_argv == 4:
                    shutil.move('log_aggregator.log', f'{outputDirectory_object}/{sys.argv[3]}_log_aggregator.log')
            except Exception as error:
                if os.path.isfile(outputDirectory_object/"log_aggregator.log"):
                    logging.info('log_aggregator.log file already exists. Hence removing and copying it.')
                    os.remove(log)
                shutil.move('log_aggregator.log', f'{outputDirectory_object}/')
        

        logging.info('out directory: %s', outputDirectory_object)
        
        end = datetime.now()
        logging.debug('end of execution time: %s', end)
            
        duration = end.timestamp() - start.timestamp()
        logging.debug('Total time taken %s', duration)
        
        if num_argv == 5:
            out_zipFile = f"{sys.argv[4]}_{Path('outfile.zip')}"
            with ZipFile(out_zipFile, "a", compression= zipfile.ZIP_DEFLATED) as zip:
                for path in Path(outputDirectory_object).rglob(f"{sys.argv[4]}_*.*"):
                    zip.write(path)
            print(f"OARM_OUTPUT_FILENAME|{Path(out_zipFile).absolute()}")
                
        elif num_argv == 4:
            out_zipFile = f"{sys.argv[3]}_{Path('outfile.zip')}"
            with ZipFile(out_zipFile, "a", compression= zipfile.ZIP_DEFLATED) as zip:
                for path in Path(outputDirectory_object).rglob(f"{sys.argv[3]}_*.*"):
                    zip.write(path)
            print(f"OARM_OUTPUT_FILENAME|{Path(out_zipFile).absolute()}")
        
        
    def remove_backdated_files(self, outputDirectory_object, back_date):
        outfiles = [p for p in outputDirectory_object.glob(f"*_{back_date}__*.*")]
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
            if cmd_argv == 5:
                input_date = validation_object.validate_date()
                validation_object.validate_srvkey(sys.argv[3])
                return (fmsisdn, input_date)
            else:
                try:
                    validation_object.validate_timedtdata(sys.argv[2])
                    return (fmsisdn)
                except Exception as error:
                    raise
        except Exception as error:
            raise
                            
if __name__ == '__main__':
    main_object = Main()
    main_object.init()