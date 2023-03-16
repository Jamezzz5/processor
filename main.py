import os
import sys
import logging
import argparse
import pandas as pd
import reporting.utils as utl
import reporting.export as exp
import reporting.analyze as az
import reporting.tbapi as tbapi
import reporting.vmcolumns as vmc
import reporting.expcolumns as exc
import reporting.dictionary as dct
import reporting.vendormatrix as vm
import reporting.importhandler as ih


def set_log():
    formatter = logging.Formatter('%(asctime)s [%(module)14s]'
                                  '[%(levelname)8s] %(message)s')
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    log.addHandler(console)

    try:
        log_file = logging.FileHandler('logfile.log', mode='w')
        log_file.setFormatter(formatter)
        log.addHandler(log_file)
    except PermissionError as e:
        logging.warning('Could not open logfile with error: \n {}'.format(e))


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception


def get_args(arguments=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--api', choices=[
        'all', 'fb', 'aw', 'tw', 'ttd', 'ga', 'nb', 'af', 'sc', 'aj', 'dc',
        'rs', 'db', 'vk', 'rc', 'szk', 'red', 'dv', 'adk', 'inn', 'tik', 'amz',
        'cri', 'pm', 'sam', 'gs', 'qt', 'yv', 'amd', 'ss'])
    parser.add_argument('--ftp', choices=['all', 'sz'])
    parser.add_argument('--dbi', choices=['all', 'dna'])
    parser.add_argument('--s3', choices=['all', 'dna'])
    parser.add_argument('--noprocess', action='store_true')
    parser.add_argument('--analyze', action='store_true')
    parser.add_argument('--update', choices=['all', 'vm', 'dct'])
    parser.add_argument('--exp', choices=['all', 'db', 'ftp'])
    parser.add_argument('--tab', action='store_true')
    parser.add_argument('--basic', action='store_true')
    parser.add_argument('--nolog', action='store_true')
    if arguments:
        args, unknown = parser.parse_known_args(arguments.split())
    else:
        args, unknown = parser.parse_known_args()
    return args


OUTPUT_FILE = vmc.output_file


def main(arguments=None):
    args = get_args(arguments)
    if not args.nolog:
        set_log()
    if args.update == 'all' or args.update == 'vm':
        vm.vm_update()
    if args.update == 'all' or args.update == 'dct':
        dct.dict_update()
    df = pd.DataFrame()
    matrix = vm.VendorMatrix()
    if args.analyze:
        aly = az.Analyze(df=df, file_name=OUTPUT_FILE, matrix=matrix)
        aly.do_analysis_and_fix_processor(pre_run=True)
        matrix = vm.VendorMatrix()
    if args.api:
        api = ih.ImportHandler(args.api, matrix)
        api.api_loop()
    if args.ftp:
        ftp = ih.ImportHandler(args.ftp, matrix)
        ftp.ftp_loop()
    if args.dbi:
        dbi = ih.ImportHandler(args.dbi, matrix)
        dbi.db_loop()
    if args.s3:
        s3 = ih.ImportHandler(args.s3, matrix)
        s3.s3_loop()
    if not args.noprocess:
        df = matrix.vm_loop_with_costs(OUTPUT_FILE)
        if args.analyze and not os.path.isfile(
                os.path.join(utl.config_path, exc.upload_id_file)):
            logging.info('First run - analyzing data.')
            aly = az.Analyze(df=df, file_name=OUTPUT_FILE, matrix=matrix)
            fixes_to_run = aly.do_analysis_and_fix_processor()
            if fixes_to_run:
                logging.info('Fixes applied, rerunning processor.')
                matrix = vm.VendorMatrix()
                df = matrix.vm_loop_with_costs(OUTPUT_FILE)
    if args.exp:
        exp_class = exp.ExportHandler()
        exp_class.export_loop(args.exp)
    if args.tab:
        tb = tbapi.TabApi()
        tb.refresh_extract()
    if args.analyze:
        aly = az.Analyze(df=df, file_name=OUTPUT_FILE, matrix=matrix)
        aly.do_all_analysis()


if __name__ == '__main__':
    main()
