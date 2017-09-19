import logging
import argparse
import sys
import reporting.vendormatrix as vm
import reporting.dictionary as dct
import reporting.importhandler as ih
import reporting.calc as cal
import reporting.export as exp
import reporting.expcolumns as exc

logging.basicConfig(stream=sys.stdout,
                    filename='logfile.log',
                    filemode='w',
                    level=logging.INFO,
                    disable_existing_loggers=False,
                    format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
console = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(module)14s]' +
                              '[%(levelname)8s] %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

parser = argparse.ArgumentParser()
parser.add_argument('--api', choices=['all', 'fb', 'aw', 'tw', 'ttd', 'ga'])
parser.add_argument('--ftp', choices=['all', 'sz'])
parser.add_argument('--noprocess', action='store_true')
parser.add_argument('--update', choices=['all', 'vm', 'dct'])
parser.add_argument('--db', action='store_true')
args = parser.parse_args()

OUTPUT_FILE = 'Raw Data Output.csv'


def main():
    if args.update == 'all' or args.update == 'vm':
        vm.vm_update()
    if args.update == 'all' or args.update == 'dct':
        dct.dict_update()
    matrix = vm.VendorMatrix()
    if args.api:
        api = ih.ImportHandler(args.api, matrix)
        api.api_loop()
    if args.ftp:
        ftp = ih.ImportHandler(args.ftp, matrix)
        ftp.ftp_loop()
    if not args.noprocess:
        df = matrix.vm_loop()
        df = cal.calculate_cost(df)
        try:
            df.to_csv(OUTPUT_FILE, index=False)
            logging.info('Final Output Successfully generated')
        except IOError:
            logging.warn(OUTPUT_FILE + ' could not be opened.  ' +
                         'Final Output not updated.')
    if args.db:
        dbu = exp.DBUpload()
        dbu.upload_to_db(exc.db_config_file, exc.db_schema_file,
                         exc.db_translation_file, OUTPUT_FILE)

if __name__ == '__main__':
    main()
