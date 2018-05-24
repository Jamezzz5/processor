import logging
import argparse
import sys
import reporting.vendormatrix as vm
import reporting.dictionary as dct
import reporting.importhandler as ih
import reporting.calc as cal
import reporting.export as exp

formatter = logging.Formatter('%(asctime)s [%(module)14s]' +
                              '[%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
log.addHandler(console)

log_file = logging.FileHandler('logfile.log', mode='w')
log_file.setFormatter(formatter)
log.addHandler(log_file)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception

parser = argparse.ArgumentParser()
parser.add_argument('--api', choices=['all', 'fb', 'aw', 'tw', 'ttd', 'ga',
                                      'nb', 'af', 'sc', 'aj', 'dc', 'rs'])
parser.add_argument('--ftp', choices=['all', 'sz'])
parser.add_argument('--dbi', choices=['all', 'dna'])
parser.add_argument('--s3', choices=['all', 'dna'])
parser.add_argument('--noprocess', action='store_true')
parser.add_argument('--update', choices=['all', 'vm', 'dct'])
parser.add_argument('--exp', choices=['all', 'db', 'ftp'])
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
    if args.dbi:
        dbi = ih.ImportHandler(args.dbi, matrix)
        dbi.db_loop()
    if args.s3:
        s3 = ih.ImportHandler(args.s3, matrix)
        s3.s3_loop()
    if not args.noprocess:
        df = matrix.vm_loop()
        df = cal.calculate_cost(df)
        try:
            logging.info('Writing to: ' + OUTPUT_FILE)
            df.to_csv(OUTPUT_FILE, index=False)
            logging.info('Final Output Successfully generated')
        except IOError:
            logging.warning(OUTPUT_FILE + ' could not be opened.  ' +
                            'Final Output not updated.')
    if args.exp:
        exp_class = exp.ExportHandler()
        exp_class.export_loop(args.exp)


if __name__ == '__main__':
    main()
