import logging
import argparse
import reporting.vendormatrix as vm
import reporting.apihandler as api
import reporting.calc as cal
import reporting.dictionary as dct


logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('--api', choices=['all', 'fb', 'aw'])
parser.add_argument('--noprocess', action='store_true')
args = parser.parse_args()

OUTPUT_FILE = 'Raw Data Output.csv'


def main():
    matrix = vm.VendorMatrix()
    if args.api:
        api.apicalls(args.api, matrix)
    if not args.noprocess:
        data = matrix.vmloop()
        data = cal.netcost_calculation(data)
        data = cal.netcostfinal_calculation(data,
                                            matrix.vm[dct.FPN][vm.plankey])
        try:
            data.to_csv(OUTPUT_FILE, index=False)
            logging.info('Final Output Successfully generated')
        except IOError:
            logging.info(OUTPUT_FILE + ' could not be opened.  ' +
                         'Final Output not updated.')

if __name__ == '__main__':
    main()
