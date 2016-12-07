import logging
import reporting.calc as cal
import reporting.vendormatrix as vm
import reporting.dictionary as dct

logging.basicConfig(format=('%(asctime)s [%(module)14s]' +
                            '[%(levelname)8s] %(message)s'))
log = logging.getLogger()
log.setLevel(logging.INFO)

OUTPUT_FILE = 'Raw Data Output.csv'


def main():
    matrix = vm.VendorMatrix()
    data = matrix.vmloop()
    data = cal.netcost_calculation(data)
    data = cal.netcostfinal_calculation(data, matrix.vm[dct.FPN][vm.plankey])
    try:
        data.to_csv(OUTPUT_FILE, index=False)
        logging.info('Final Output Successfully generated')
    except IOError:
        logging.info(OUTPUT_FILE + ' could not be opened.  ' +
                     'Final Output not updated.')

if __name__ == '__main__':
    main()
