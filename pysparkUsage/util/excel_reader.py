__author__ = 'jjzhu'
import csv


def read_excel():
    reader = csv.reader(open('F:\workspace\space4py\workspace\PySparkUsage\data\Wholesale_customers_data.csv'))

    for row_list in reader:
        print(row_list)


def read_file(input_dir, output_path):
    import os
    import platform
    output_file = open(output_path, 'w+')
    if platform.system() == 'Windows':
        deli = '\\'
    elif platform.system() == 'Linux':
        deli = '/'
    else:
        print('unknown platform: %s' % platform.system(), file=sys.stderr)
    for i in os.listdir(input_dir):
        curr_dir = input_dir+deli+i
        for j in os.listdir(curr_dir):
            if j.__contains__('part'):
                with open(curr_dir+deli+j) as f:
                    print(curr_dir+deli+j)

                    for line in f.readlines():
                        print(line[1:len(line)-2])
                        output_file.write(line[1:len(line)-2]+'\n')

    output_file.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: read_file <input_dir> <output_path> ", file=sys.stderr)
        exit(-1)
    read_file(sys.argv[1], sys.argv[2])



