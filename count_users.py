import json
from pyspark import SparkContext, SparkConf
import sys

def columnas(line):
    data = json.loads(line)
    columns_list = ['user_type']
    returned_list = []
    for column in columns_list:
        returned_list.append(data[column])
    return returned_list

def main(sc, infile, outfile):
    rdd_base = sc.textFile(infile)
    users_count = rdd_base.map(columnas).count()
    with open(outfile, 'w') as o:
        o.write("El número de usuarios registrados en " + infile + " es " + str(users_count))

if __name__ == '__main__':
    if len(sys.argv) == 3:
        conf = SparkConf().setAppName("contadorUsuarios")
        with SparkContext(conf=conf) as sc:
            sc.setLogLevel("ERROR")
            infile = sys.argv[1]
            outfile = sys.argv[2]
            main(sc, infile, outfile)
    else:
        print("Uso: python3 {0} <input_file> <output_file>".format(sys.argv[0]))
