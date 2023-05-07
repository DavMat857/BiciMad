#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
posibles modificaciones: hacer que pueda leer mas de 1 base de datos para poder tener mas datos recopilados de varios meses
    -> añadir archivos en el 'sys.argv' del __name__ == __main__ 
"""

import sys
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

import sys
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def extract_age_range(row): #se comprueba si 'ageRange' está en la fila que estamos leyendo
    if 'ageRange' in row:
        return row.ageRange
    else:
        return None

def grafica_datos_rango_edades(OX, OY, filename):
    fig, ax = plt.subplots()
    ax.bar(OX, OY)
    ax.set_xlabel("Rango de edades")
    ax.set_ylabel("Número de viajes realizados")
    fig.savefig(filename)

def grafica_queso(claves, valores, filename):
    fig, ax = plt.subplots()
    ax.pie(valores, labels=claves, autopct='%1.1f%%', startangle=90)
    ax.axis('equal') #hacemos que el gráfico sea un círculo
    fig.savefig(filename)


def main(spark, bd):
    data = spark.read.json(bd) # leemos el archivo y creamos un nuevo dataframe con esta información
    data_rdd = data.rdd #a partir del dataframe anterior, creamos un nuevo rdd
    
    # Extrae los valores de la clave "ageRange" utilizando la función auxiliar
    ageRange_rdd = data_rdd.map(extract_age_range).filter(lambda x: x is not None) #cogemos los age_range. Si alguno de ellos tiene el valor None, lo eliminamos
    ageRange_conteo_rdd = ageRange_rdd.countByValue() #contamos cuantos hay de cada rango de edad
    
    datos_dict = dict(ageRange_conteo_rdd) #y lo convertimos en diccionario
    
    datos_ordenados = dict(sorted(datos_dict.items()))
    
    #para ver visualmente los datos obtenidos, mostramos un gráfico
    lista_claves = list(datos_ordenados.keys())
    lista_valores = list(datos_ordenados.values())
    grafica_datos_rango_edades(lista_claves, lista_valores, "gráfico_edades_VS_cantidad_viajes.jpg")
    grafica_queso(lista_claves, lista_valores, "gráfico_queso_viajes.jpg")

    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file.json>".format(sys.argv[0]))
    else:
        bd = sys.argv[1]
        with SparkSession.builder.appName("Análisis base de datos " + bd).getOrCreate() as spark:
            main(spark, bd)
