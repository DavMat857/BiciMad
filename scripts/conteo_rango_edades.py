#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pyspark import SparkContext, SparkConf
# import json
import matplotlib.pyplot as plt
from ast import literal_eval

equivalencia_edad = {0: "Edad no determinada", 
                     1: "0 - 16 años",
                     2: "17 - 18 años",
                     3: "19 - 26 años",
                     4: "27 - 40 años",
                     5: "41 - 65 años",
                     6: " > 66 años"}

def grafica_queso(claves, valores, nombre, ruta):
    if not os.path.exists(ruta): #comprobamos a ver si existe la carpeta "resultados", sino, la creamos
        os.makedirs(ruta)
    direccion = os.path.join(ruta, nombre) #dirección donde se va a guardar el gráfico
    fig, ax = plt.subplots()
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
    ax.pie(valores, labels = claves, colors = colors, startangle=90)
    ax.set_title("GRÁFICO DE SECTORES MOSTRANDO USO POR RANGO DE EDAD.")
    ax.axis('equal') #hacemos que el gráfico sea un círculo
    total, lst_leyenda = sum(valores), []
    for clave, valor in zip(claves, valores):
        porcentaje = round((valor/total) * 100, 2) #cogemos el porcentaje y lo rodeamos a 2 decimales
        lst_leyenda.append(str(clave) + ": " + equivalencia_edad[clave] + " con porcentaje " + str(porcentaje) + "%")
    ax.legend(lst_leyenda, loc="lower right", bbox_to_anchor=(1.13, -0.1))
    fig.savefig(direccion)
    plt.close(fig)
    print("El gráfico {0} se ha guardado correctamente en la siguiente ruta: {1}".format(nombre, direccion))


def obtener_contenido(fila_json):
    # print(fila_json))
    # print(type(fila_json))
    if fila_json == "\n" or fila_json == "" or fila_json == " ":
        return None
    dict_row = literal_eval(fila_json) #convertimos un string (con forma de diccionario) a diccionario
    # print(type(dict_row))
    # print(dict_row.keys())
    lista_claves = dict_row.keys()
    if 'ageRange' in lista_claves and 'travel_time' in lista_claves: #travel time representa los segundos que ha durado el viaje, 
                                                                     #desde el momento en la que se desengancha la bici hasta que se vuelve a enganchar
        #print(dict_row['ageRange'])
        return dict_row['ageRange'], dict_row['travel_time'] #par (rango_edad, tiempo de uso)
    else:
        return None #si uno de los 2 valores no se encuentra, devolvemos None por defecto

def main2(sc, files):
    plt.ioff()

    rdd_contenido = sc.emptyRDD()
    for file in files:
        file_rdd = sc.textFile(file) #leemos el archivo
        contenido_rdd = file_rdd.map(obtener_contenido)
        
        contenido_filtrado_1_rdd = contenido_rdd.filter(lambda x: x!= None) #obtenemos el ageRange de cada archivo y eliminamos todos aquellos que nos hayan salido 'None'
        
        contenido_filtrado_2_rdd = contenido_filtrado_1_rdd.filter(lambda par: par[1] >= 60) #filtramos todos aquellos viajes de manera que su duración es superior al minuto
        
        rdd_contenido = rdd_contenido.union(contenido_filtrado_2_rdd) #unimos los rdd`s para tener 1 único rdd de pares
    
    #veamos un gráfico del rango de edad para poder sacar conclusiones: 
    ageRange_rdd = rdd_contenido.map(lambda par: par[0])
    ageRange_conteo_rdd = ageRange_rdd.countByValue()
    
    # print(ageRange_conteo)
    datos_dict = dict(ageRange_conteo_rdd) #y lo convertimos en diccionario
    datos_ordenados = dict(sorted(datos_dict.items()))
    
    #para ver visualmente los datos obtenidos, mostramos un gráfico
    lista_claves = list(datos_ordenados.keys())
    lista_valores = list(datos_ordenados.values())
    
    grafica_queso(lista_claves, lista_valores, "comparacion_rango_edades.png", "resultados") #creamos una gráfica de sectores
    
    #eliminamos la "Edad no determinada" para poder obtener información más relevante
    claves_actualizadas = lista_claves[1:]
    valores_actualizados = lista_valores[1:]
    grafica_queso(claves_actualizadas, valores_actualizados, "comparacion_rango_edades_filtrado.png", "resultados")
    
    # calculemos ahora el tiempo del viaje:
    
    time_rdd = rdd_contenido.map(lambda par: par[1])
    # print(time_rdd.collect())
    num_viajes = sum(lista_valores)
    suma = time_rdd.sum()
    media = round(suma / num_viajes, 3)
    print("La media de duración de los viajes (redondeada a 3 decimales) es: " + str(media) + " segundos.")

if __name__ == "__main__": #admite todas las bases de datos que le queramos introducir, no solamente 1
    long = len(sys.argv)
    if long == 1 or long == 2:
        print("Uso: python3 {0} ruta/de/archivos <filename_1.json> <filename_2.json> ... <filename_n.json>".format(sys.argv[0]))
    else:
        conf = SparkConf().setAppName("analisis base de datos -> rango edad y tiempo medio")
        with SparkContext(conf=conf) as sc:
            ruta = sys.argv[1]
            sc.setLogLevel("ERROR")
            if ruta != "actual":
                lst = [ruta + "/" + sys.argv[i] for i in range(2,long)] #lista con la dirección de cada archivo
            else:
                lst = [sys.argv[i] for i in range(2,long)]
            main2(sc, lst)
