from pyspark.sql import functions as F
from pyspark.sql import types as T

import matplotlib.pyplot as plt
import numpy as np
import ast

from math import radians, cos, sqrt


# Función para calcular la distancia entre dos puntos en la superficie de la Tierra
def flat_distance(p_d, q_d):
    # Conversión de coordenadas de grados a radianes
    p = tuple(map(radians, p_d))
    q = tuple(map(radians, q_d))

    # Radio de la Tierra en Madrid en metros
    R = 6369160

    # Distancia en latitud y longitud en radianes
    dlat = q[0] - p[0]
    dlon = q[1] - p[1]

    x = dlon * cos(0.5 * (p[0] + q[0])) 
    y = dlat

    # Distancia euclidiana
    distance = R * sqrt(x**2 + y**2)

    return distance


# Función para obtener la distancia entre dos puntos
def get_distance(plug, unplug):
    plug_tuple = ast.literal_eval(plug)
    unplug_tuple = ast.literal_eval(unplug)
    if None in plug_tuple or None in unplug_tuple:
        return None
    else:
        distance = flat_distance(plug_tuple, unplug_tuple)
        if distance < 500:
            return 0.0
        return distance


# Función para agregar la columna de distancia en metros
def add_dist_metros(movements_df, id_map):
    # Registramos la función como una función definida por el usuario (UDF) en PySpark
    get_distance_udf = F.udf(get_distance)

    # Seleccionamos las columnas que nos interesan
    output_movements_df = movements_df.select(["_id", "idplug_station", "idunplug_station", "travel_time"])

    # Creamos una nueva columna "lat_lon_plug" y "lat_lon_unplug"
    get_lat_long = F.udf(lambda id: id_map.get(id, {}).get("lat_long", "(None, None)"))
    output_movements_df = output_movements_df.withColumn("lat_lon_plug", get_lat_long(F.col("idplug_station")))
    output_movements_df = output_movements_df.withColumn("lat_lon_unplug", get_lat_long(F.col("idunplug_station")))

    # Creamos una nueva columna "dist_metros" aplicando la UDF get_distance_udf
    output_movements_df = output_movements_df.withColumn("dist_metros", get_distance_udf(F.col("lat_lon_plug"), F.col("lat_lon_unplug")))

    return output_movements_df


# Función para obtener un mapa de identificadores
def get_id_map(stations_df):
    # Definir una función que transforma una lista de filas en un diccionario
    fix_name = lambda name: name.replace("'", "")
    def rows_to_dict(rows):
        row_info = {}

        for row in rows:
            try:
                latitude = float(row.latitude)
                longitude = float(row.longitude)
            except ValueError:
                latitude = None
                longitude = None

            row_info[row.id] = {"'lat_long'": f"'({latitude}, {longitude})'", "'name'": f"'{fix_name(row.name)}'"}

        return row_info

    # Convertir la función en una UDF
    rows_to_dict_udf = F.udf(rows_to_dict, T.MapType(T.StringType(), T.StringType()))

    # Aplicar la UDF a la columna 'stations'
    stations_df_mod = stations_df.withColumn("stations", rows_to_dict_udf(F.col("stations")))

    # Obtenemos la información necesario mediante un explode
    id_map = stations_df_mod.select(F.explode("stations")).groupBy("key").agg(F.first("value").alias("value")).collect()

    # Convertimos los resultados a un diccionario de Python
    id_map = {ast.literal_eval(row['key']): ast.literal_eval(row['value'].replace("=", ":")) for row in id_map}

    return id_map


# Función para obtener una cadena de ruta
def get_route_str(dist_metros, idplug, idunplug):
    if dist_metros == 0.0:
        return None
    else:
        route_str = "-".join([str(id) for id in sorted([idplug, idunplug])])
        return route_str


# Mapea el código de la ruta el nombre real de las estaciones
def map_ruta(ruta, id_map):
    estacion_a, estacion_b = ruta.split("-")
    nombre_a = id_map[int(estacion_a)]["name"]
    nombre_b = id_map[int(estacion_b)]["name"]
    return "-".join([nombre_a, nombre_b])


# Mostrar resultados del diccionario
def plot_results(rutas_lentas, ruta_resultados, fecha='2020'):
    # Ahora vamos a mostrar los resultados
    keys = list(rutas_lentas.keys())
    values = list(rutas_lentas.values())

    colors = plt.cm.winter(np.linspace(0, 1, len(keys)))

    # Crea las barras horizontales
    plt.barh(keys, values, color=colors)

    # Etiquetas y título
    plt.xlabel('Velocidad Media (m/s)')
    plt.ylabel('Rutas')
    plt.title(f'Rutas más lentas {fecha}')
    plt.gca().invert_yaxis()

    plt.savefig(ruta_resultados, bbox_inches='tight')
    print(f"El gráfico se ha guardado correctamente en la siguiente ruta: {ruta_resultados}")


# Función principal para obtener las velocidades de las rutas
def obtener_velocidades(ruta_movements, ruta_stations, ruta_resultados, spark_session, min_count=50, top_n=10):

    # Leemos los datos de las estaciones y los movimientos
    stations_df = spark_session.read.json(ruta_stations)
    movements_df = spark_session.read.json(ruta_movements)

    # Obtenemos el mapa de identificadores
    id_map = get_id_map(stations_df)

    # Agregamos la columna de distancia en metros
    movements_df = add_dist_metros(movements_df, id_map)
    
    # Creamos una nueva columna "velocidad_m_s" dividiendo "dist_metros" por "travel_time"
    movements_df = movements_df.withColumn("velocidad_m_s", F.col("dist_metros") / F.col("travel_time"))

    # Creamos una nueva columna "ruta" aplicando la UDF get_route_str
    get_route_str_udf = F.udf(get_route_str)
    movements_df = movements_df.withColumn("ruta", get_route_str_udf(F.col("dist_metros"), F.col("idplug_station"), F.col("idunplug_station")))

    # Agrupar por "id" y calcular la media y el conteo
    results = movements_df.groupBy("ruta").agg(F.mean('velocidad_m_s').alias('mean'), F.count('velocidad_m_s').alias('count'))

    # Filtramos los resultados donde el conteo es mayor que 'min_count' y media distinto de 0
    results = results.filter(F.col("count") > min_count)
    results = results.filter(F.col("mean") > 0.0)

    # Ordenamos los resultamos por velocidad media
    results_orden = results.orderBy(F.col("mean"))

    # Seleccionamos las 'top_n' rutas más lentas y más rapidas del mes
    rutas_lentas = {map_ruta(row['ruta'], id_map): row['mean'] for row in results_orden.limit(top_n).collect()}

    # Ahora vamos a mostrar los resultados obtenidos
    plot_results(rutas_lentas, ruta_resultados)


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import sys

    spark_session = SparkSession.builder.appName("RutasLentas").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    argv = sys.argv

    if len(argv) < 3:
        
        print("Uso: python3 rutas_lentas.py <ruta_movements> <ruta_stations> <ruta_resultados>")

        # Valores por defecto
        ruta_movements = ["202012_movements.json","202011_movements.json","202010_movements.json","202009_movements.json","202008_movements.json","202007_movements.json","202006_movements.json"]
        ruta_stations = ["202012_stations.json","202011_stations.json","202010_stations.json","202009_stations.json","202008_stations.json","202007_stations.json","202006_stations.json"]
        ruta_resultados = "rutas_lentas.png"
    
    else:
        ruta_movements, ruta_stations = [], []
        for i in range(1,len(sys.argv)-1):
        	if sys.argv[i].endswith("movements.json"):
        		ruta_movements += [sys.argv[i]]
        	elif sys.argv[i].endswith("stations.json"):
        		ruta_stations += [sys.argv[i]]
        ruta_resultados = argv[-1]

    obtener_velocidades(ruta_movements, ruta_stations, ruta_resultados, spark_session, min_count=50, top_n=10)
