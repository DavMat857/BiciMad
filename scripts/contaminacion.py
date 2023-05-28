from pyspark.sql.functions import sum
from pyspark.sql import functions as F
from pyspark.sql import types as T


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


def distancia_recorrida(ruta_movements, ruta_stations, spark_session):
    # Obtenemos la fecha de los datos
    fecha_movements = ''.join(c for c in ruta_movements if c.isdigit())
    fecha_stations = ''.join(c for c in ruta_stations if c.isdigit())
    if fecha_stations == fecha_movements:
        fecha = f"{fecha_movements[:4]}/{fecha_movements[4:]}"
        print(f"Trabajaremos con los datos de {fecha}")
    else:
        print("AVISO: Las fechas de los datos proporcionados no coinciden, puede causar problemas")

    # Leemos los datos de las estaciones y los movimientos
    stations_df = spark_session.read.json(ruta_stations)
    movements_df = spark_session.read.json(ruta_movements)

    # Obtenemos el mapa de identificadores
    id_map = get_id_map(stations_df)

    # Agregamos la columna de distancia en metros
    movements_df = add_dist_metros(movements_df, id_map)

    # Obtenemos la distancia total
    total_distancia = movements_df.agg(sum("dist_metros")).collect()[0][0]

    return total_distancia


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import sys

    spark_session = SparkSession.builder.appName("distancia_recorrida").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    argv = sys.argv

    if len(argv) < 2:
        
        print("Uso: python3 rutas_lentas.py <ruta_movements> <ruta_stations> ")

        # Valores por defecto
        ruta_movements = "datos/movements/202012_movements.json"
        ruta_stations = "datos/stations/202012_stations.json"
    
    else:
        ruta_movements = argv[0]
        ruta_stations = argv[1]

    distancia_recorrida(ruta_movements, ruta_stations, spark_session)