from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from math import radians, cos, sqrt
import ast


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


def get_route_str(dist_metros, idplug, idunplug):
    if dist_metros == 0.0:
        return None
    else:
        route_str = "-".join([str(id) for id in sorted([idplug, idunplug])])
        return route_str


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


def obtener_velocidades(ruta_movements, ruta_stations, spark_session):

    stations_df = spark_session.read.json(ruta_stations)
    movements_df = spark_session.read.json(ruta_movements)

    id_map = get_id_map(stations_df)

    movements_df = add_dist_metros(movements_df, id_map)
    
    # Creamos una nueva columna "velocidad_m_s" dividiendo "dist_metros" por "travel_time"
    movements_df = movements_df.withColumn("velocidad_m_s", F.col("dist_metros") / F.col("travel_time"))

    # Creamos una nueva columna "ruta" aplicando la UDF get_route_str
    get_route_str_udf = F.udf(get_route_str)
    movements_df = movements_df.withColumn("ruta", get_route_str_udf(F.col("dist_metros"), F.col("idplug_station"), F.col("idunplug_station")))

    # Filtrar los valores None en la columna "precio"
    movements_df_nonull = movements_df.filter(movements_df.velocidad_m_s.isNotNull())
    movements_df_filtrado = movements_df_nonull.filter(F.col("velocidad_m_s") > 0.0)

    # Agrupar por "id" y calcular la media y el conteo
    results = movements_df_filtrado.groupBy("ruta").agg(F.mean('velocidad_m_s').alias('mean'), F.count('velocidad_m_s').alias('count'))

    # Filtramos los resultados donde el conteo es mayor que min_count
    min_count = 50
    results_filtrado = results.filter(F.col("count") > min_count)

    # Creamos un diccionario para el gráfico
    results_dict = {row['ruta']: row['mean'] for row in results_filtrado.collect()}

    return results_dict