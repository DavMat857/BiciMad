import pandas as pd
import ast

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

from math import radians, cos, sqrt

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

def get_distance(movement):
    plug = movement["lat_lon_plug"]
    unplug = movement["lat_lon_unplug"]
    if plug and unplug:
        plug = ast.literal_eval(plug)
        unplug = ast.literal_eval(unplug)
        distance = flat_distance(plug, unplug)
        if distance < 500:
            return 0.0
        return distance
    else:
        return None
    
def get_route_str(row):
    if row["dist_metros"] == 0.0:
        return None
    else:
        origen = row["idplug_station"]
        destino = row["idunplug_station"]
        route_str = str(sorted([origen, destino]))
        return route_str

def obtener_velocidades(ruta_mov, ruta_sta):
    spark = SparkSession.builder.appName("mapaBicimad").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    mov_df = spark.read.json(r"D:\Documentos\bicimad-backup\datos\movimientos\202012_movements.json")
    sta_df = spark.read.json(r"D:\Documentos\bicimad-backup\datos\estaciones\202012_stations.json")

    mov_pdf = mov_df.toPandas()
    mov_pdf = mov_pdf[["_id", "idplug_station", "idunplug_station", "travel_time", "unplug_hourTime"]]

    # Definir una función que transforma una lista de filas en un diccionario
    def rows_to_dict(rows):
        return {row.id: f"'({row.latitude}, {row.longitude})'" for row in rows}

    # Convertir la función en una UDF
    rows_to_dict_udf = udf(rows_to_dict)

    # Aplicar la UDF a la columna 'stations'
    sta_df_m = sta_df.withColumn("stations", rows_to_dict_udf(col("stations")))
    sta_pdf_m = sta_df_m.toPandas()
    sta_pdf_m["stations"] = sta_pdf_m["stations"].apply(lambda d: ast.literal_eval(d.replace("=", ":")))
    id_map = {}
    for d in sta_pdf_m["stations"]:
        id_map.update(d)

    
    mov_pdf["lat_lon_plug"] = mov_pdf["idplug_station"].apply(lambda id: id_map.get(id))
    mov_pdf["lat_lon_unplug"] = mov_pdf["idunplug_station"].apply(lambda id: id_map.get(id))

    mov_pdf["dist_metros"] = mov_pdf.apply(get_distance, axis=1)
    mov_pdf["velocidad_m_s"] = mov_pdf["dist_metros"] / mov_pdf["travel_time"]

    mov_pdf["route"] = mov_pdf.apply(get_route_str , axis=1)

    results = mov_pdf.dropna(subset="route").groupby("route").agg({'velocidad_m_s': ['mean', 'count']}).reset_index()

    min_count = 50
    results = results[results["velocidad_m_s"]["count"] > min_count]

    results_for_plot = dict(zip(results["route"].tolist(), results["velocidad_m_s"]["mean"].tolist()))

    return results_for_plot, id_map