from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import get_id_map, add_dist_metros


# Función para obtener una cadena de ruta
def get_route_str(dist_metros, idplug, idunplug):
    if dist_metros == 0.0:
        return None
    else:
        route_str = "-".join([str(id) for id in sorted([idplug, idunplug])])
        return route_str


# Función principal para obtener las velocidades de las rutas
def obtener_velocidades(ruta_movements, ruta_stations, spark_session, min_count=50, top_n=10):

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

    # Seleccionamos las 'top_n' rutas las lentas del mes
    top_results = {row['ruta']: row['mean'] for row in results.orderBy(F.col("mean").desc()).limit(top_n).collect()}

    return top_results


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder.appName("RutasLentas").config("spark.executor.memory", "8g").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    # Elegimos el mes que vamos a analizar
    ruta_movements = r"datos\movements\202012_movements.json"
    ruta_stations = r"datos\stations\202012_stations.json"

    velocidades = obtener_velocidades(ruta_movements, ruta_stations, spark_session, min_count=50, top_n=10)

    print(velocidades)
