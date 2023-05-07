from pyspark import SparkContext, SparkConf
import sys, json, folium

import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

ordered_colors = ['lightred', 'red', 'darkred', 'darkpurple']

# Función para eliminar duplicados en una lista de diccionarios
def remove_duplicates(dict_list):
    result = []
    keys = set()
    for dicc in dict_list:
        key = next(iter(dicc))
        if key not in keys:
            keys.add(key)
            result.append(dicc)
    return result

# Función para guardar las posiciones y datos de enganches por estación
def merge(list1, list2, n):
    result_dict1, result_dict2 = {}, {}
    idplug_counts, idunplug_counts = [0] * n, [0] * n

    for item1 in list1:
        idplug_counts[item1['idplug_station'] - 1] += 1
        idunplug_counts[item1['idunplug_station'] - 1] += 1

    for i in range(1, n + 1):
        if i in list2.keys():
            item2 = list2[i]
            result_dict1[item2['name']] = [float(item2['latitude']), float(item2['longitude'])]
            result_dict2[item2['name']] = [idplug_counts[i-1], idunplug_counts[i-1]]

    return result_dict1, result_dict2

# Función para tomar un color según un valor entre 0 y 1
def select_color(num):
    num = max(0, min(num, 1))
    return ordered_colors[min(int(num * len(ordered_colors)), 3)]

# Función para conseguir la mayor suma de un elemento en una lista de listas
def max_sum_sublist(lst):
    max_sum = max(sum(sublist) for sublist in lst)
    return max_sum

def main(sc, usage_file, stations_file, day, outfile):
    usage_rdd = sc.textFile(usage_file)
    stations_rdd = sc.textFile(stations_file)

    # Filtramos los datos de uso basado sen el día dado
    filtered_usage_rdd = usage_rdd.filter(lambda x: json.loads(x)['unplug_hourTime'].startswith(day))

    # Extraemos la información de los enganches y desenganches
    result1 = filtered_usage_rdd.map(lambda x: {'idplug_station': json.loads(x)['idplug_station'], 
                                            'idunplug_station': json.loads(x)['idunplug_station']}).collect()

    # Filtramos la situación de las estaciones para el día dado
    filtered_rdd = stations_rdd.filter(lambda x: json.loads(x)['_id'].startswith(day))
    filtered_rdd = filtered_rdd.flatMap(lambda x: json.loads(x)['stations'])
    filtered_rdd = filtered_rdd.filter(lambda x: x['activate'] == 1)

    # Extraemos la información de las estaciones en un diccionario
    result2 = filtered_rdd.map(lambda x: {x['id']: {'name': x['name'], 'longitude': x['longitude'], 
                                                    'latitude': x['latitude']}}).collect()
    result2 = remove_duplicates(result2)

    # Obtenemos el mayor valor para los identificadores de las estaciones
    n = max([max(d.keys()) for d in result2])

    # Combinamos los diccionarios de result2
    merged_result = {}
    for d in result2:
        merged_result.update(d)

    # Recolectamos la información de posiciones y enganches por estación
    stations, operations = merge(result1, merged_result, n)

    # Guardamos las coordenadas de la ciudad de Madrid para el mapa
    madrid_coords = [40.4168, -3.7038]
    madrid_map = folium.Map(location = madrid_coords, zoom_start = 12)
    
    # Valor entre el que habrá que dividir para la escala de color
    ratio = max_sum_sublist(list(operations.values()))

    for name, location in stations.items():
        enganches, desenganches = operations[name][0], operations[name][1]
        normalized = (enganches + desenganches) / ratio
        color = select_color(normalized)
        popup_text = f"<b>Estación:</b> {name}<br><b>Nº de enganches:</b> {enganches}<br><b>Nº de desenganches:</b> {desenganches}"
        popup = folium.Popup(popup_text, max_width=400, max_height=400)
        folium.Marker(location=location, popup=popup, icon=folium.Icon(color=color)).add_to(madrid_map).add_to(madrid_map)
        
    madrid_map.save(outfile)


if __name__ == '__main__':
    if len(sys.argv) == 5:
        conf = SparkConf().setAppName("mapaBicimad")
        with SparkContext(conf=conf) as sc:
            sc.setLogLevel("ERROR")
            usage_file, stations_file = sys.argv[1], sys.argv[2]
            day = sys.argv[3]
            outfile = sys.argv[4]
            print(usage_file, stations_file, day, outfile)
            main(sc, usage_file, stations_file, day, outfile)
    else:
        print("Uso: python3 {0} <input_file> <output_file>".format(sys.argv[0]))