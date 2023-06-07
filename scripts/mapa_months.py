from pyspark import SparkContext, SparkConf
import json, folium

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
        if item1['idplug_station'] < n and item1['idunplug_station'] < n:
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


from collections import Counter

# Función para contar el número de ocurrencias en una lista
def count_occurrences(lists):
    counts = Counter(tuple(lst) for lst in lists)
    return [[*lst, count] for lst, count in counts.items()]


def main(sc, usage_files, stations_files, outfile, top=500):
    usage_rdd = sc.textFile(','.join(usage_files))
    stations_rdd = sc.textFile(','.join(stations_files))
    
    # Extraemos la información de los enganches y desenganches
    result = usage_rdd.map(lambda x: {'idplug_station': json.loads(x)['idplug_station'], 
                                           'idunplug_station': json.loads(x)['idunplug_station']})
    result1 = result.collect()
    trips = result.map(lambda x: list(x.values())).collect()
    trips_count = count_occurrences(trips)
    sorted_trips = sorted(trips_count, key=lambda x: x[2])
    head_trips = sorted_trips[len(sorted_trips)-top:]

    # Filtramos la situación de las estaciones para el día dado
    filtered_rdd = stations_rdd.flatMap(lambda x: json.loads(x)['stations'])
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
        folium.Marker(location=location, popup=popup, icon=folium.Icon(color=color)).add_to(madrid_map)
    
    for trip in head_trips:
        start_index, end_index = trip[0], trip[1]
        
        if start_index < n and end_index < n:
            start_pos = [float(merged_result[start_index]["latitude"]),
                     float(merged_result[start_index]["longitude"])]
        
            end_pos = [float(merged_result[end_index]["latitude"]),
                   float(merged_result[end_index]["longitude"])]
        
            line_points = [start_pos, end_pos]
            popup_text = "<b>Número de viajes:</b>" + str(int(trip[2]))
            popup = folium.Popup(popup_text, max_width=400, max_height=400)
            line = folium.PolyLine(locations=line_points, color='blue', weight=2, popup=popup)
            line.add_to(madrid_map)
    
    madrid_map.save(outfile)
    
if __name__ == '__main__':
    if len(sys.argv) >= 4:
        conf = SparkConf().setAppName("mapaBicimad")
        with SparkContext(conf=conf) as sc:
            sc.setLogLevel("ERROR")
            usage_files, stations_files = [], []
            for i in range(1,len(sys.argv)-1):
                if sys.argv[i].endswith("movements.json"):
                    usage_files += [sys.argv[i]]
                elif sys.argv[i].endswith("stations.json"):
                    stations_files += [sys.argv[i]]
            outfile = sys.argv[-1]
            # usage_files = ["../datos/movements/202011_movements.json", "../datos/movements/202012_movements.json"]
            # stations_files = ["../datos/stations/202011_stations.json", "../datos/stations/202011_stations.json"]
            # outfile = "prueba.html"
            print(usage_files, stations_files, outfile)
            main(sc, usage_files, stations_files, outfile)
    else:
        print("Uso: python3 {0} <input_file> <output_file>".format(sys.argv[0]))