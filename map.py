import folium
import random

# Set the coordinates for Madrid
madrid_coords = [40.4168, -3.7038]

# Create the map object and set the initial zoom level
madrid_map = folium.Map(location=madrid_coords, zoom_start=12)

# Add markers for the main train stations in Madrid
train_stations = {
    'Atocha': [40.4076, -3.6868],
    'Chamartin': [40.4720, -3.6889],
    'Nuevos Ministerios': [40.4469, -3.6933]
}

ordered_colors = ['lightred', 'red', 'darkred', 'darkpurple']

# Function that given a number between 0 and 1, selects a color
def select_color(num):
    num = max(0, min(num, 1))
    return ordered_colors[min(int(num * len(ordered_colors)), 3)]

for name, location in train_stations.items():
    enganches, desenganches = random.randint(10, 50), random.randint(10, 50)
    normalized = (enganches + desenganches) / 100
    color = select_color(normalized)
    popup_text = f"{name}\n{enganches} - {desenganches}"
    folium.Marker(location=location, popup=popup_text, icon=folium.Icon(color=color)).add_to(madrid_map)

# Function that given a value between 0 and 1, returns an integer value of {1, 2, 3}
def map_value(num):
    num = max(0, min(num, 1))
    return 1 + int(num * 2)

# Set the locations of Atocha and Chamartin
atocha_loc = train_stations['Atocha']
chamartin_loc = train_stations['Chamartin']

# Set the value between 0 and 1 that determines the width of the line
line_width_value = random.random()

# Map the value to a line width between 1 and 6
line_width = map_value(line_width_value)

# Create a line object between Atocha and Chamartin with the mapped line width
line_points = [atocha_loc, chamartin_loc]
popup_text = str(int(line_width_value*10))
line = folium.PolyLine(locations=line_points, weight=line_width, popup=popup_text)

# Add the line to the map
line.add_to(madrid_map)

# Display the map
madrid_map

madrid_map.save("madrid_map.html")
