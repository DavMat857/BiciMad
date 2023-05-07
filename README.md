# BiciMad
1) previo.ipynb es un archivo que contiene informacíón acerca de los archivos con los que
vamos a trabajar y contiene operaciones básicas que vamos a realizar.

Problemas a solucionar, o más bien datos a analizar de BiciMad:
* Mapa de calor 
* Cliente objetivo:
	Obtendremos información acerca del cliente objetivo, en función de este dato
	podremos buscar estrategias para llegar a otros públicos.
* Mejora medioambiental existente gracias al producto.
	1)Creamos tuplas destino-origen
	2)Calculamos las distancias
	3)Creamos función del %gasolina, %diesel, ...
	calculamos los km que hace BiciMad y añadimos CO2 ahorrado
* Media km por grupo de edad.
* Calcular la velocidad media del conjunto de todos los trayectos


## DatosDeUso_12_2020.json

## SituacionesEstaciones12_2020.json

## mapaDay.py

Este programa ejecuta una aplicación de nombre `mapaBicimad` que lee dos archivos en formato JSON que contienen información sobre los movimientos y las situaciones de las estaciones de Bicimad durante un mes. Además, se le debe indicar un día del mes que se quiere estudiar en formato `YYYY-MM-DD` y el archivo HTML en que se guardará la visualización de los datos recopilados. 

Primero, el programa guarda las posiciones (longitud y latitud) y los identificadores de las estaciones activas para el día específico que se ha introducido como valor de entrada, y hace lo propio con las variables `idplug_station` (estación de enganche) e `idunplug_station` (estación de desenganche) del fichero que guarda los movimientos de los usuarios conectados a la red de Bicimad.

Con esta información se evalúa cuáles han sido las estaciones más concurridas a lo largo del día introducido, y utilizando la librería de visualización geoespacial `folium` presentamos esta información en un mapa de forma que se pueda acceder a la información de enganches y desenganches de todas las estaciones, así como proporcionar una representación que (mediante una escala de color) permita conocer cuáles son las estaciones de mayor interés. Es importante remarcar que el formato de los archivos JSON debe ser el que siguen los archivos correspondientes al año 2020 (véase como referencia) de libre acceso.

## 2020-12-01.html

Este archivo muestra el resultado obtenido al ejecutar el archivo `mapaDay.py` con la información correspondiente al mes de julio de 2020, más específicamente, sobre el día `2020-12-01` (`YYYY-MM-DD`).

