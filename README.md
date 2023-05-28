# Práctica Obligatoria Spark. BiciMad

Trabajo realizado por el grupo de los siguientes integrantes: 
- David Labrador Merino
- Rodrigo de la Nuez Moraleda
- Álvaro Cámara Fernández 
- Abdelaziz el Kadi Lachehab

<br>

CAMBIAR, NO ESTA ACTUALIZADO ----> Problemas a solucionar y datos a analizar de BiciMad:
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
* Calcular la velocidad media las distintas rutas. Para identificar tramos lentos, indicando posibles problemas y carencias en la infraestructura de la ciudad: Falta de carriles bici, malas condiciones de la via, etc.

Para poder probar las distintas funciones, observar algunos resultados de los estudios y ver ejemplos de cómo ejecutar los distintos programas se recomienda seguir el Notebook resumen: ``summary.ipynb``.

En el repositorio también se pueden encontrar los datos usados (en la carpeta ``/datos``), los scripts para obtener resultados (en la carpeta ``/scripts``) y los resultados (en la carpeta ``\resultados``). También se encuenta el Notebook ``previos.ipynb``, en que se han realizado pruebas para entender los datos de entrada.  

<br>

## Datos usados

Para realizar los distintos estudios se han utilizado los datos de la página web oficial de [BiciMad](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)). Estos datos se pueden encontrar dividos por tipo: 

* Movimiento (``datos\movements``): Información sobre el uso de las bicicletas por parte del usuario final)
* Estaciones (``datos\stations``): Información de la situación de las estaciones). 

Se encuentran disponibles los datos desde Junio hasta Diciembre del 2020. Para más información sobre los datos, se puede consultar el archivo `leyenda.pdf`. Este contiene información acerca del formato de los datos y la información que podemos encontrar en ellos.

<br>

## Estudios realizados

### Estudio 1: Estadísticas de la edad

Script: conteo_rango_edades.py

Este archivo lee una serie de base de datos en las que se encuentre el dato `ageRange` (si no se encuentra, se pone un None y filtra luego segun los datos encontrados). Estas bases de datos deben de tener un formato similar al JSON:

	Son archivos en los cuales cada línea es un archivo JSON (es un diccionario).

Así, cuando ya tenemos recogidos todos los datos, contamos la cantidad de veces que aparece cada dato usando la función `countByValue()` y, seguidamente, se representan los datos mediante un diagrama de sectores (también damos opción a devolver un gráfico de barras, pero es más visual el gráfico de sectores). El gráfico, finalmente, se guardará en la carpeta desde la que se esté ejecutando el archivo.

<br>

### Estudio 2: Mapa de rutas más usadas en un día

Script: mapa_dia.py

Este programa ejecuta una aplicación de nombre `mapaBicimad` que lee dos archivos en formato JSON que contienen información sobre los movimientos y las situaciones de las estaciones de Bicimad durante un mes. Además, se le debe indicar un día del mes que se quiere estudiar en formato `YYYY-MM-DD` y el archivo HTML en que se guardará la visualización de los datos recopilados. 

* Ejemplo de ejecución del programa (suponiendo que los datos están en el mismo directorio):

`python3 mapa_dia.py 202012_movements.json 202012_stations.json 2020-12-01 mapa_2020-12-01.html`

Primero, el programa guarda las posiciones (`[longitude,latitude]`) y los identificadores de las estaciones activas para el día específico que se ha introducido como valor de entrada, y hace lo propio con las variables `idplug_station` (estación de enganche) e `idunplug_station` (estación de desenganche) del fichero que guarda los movimientos de los usuarios conectados a la red de Bicimad.

Con esta información se evalúa cuáles han sido las estaciones más concurridas a lo largo del día introducido, y utilizando la librería de visualización geoespacial `folium` presentamos esta información en un mapa de forma que se pueda acceder a la información de enganches y desenganches de todas las estaciones, así como proporcionar una representación que (mediante una escala de color) permita conocer cuáles son las estaciones de mayor interés. Es importante remarcar que el formato de los archivos JSON debe ser el que siguen los archivos del año 2020 (véanse `202012_movements.json` y `202012_stations.json` como posibles referencias) de libre acceso en la página web de la EMT de Madrid [https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)).

Además, se han delineado los trayectos con más apariciones en el conjunto de datos de uso de los cuáles se ha seleccionado `top = 500` para visualizar las 500 rutas más concurridas a lo large de ese día, junto con la información de las estaciones explicada en el párrafo anterior. Si se desea, este valor puede cambiarse dentro del código para reducir o aumentar el número de rutas a considerar.

Por último, se puede importar la función `main(sc, usage_file, stations_file, day, outfile, top)` para usarse directamente desde otro programa. De forma predeterminada, se han seleccionado los valores de la ejecución anterior para que pueda servir como ejemplo (nótese que los archivos `.json` en este caso deberán encontrarse en el mismo directorio que el programa a ejecutar).

<br>

### Estudio 3: Rutas más lentas

Script: rutas_lentas.py

que script es, breve descripción, como se usa

<br>

### Estudio 4: (DAVID)

que script es, breve descripción, como se usa

<br>

## Resultados

### mapa_2020-12-01.html

Este archivo muestra el resultado obtenido al ejecutar el archivo `mapa_dia.py` con la información correspondiente al mes de julio de 2020, más específicamente, sobre el día `2020-12-01` (`YYYY-MM-DD`).

### ESTUDIO 1 (ALVARO)

### ESTUDIO 3 (AZIZ)

### ESTUDIO 4 (DAVID)

<br>

