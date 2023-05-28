# Práctica Obligatoria Spark. BiciMad

Trabajo realizado por el grupo de los siguientes integrantes: 
- David Labrador Merino
- Rodrigo de la Nuez Moraleda
- Álvaro Cámara Fernández 
- Abdelaziz el Kadi Lachehab

<br>

Este documento explica la estructura del repositorio y las instrucciones de uso de las distintas herramientas proporcionadas (estudios). La documentación con la información de los estudios realizados, resultados, conclusiones y propuesta de soluciones se encuentra en el Notebook ``summary.ipynb``.

<br>

Problemas a solucionar y datos a analizar de BiciMad:
* ALVARO
* Obtener las rutas y estaciones más transitadas de la ciudad para un día concreto partiendo de los datos recopilados del mes específico. Los resultados obtenidos se visualizan con la creación de un mapa interactivo.
* Calcular la velocidad media las distintas rutas. Para identificar tramos lentos, indicando posibles problemas y carencias en la infraestructura de la ciudad: Falta de carriles bici, malas condiciones de la vía, etc.
* DAVID

En el repositorio también se pueden encontrar los datos usados (en la carpeta ``/datos``), los scripts para obtener resultados (en la carpeta ``/scripts``) y los resultados (en la carpeta ``/resultados``). También se encuenta el Notebook ``previos.ipynb``, en que se han realizado pruebas para entender los datos de entrada. Este último Notebook no es de interés para la presentación de los estudios, simplemente ha servido para un primer contacto con los datos. 

<br>

## Datos usados

Para realizar los distintos estudios se han utilizado los datos de la página web oficial de [BiciMad](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)). Estos datos se pueden encontrar dividos por tipo: 

* Movimiento (``datos/movements/``): Información sobre el uso de las bicicletas por parte del usuario final
* Estaciones (``datos/stations/``): Información de la situación de las estaciones). 

Se encuentran disponibles los datos desde Junio hasta Diciembre del 2020. Para más información sobre los datos, se puede consultar el archivo `leyenda.pdf`. Este contiene información acerca del formato de los datos y la información que podemos encontrar en ellos.

<br>

## Estudios realizados

### Estudio 1: Estadísticas de la edad

Script - `conteo_rango_edades.py`

Este archivo lee una serie de base de datos en las que se encuentre el dato `ageRange` (si no se encuentra, se pone un None y filtra luego segun los datos encontrados). Estas bases de datos deben de tener un formato similar al JSON:

	Son archivos en los cuales cada línea es un archivo JSON (es un diccionario).

Así, cuando ya tenemos recogidos todos los datos, contamos la cantidad de veces que aparece cada dato usando la función `countByValue()` y, seguidamente, se representan los datos mediante un diagrama de sectores (también damos opción a devolver un gráfico de barras, pero es más visual el gráfico de sectores). El gráfico, finalmente, se guardará en la carpeta desde la que se esté ejecutando el archivo.

<br>

### Estudio 2: Mapa de rutas más usadas en un día

Script - `mapa_dia.py`

Este programa ejecuta una aplicación de nombre `mapaBicimad` que lee dos archivos en formato JSON que contienen información sobre los movimientos y las situaciones de las estaciones de Bicimad durante un mes. Además, se le debe indicar un día del mes que se quiere estudiar en formato `YYYY-MM-DD` y el archivo HTML en que se guardará la visualización de los datos recopilados. 

El programa guarda las posiciones y los identificadores de las estaciones activas, y hace lo propio con las variables `idplug_station` (estación de enganche) e `idunplug_station` (estación de desenganche) del fichero que guarda los movimientos de usuarios. Con esta información se evalúa cuáles han sido las estaciones más concurridas a lo largo del día introducido, y utilizando la librería de visualización geoespacial `folium` presentamos esta información en un mapa de forma que se pueda acceder a la información de enganches y desenganches de todas las estaciones, así como proporcionar una representación que permita conocer las estaciones y rutas de mayor interés. 

Se puede importar la función `main(sc, usage_file, stations_file, day, outfile, top)` para usarse directamente desde otro programa. De forma predeterminada, se han seleccionado los documentos de diciembre de 2020 y el primer día de dicho mes, para que pueda servir como ejemplo.

<br>

### Estudio 3: Rutas más lentas

Script - `rutas_lentas.py`

que script es, breve descripción, como se usa

<br>

### Estudio 4: (DAVID)

que script es, breve descripción, como se usa

<br>

## Resultados

### ALVARO

### mapa_2020-12-01.html

Este archivo muestra el resultado obtenido al ejecutar el archivo `mapa_dia.py` con la información correspondiente al mes de julio de 2020, más específicamente, sobre el día `2020-12-01` (`YYYY-MM-DD`).

### AZIZ

### DAVID

<br>

