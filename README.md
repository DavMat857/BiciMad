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
* Analizar el tipo de usuario que mas utiliza el sistema de BICIMAD, así como el tiempo medio del viaje. Los resultados obtenidos se verán en un par de gráficas al finalizar su ejecución.
* Obtener las rutas y estaciones más transitadas de la ciudad para un día concreto partiendo de los datos recopilados del mes específico. Los resultados obtenidos se visualizan con la creación de un mapa interactivo.
* Calcular la velocidad media las distintas rutas. Para identificar tramos lentos, indicando posibles problemas y carencias en la infraestructura de la ciudad: Falta de carriles bici, malas condiciones de la vía, etc.
* Realizar un cálculo de la cantidad de CO2 que no se ha generado gracias a BiciMad. Para ello calculamos la cantidad de kilómetros realizados por los usuarios

En el repositorio también se pueden encontrar los datos usados (en la carpeta ``/datos``), los scripts para obtener resultados (en la carpeta ``/scripts``) y los resultados (en la carpeta ``/resultados``). También se encuenta el Notebook ``previos.ipynb``, en que se han realizado pruebas para entender los datos de entrada. Este último Notebook no es de interés para la presentación de los estudios, pero ha servido como primer contacto con los datos. 

<br>

## Datos usados

Para realizar los distintos estudios se han utilizado los datos de la página web oficial de [BiciMad](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)). Estos datos se pueden encontrar dividos por tipo: 

* Movimiento (``datos/movements/``): Información sobre el uso de las bicicletas por parte del usuario final
* Estaciones (``datos/stations/``): Información de la situación de las estaciones). 

Se encuentran disponibles los datos desde Junio hasta Diciembre del 2020. Para más información sobre los datos, se puede consultar el archivo `leyenda.pdf`. Este contiene información acerca del formato de los datos y la información que podemos encontrar en ellos.

<br>

## Estudios realizados

### **Estudio 1: Estadísticas de la edad**

Script - `conteo_rango_edades.py`

Este archivo lee una serie de base de datos en las que se encuentren los datos `ageRange` y `travel_time` (si no se encuentran, se pone un None y filtra luego segun los datos encontrados). Estas bases de datos deben de tener el mismo formato que tiene el archivo `202012_movements.json`. 

Aunque en el Notebook `summary.ipynb` solamente se ejecute este archivo con la base de datos `202012_movements.json`, también admite una ejecución con varias bases de datos. Ponemos un ejemplo de ejecucion del script con 3 bases de datos:

```
python3 conteo_rango_edades.py datos/movements 202012_movements.json 202011_movements.json 202010_movements.json
```

Nota a tener en cuenta: al ejecutar este script, hay que pasarle la ruta en la que se encuentran los archivos. Esta ruta debe de ser común a todas las bases de datos. Es decir, todas las bases de datos se deben de encontrar en la misma carpeta. Si el archivo se ejecutra en la carpeta donde se encuentran todas las bases de datos, escribir `actual` para que el archivo pueda detectarlo.

Esta función nos proporcionará el tiempo medio de viaje (siempre que este supere el minuto de duración) de las bases de datos pasadas como input, así como 2 gráficas: una de ellas con todos los usuarios, y la otra solamente con los usuarios que SI han registrado su edad en la aplicación.

<br>

### **Estudio 2: Mapa de rutas más usadas en un día**

Script - `mapa_dia.py` `mapa_months.py`

Este programa ejecuta una aplicación de nombre `mapaBicimad` que lee dos archivos en formato JSON que contienen información sobre los movimientos y las situaciones de las estaciones de Bicimad durante un mes específico. Además, se le debe indicar un día del mes que se quiere estudiar en formato `YYYY-MM-DD` y el archivo HTML en que se guardará la visualización de los datos recopilados. Un ejemplo de ejecución es el siguiente:

<br>

```
python3 mapa_dia.py 202012_movements.json 202012_stations.json 2020-12-01 mapa_2020-12-01.html
```
<br>

El programa guarda las posiciones y los identificadores de las estaciones activas, y hace lo propio con las variables `idplug_station` (estación de enganche) e `idunplug_station` (estación de desenganche) del fichero que guarda los movimientos de usuarios. Con esta información se evalúa cuáles han sido las estaciones más concurridas a lo largo del día introducido, y utilizando la librería de visualización geoespacial `folium` presentamos esta información en un mapa de forma que se pueda acceder a la información de enganches y desenganches de todas las estaciones, así como proporcionar una representación que permita conocer las estaciones y rutas de mayor interés. 

Se puede importar la función `main(sc, usage_file, stations_file, day, outfile, top)` para usarse directamente desde otro programa. De forma predeterminada, se han seleccionado los documentos de diciembre de 2020 y el primer día de dicho mes, para que pueda servir como ejemplo. Además, para generalizar la creación del mapa con los datos de todos los meses hemos creado el programa alternativo `mapa_months.py` que puede tomar una lista de ficheros con información de movimientos y estaciones para distintos meses.

<br>

### **Estudio 3: Rutas más lentas**

Script - `rutas_lentas.py`

Este script analiza los datos de BiciMad para identificar las rutas más lentas en la red de bicicletas de Madrid. Utiliza los datos de las estaciones y los movimientos de los usuarios para calcular la velocidad media de cada ruta y selecciona las rutas con al menos 50 viajes en un mes determinado.

Para ejecutar el script, es necesario proporcionar:
* Ruta al archivo JSON con datos de las estaciones. En la ruta ``datos/stations/`` se pueden encontrar datos de prueba en el formato ``AAAAMM_stations.json``.
* Ruta al archivo JSON con datos de los movimientos. En la ruta ``datos/movements/`` se pueden encontrar datos de prueba en el formato ``AAAAMM_movements.json``.
* Ruta para guardar el gráfico de resultados.

```
python3 rutas_lentas.py <datos_movimientos> <datos_estaciones> <resultado>
```

Por defecto, si no se indican las rutas mencionadas, se tomarán las siguientes (donde se analizan los datos de Diciembre de 2020):

* Ruta datos de movimientos: ``datos/movements/202012_movements.json`` 
* Ruta datos de estaciones: ``datos/stations/202012_stations.json``
* Ruta resultados: ``resultados/rutas_lentas.png``

También está la posibilidad de importar la función ``obtener_velocidades``y hacer la llamada (junto con una SparkSession) con los parámetros que se indiquen. De esta forma, además, se podrá indicar el número de trayectorias que debe de haber en una ruta y el número de estación lentas que se quiera ver:

```
obtener_velocidades(ruta_movements, ruta_stations, ruta_resultados, spark_session, min_count=50, top_n=10)
```

<br>

### **Estudio 4: Contaminación**

Script - `contaminación.py`

Este script calcula la cantidad de CO2 que no se emite en relación la cantidad de kilómetros realizados por los usuarios de BiciMad.

Para ejecutar el script, es necesario proporcionar:
* Ruta al archivo JSON con datos de las estaciones. En la ruta ``datos/stations/`` se pueden encontrar datos de prueba en el formato ``AAAAMM_stations.json``.
* Ruta al archivo JSON con datos de los movimientos. En la ruta ``datos/movements/`` se pueden encontrar datos de prueba en el formato ``AAAAMM_movements.json``.
* Diccionario que tome como clave el combustible utilizado y como valores una lista que contengas litro_gasolina por 100km, porcentaje en tanto por 1 de la cantidad de vehículos asociados a ese combustible y la cantidad de CO2 por litro de combustible.

```
python3 contaminacion.py <datos_movimientos> <datos_estaciones> <diccionario_combustibles>
```

También está la posibilidad de importar la función ``co2`` y hacer la llamada (junto con una SparkSession) con los parámetros que se indiquen. 

```
co2(ruta_movements, ruta_stations,diccionario_combustibles ,spark_session)
```
<br>

## Resultados

### comparacion_rango_edades.png

Este archivo es uno de los obtenidos al ejecutar el archivo `conteo_rango_edades.py`. Aquí se nos muestra una gráfica de sectores la cual nos indica el porcentaje de personas que han usado el sistema BICIMAD en función de su edad. Esta medición se realiza siempre que la duración del viaje sea mayor o igual que 1 minuto.

### comparacion_rango_edades_filtrado.png

Este es el otro archivo obtenido al ejecutar `conteo_rango_edades.py`. Aquí se nos muestra la información de las personas que SI han marcado su edad a la hora de coger la bici. Estos datos, al igual que en el anterior, son recogidos siempre y cuando la duración del viaje sea mayor o igual que 1 minuto.

### mapa_2020-12-01.html

Este archivo muestra el resultado obtenido al ejecutar el archivo `mapa_dia.py` con la información correspondiente al mes de diciembre de 2020, más específicamente, sobre el día `2020-12-01` (`YYYY-MM-DD`) que son los valores de entrada seleccionados de forma predeterminada para comprobar el buen funcionamiento de nuestro programa.

### rutas_lentas.png

Gráfica mostrando las rutas más lentas detectadas en el mes de la última ejecución del estudio de Rutas Lentas. En este grafico podemos observar, ordenadas de más lentas a menos, las rutas (indicadas con los nombre de las entaciones) más lentas junto a su velocidad media.

### contaminación

Muestra por pantalla la cantidad de CO2 que no hemos emitido

<br>

