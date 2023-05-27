# Práctica Obligatoria Spark. BiciMad

Trabajo realizado por el grupo de los siguientes integrantes: 
- David Labrador Merino
- Rodrigo de la Nuez Moraleda
- Álvaro Cámara Fernández 
- Abdelaziz el Kadi Lachehab

<br>

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
* Calcular la velocidad media las distintas rutas. Para identificar tramos lentos, indicando posibles problemas y carencias en la infraestructura de la ciudad: Falta de carriles bici, malas condiciones de la via, etc.

<br>

Para comprobar que las ejecuciones de los distintos programas funcionan correctamente se han utilizado los archivos de datos de movimiento `DatosDeUso12_2020.json` y de la información de la situación de las distintas estaciones `SituacionesEstaciones12_2020.json` que se corresponden con los archivos correspondientes al mes de diciembre de 2020, que pueden encontrarse en la carpeta *datos* y la página web oficial de BiciMad: [https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)).

<br>

El archivo `leyenda.pdf` contiene información acerca del formato de los datos que pueden encontrarse en los ficheros correspondientes a los datos de movimiento de los usuarios que han utilizado los servicios disponibles y las situaciones de la estaciones activas. Además, pueden verse ejemplos de cómo ejecutar los distintos programas y algunos resultados obtenidos en el Jupyter Notebook `summary.ipynb`.

<br>

## mapa_dia.py

Este programa ejecuta una aplicación de nombre `mapaBicimad` que lee dos archivos en formato JSON que contienen información sobre los movimientos y las situaciones de las estaciones de Bicimad durante un mes. Además, se le debe indicar un día del mes que se quiere estudiar en formato `YYYY-MM-DD` y el archivo HTML en que se guardará la visualización de los datos recopilados. 

* Ejemplo de ejecución del programa:

`python3 mapa_dia.py DatosDeUso_12_2020.json SituacionesEstaciones12_2020.json 2020-12-01 mapa_2020-12-01.html`

Primero, el programa guarda las posiciones (`[longitude,latitude]`) y los identificadores de las estaciones activas para el día específico que se ha introducido como valor de entrada, y hace lo propio con las variables `idplug_station` (estación de enganche) e `idunplug_station` (estación de desenganche) del fichero que guarda los movimientos de los usuarios conectados a la red de Bicimad.

Con esta información se evalúa cuáles han sido las estaciones más concurridas a lo largo del día introducido, y utilizando la librería de visualización geoespacial `folium` presentamos esta información en un mapa de forma que se pueda acceder a la información de enganches y desenganches de todas las estaciones, así como proporcionar una representación que (mediante una escala de color) permita conocer cuáles son las estaciones de mayor interés. Es importante remarcar que el formato de los archivos JSON debe ser el que siguen los archivos del año 2020 (véanse `DatosDeUso_12_2020.json` y `SituacionesEstaciones12_2020.json` como posibles referencias) de libre acceso en la página web de la EMT de Madrid [https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales](https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)).

Además, se han delineado los trayectos con más apariciones en el conjunto de datos de uso de los cuáles se ha seleccionado `top = 500` para visualizar las 500 rutas más concurridas a lo large de ese día, junto con la información de las estaciones explicada en el párrafo anterior. Si se desea, este valor puede cambiarse dentro del código para reducir o aumentar el número de rutas a considerar.

Por último, se puede importar la función `main(sc, usage_file, stations_file, day, outfile, top)` para usarse directamente desde otro programa. De forma predeterminada, se han seleccionado los valores de la ejecución anterior para que pueda servir como ejemplo (nótese que los archivos `.json` en este caso deberán encontrarse en el mismo directorio que el programa a ejecutar).

<br>

## mapa_2020-12-01.html

Este archivo muestra el resultado obtenido al ejecutar el archivo `mapa_dia.py` con la información correspondiente al mes de julio de 2020, más específicamente, sobre el día `2020-12-01` (`YYYY-MM-DD`).

<br>

## conteo_rango_edades.py

Este archivo lee una serie de base de datos en las que se encuentre el dato `ageRange` (si no se encuentra, se pone un None y filtra luego segun los datos encontrados). Estas bases de datos deben de tener un formato similar al JSON:

	Son archivos en los cuales cada línea es un archivo JSON (es un diccionario).

Así, cuando ya tenemos recogidos todos los datos, contamos la cantidad de veces que aparece cada dato usando la función `countByValue()` y, seguidamente, se representan los datos mediante un diagrama de sectores (también damos opción a devolver un gráfico de barras, pero es más visual el gráfico de sectores). El gráfico, finalmente, se guardará en la carpeta desde la que se esté ejecutando el archivo.

