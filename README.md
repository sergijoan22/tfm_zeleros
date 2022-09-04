# TFM ZELEROS

## Activar el entorno virtual para python

Se ha seguido https://cloud.google.com/python/docs/setup, que decía de crear un entorno virtual.

En caso de que salga algún error de permisos al activar el error usando:

```bash
env\Scripts\activate
```

Poner primero:

```bash
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
```

La carpeta con el entorno lo he dejado fuera del repositorio, pero los paquetes usados en este están en el archivo requirements.txt.

Para instalar los paquetes usar:

```
pip install -r /path/to/requirements.txt
```

En caso de que se use un nuevo paquete usar el siguiente comando, que crea un nuevo requirements.txt con todos los paquetes que hay en el entorno virtual

```
pip freeze > requirements.txt
```

## Google Cloud

Se ha usado Google Cloud, en este [README](https://github.com/sergijoan22/tfm_zeleros_google_cloud/README.md) hay información de lo que se ha usado

## Preparar y subir archivos a Google Cloud

### Descargar archivos

#### Flights Spain

1. Ir a https://nap.mitma.es/Files/Detail/920
2. Descargar la version TXT AECFA
3. Renombrar el archivo a F_ES.txt ya que el nombre va cambiando
4. Poner el archivo en la carpeta data

#### Train Spain

1. Ir a https://nap.mitma.es/Files/Detail/897
2. Descargar la versión GTFS
3. Renombrar el zip a T_ES.zip
4. Poner el zip en la carpeta data

#### Airports data

1. Al ejecutar el script se cargan de la web a Cloud Storage directamente (Desde https://ourairports.com/data/airports.csv)

#### World Cities

1. Ir a https://simplemaps.com/data/world-cities
2. Descargar el archivo de la versión Basic
3. Poner en zip en la carpeta data

### Preparar archivos

Una vez hecho el paso anterior, el script `prepare_data.py` ubicado en la carpeta Google Cloud prepara los archivos para ser subidos a Cloud Storage.

### Subir los archivos

Dentro de la carpeta Google Cloud, el archivo `data_to_cloud_storage.py` sube los archivos desde la carpeta a Cloud Storage.

En el archivo `files_info.csv` de la carpeta data/info está especificado el nombre de los archivos y donde se van a cargar dentro del bucket de Cloud Storage. 

El script usa el archivo para saber que archivos subir a Cloud Storage. El campo actualizar se puede tocar para elegir que archivos actualizar y cuales no.

## Organización

(ESTO VA A CAMBIARSE, NO HACER CASO)

La carpeta `data_in` contiene archivos con datos recogidos. El archivo `sources.txt` explica como se han obtenido

La carpeta `etl` tiene scripts que transformar los datos de `data_in` a la carpeta de `data_out`. Los archivos se diferencias usando el código del país, además de una F para datos de vuelo y una T para datos de tren.

La carpeta `reports` contiene archivos que sirvan para mostrar los datos obtenidos.  