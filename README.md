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

## Organización

(ESTO VA A CAMBIARSE, NO HACER CASO)

La carpeta `data_in` contiene archivos con datos recogidos. El archivo `sources.txt` explica como se han obtenido

La carpeta `etl` tiene scripts que transformar los datos de `data_in` a la carpeta de `data_out`. Los archivos se diferencias usando el código del país, además de una F para datos de vuelo y una T para datos de tren.

La carpeta `reports` contiene archivos que sirvan para mostrar los datos obtenidos.  