# DESCARGAR ARCHIVOS EN LA MAQUINA LOCAL DE UN BUCKET DE GOOGLE CLOUD STORAGE

# librerias
from google.cloud import storage
import pandas as pd
import os

# id del proyecto en google cloud
project_id = 'tfm-zeleros'
bucket_name = 'tfm_zeleros_bucket'

# funcion para descargar un archivo
def download_file(origin_name, destination_file_name):
    # get bucket object
    bucket = storage_client.bucket(bucket_name)
    # use a blob object to upload the file
    blob = bucket.blob(origin_name)
    blob.download_to_filename(destination_file_name)
    print(
        "Downloaded storage object {} from {} to local file {}.".format(
            origin_name, bucket_name, destination_file_name
        )
    )


# ejecutar
if __name__ == "__main__":

    # identificarse en Google Cloud
    storage_client = storage.Client(project=project_id)
    
    # lee el archivo que indica que hay que subir
    files_to_process = pd.read_csv('google_cloud/files_info.csv')
    # omitir archivos que se ha especificado no actualizar
    files_to_process = files_to_process[files_to_process['actualizar'] == 1]
    
    # si los archivos se quieren guardar en un directorio que no existe da error
    # por lo que hay que crearlo antes
    directorios = ['data/T_ES'] 
    for dir in directorios:
        if not(os.path.isdir(dir)):
            os.mkdir(dir)
    
    # descarga todos los archivos
    for row in files_to_process.index:
        download_file(files_to_process['ruta_des'][row], files_to_process['ruta_ori'][row])
        