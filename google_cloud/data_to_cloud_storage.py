# SUBE ARCHIVOS EN LA MAQUINA LOCAL A UN BUCKET DE GOOGLE CLOUD STORAGE

# librerias
from google.cloud import storage
import pandas as pd

# id del proyecto en google cloud
project_id = 'tfm-zeleros'
bucket_name = 'tfm_zeleros_bucket'

# funcion para subir un archivo
def upload_file(origin_name, destination_name):
    try:
        # get bucket object
        bucket = storage_client.bucket(bucket_name)
        # use a blob object to upload the file
        blob = bucket.blob(destination_name)
        blob.upload_from_filename(origin_name)
        print('File ',origin_name,' uploaded to bucket ',bucket_name,' successfully')
    except Exception as e:
        print(e)

# ejecutar
if __name__ == "__main__":

    # identificarse en Google Cloud
    storage_client = storage.Client(project=project_id)

    # lee el archivo que indica que hay que subir
    files_to_refresh = pd.read_csv(r'data\info\files_info.csv')
    # omitir archivos que se ha especificado no actualizar
    files_to_refresh = files_to_refresh[files_to_refresh['actualizar'] == 1]
    
    # sube todos los archivos
    for row in files_to_refresh.index:
        upload_file(files_to_refresh['ruta_ori'][row], files_to_refresh['ruta_des'][row])