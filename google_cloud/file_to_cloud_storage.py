# SUBE UN ARCHIVO EN LA MAQUINA LOCAL A UN BUCKET DE GOOGLE CLOUD STORAGE

# librerias
from google.cloud import storage

# id del proyecto en google cloud
project_id = 'tfm-zeleros'

# funcion para subir un archivo
def upload_file(origin_name, bucket_name, destination_name):
    storage_client = storage.Client(project=project_id)
    try:
        # get bucket object
        bucket = storage_client.bucket(bucket_name)
        # use a blob object to upload the file
        blob = bucket.blob(destination_name)
        blob.upload_from_filename(origin_name)
        print('file: ',origin_name,' uploaded to bucket: ',bucket_name,' successfully')
    except Exception as e:
        print(e)

# ejecutar
if __name__ == "__main__":
    # subir archivo (ruta en local, nombre bucket y ruta en bucket)
    upload_file('data_in\C\W\worldcities.csv', 'tfm_zeleros_bucket', 'global/worldcities.csv')