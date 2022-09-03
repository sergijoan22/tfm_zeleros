# DESCARGAR ARCHIVOS EN LA MAQUINA LOCAL DE UN BUCKET DE GOOGLE CLOUD STORAGE

# librerias
from google.cloud import storage
#import sys ----------------------------------

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
    
    # descarga el archivo
    download_file('global/airports.csv', 'data_cs/airports.csv')