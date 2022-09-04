# SUBIR UN DATAFRAME DE PANDAS A BIGQUERY

# librerías necesarias
import datetime
from google.cloud import bigquery
import pandas as pd

# lee el csv (Hay que cambiarlo para leerlo de Cloud Storabe basandose en data_to_cloud_storage.py)
cities_world = pd.read_csv('data\worldcities.csv', sep=",", decimal = '.')

project_id = 'tfm-zeleros'
table_id = "tfm-zeleros.tfm_zeleros.prueba" # proyecyo.dataset.tabla


# objeto de conexión a bigquery
client = bigquery.Client(project=project_id)

# configuración de la subida a bigquery
job_config = bigquery.LoadJobConfig(
    # definir el esquema (o parcialmente). Todas las columnas se escriben en la tabla
    schema=[
        # especificar el tipo de las columnas que no queremos que se autodetecte.
        # los casos de abajo es para probar, solo haria falta definir alguna columna que de error
        bigquery.SchemaField("city", bigquery.enums.SqlTypeNames.STRING),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("lat", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("lng", bigquery.enums.SqlTypeNames.FLOAT64)
    ],
    # este modo trunca la tabla antes de escribir los registros del dataframe
    write_disposition="WRITE_TRUNCATE",
)

# carga el dataframe a la tabla elegida con la configuracion especificada
job = client.load_table_from_dataframe(
    cities_world, table_id, job_config=job_config
)

job.result()  # espera a que se complete el job

table = client.get_table(table_id)  # lee la tabla subida en bigquery

# devuelve lo que hay en la tabla de bigquery
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)