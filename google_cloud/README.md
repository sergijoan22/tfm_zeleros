# Google Cloud

- Cuenta: [tfm.zeleros.edem@gmail.com](mailto:tfm.zeleros.edem@gmail.com)

- Proyecto: tfm-zeleros
- Proyecto_id: tfm-zeleros

## Google Cloud Storage

Se usa de data lake para guardar los archivos en bruto antes de tratarlos.

Se ha creado un bucket llamado tfm_zeleros_bucket con varias carpetas dentro para tener los archivos ordenados (Hay que ver la mejor manera para organizarlo). El bucket esta en la zona europe-southwest1 (En Madrid, que se estreno hace poco)

Para crear el bucket: [Quickstart: Discover object storage with the Google Cloud console | Cloud Storage](https://cloud.google.com/storage/docs/discover-object-storage-console)

Para poder usar Python con Cloud Storage hay que identificarse: [Provide credentials for Application Default Credentials  | Authentication  | Google Cloud](https://cloud.google.com/docs/authentication/provide-credentials-adc#local-dev)

Para usar lo de antes creo que hay que descargarse el Google Cloud SDK: [Guía de inicio rápido: Instala Google Cloud CLI  | Documentación de la CLI de Google Cloud](https://cloud.google.com/sdk/docs/install-sdk?hl=es)

La base del código para subir archivos a Cloud Storage lo he sacado de aquí: [Guía de inicio rápido: Instala Google Cloud CLI  | Documentación de la CLI de Google Cloud](https://cloud.google.com/sdk/docs/install-sdk?hl=es)

Luego, funciones ya para subir o bajar archivos y demás buscando por ahí, como: [How To Upload And Download Files From Google Cloud Storage In Python (datacourses.com)](https://www.datacourses.com/how-to-upload-and-download-files-from-google-cloud-storage-python-3131/)

Cuando ejecutas el código sale un WARNING sobre algo de que no hay definida ninguna quota (Que son limites que se ponen al uso que se hace de las APIs). Al ir a ver las quotas en Google Cloud no deja crear una por ser cuenta gratuita, así que de momento no se si se puede quitar el warning, pero bueno, funciona igual por ahora.