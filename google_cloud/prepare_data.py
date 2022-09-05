import zipfile
import os
import shutil
import requests

# DESCARGAR AEROPUERTOS
url_airports = "https://ourairports.com/data/airports.csv"
# descargar de la web
response = requests.get(url_airports, stream=True)

# guardar en un archivo
# ! lo está guardando en binario por lo que luego al leerlo da problemas
with open('data/airports.csv', 'wb') as out_file:
  shutil.copyfileobj(response.raw, out_file)
print('Aeropuertos mundo: Actualizado')

# DESCOMPRIMIR ZIP TRENES ESPAÑA
try:
    # borrar carpeta si existe
    if os.path.isdir('data\T_ES') and os.path.exists('data\T_ES.zip'):
        shutil.rmtree('data\T_ES')
    # descomprimir zip
    with zipfile.ZipFile('data\T_ES.zip', 'r') as zip_ref:
        zip_ref.extractall('data\T_ES')
    # borrar zip
    os.remove('data\T_ES.zip')
    print("Trenes España: Actualizado")
except FileNotFoundError:
    print("Trenes España: El zip ya se ha descomprimido o aún no se ha cargado")

# DESCOMPRIMIR CIUDADES MUNDO
zip_dir = 'data\simplemaps_worldcities_basicv1.75.zip'
try:
    # borrar archivo si existe
    if os.path.exists("data\worldcities.csv") and os.path.isdir(zip_dir):
        os.remove("data\worldcities.csv")
    # descomprimir zip
    with zipfile.ZipFile(zip_dir, 'r') as zip_ref:
        zip_ref.extractall('data\WC')
    # borrar zip
    os.remove(zip_dir)
    # move the csv to the main folder
    os.replace("data\WC\worldcities.csv", "data\worldcities.csv")
    # remove folder
    shutil.rmtree('data\WC')
    print("Ciudades Mundo: Actualizado")
except FileNotFoundError:
    print("Ciudades Mundo: El zip ya se ha descomprimido o aún no se ha cargado")