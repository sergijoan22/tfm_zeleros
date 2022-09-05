# TRANSFORMA LOS ARCHIVOS PAR ALUEGO SER SUBIDOS A BIGQUERY

# librerias
import pandas as pd
from datetime import datetime
import os
import shutil


# CIUDADES ARABIA Y AUSTRALIA
# read file
cities_world = pd.read_csv('data/worldcities.csv', sep=",", decimal = '.')
# filter to Arabia and Australia cities
cities = cities_world.loc[cities_world['country'].isin(['Saudi Arabia', 'Australia'])]
# choose columns
cities = cities[["city_ascii", "lat", "lng", "population"]]
# columna con el nombre completo
cities['full_name'] = cities.apply(lambda x: x['city_ascii'] + ', ' + 'SA', axis=1)
# renombrar columnas
column_names = {'city_ascii':'name',
    "lat":"latitud",
    "lng":"longitude"}
cities.rename(columns = column_names, inplace = True)

# borrar la carpeta data
#shutil.rmtree('data')


# AEROPUERTOS
#! leer archivo (da error porque el archivo original se guarda desde la web en binario)
airports_data = pd.read_csv(r'data\airports.csv')
# eliminar columnas
columnas_elegir = ["iata_code", "name", "latitude_deg", "longitude_deg",
                   "municipality", "iso_country", "continent"]
airports_data2 = airports_data[columnas_elegir]
# filtrar por los de europa, australia y arabia.
# de europa se descartan si están mas al este de Moscú
# se descartan los aeropuertos sin código iata ya que son helipuertos, etc.
airports_data3 = airports_data2.loc[airports_data2.iata_code.notnull()]
airports_data3 = airports_data3.loc[airports_data3.iata_code != '0']
airports_data3 = airports_data3.loc[((airports_data3.continent == "EU")
                                     & (airports_data3.longitude_deg.between(-25, 40))
                                     & (airports_data3.latitude_deg.between(25, 75)))
                                    | (airports_data3.iso_country.isin(["SA", "AU"]))]
# hay un aeropuerto con registro duplicado
airports_data3.drop_duplicates(inplace = True)
#renombrar columnas
column_names = {'latitude_deg':'airport_latitude',
    "longitude_deg":"airport_longitude",
    "iata_code":"airport_id",
    "name":"airport_name",
    "iso_country": "airport_country_iso",
    "continent": "airport_continent_iso",
    "municipality": "airport_city"
    }
airports_data3.rename(columns = column_names, inplace = True)
print(airports_data3.head(10))
# VUELOS ESPAÑA
flights_spain = pd.read_csv('data_in\F\ES\F_ES.txt', sep=",", low_memory=False)
