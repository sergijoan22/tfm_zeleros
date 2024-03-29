import os
import pandas
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.cloud import storage
from numpy.core import *
from pandas import unique
import pandas_gbq # para interactuar con BQ



#Función para conexión con GCP y pasar los csv:
def connect_bigquery(credentials_path):
    from google.oauth2 import service_account # para generar conexion con BigQuery
    bq_cred = service_account.Credentials.from_service_account_file(credentials_path)
    return bq_cred

#Creamos la tabla en la DB si no existe en BQ, a partir del dataframe: mode puede ser replace o append
def df_toBQ(file, credentials_path, table_id, p_id):
    bq_cred = connect_bigquery(credentials_path)
    df = pd.read_csv(file)
    #pandas_gbq.to_gbq(df, table_id, project_id=p_id, if_exists=mode, credentials=bq_cred)
    pandas_gbq.to_gbq(df, table_id, project_id=p_id, credentials=bq_cred, if_exists='append')
#Función que lee los csv:
def create_df(event, context):
    client = storage.Client()
    print('event',event)
    my_bucket = 'upload_csv_data_europe'
    filename = 'gs://upload_csv_data_europe/' + event['name']
    #print("Leyendo", filename)
    df = pd.read_csv(filename, sep='\t')
    # columns_values = df.columns.values
    columns_delete = [s for s in df.columns if "Q" in s]
    for col in columns_delete:
        del (df[col])
    # df.drop(columns_delete, axis=1) #axis = 1 para eliminar las columnas, 0 para eliminar filas
    #print(df.columns)
    # columns_names_list = list(columns_names)
    # print(columns_names)
    columns_delete = [s for s in df.columns if "M" not in s]
    #print(columns_delete)
    columns_delete = columns_delete[1:]
    #print(columns_delete)
    for col in columns_delete:
        del (df[col])

    #print(df.columns)
    first_col = df.columns[0]
    df[['category', 'second', 'third']] = df[first_col].str.split(pat=',', expand=True)
    df[['ORIGIN_COUNTRY', 'ORIGIN_AIRPORT', 'DEST_COUNTRY', 'DEST_AIRPORT']] = df['third'].str.split(pat='_', expand=True)
    # print(df.head)

    # VAMOS A QUEDARNOS CON LOS PASAJEROS PAS_CRD_DEP (ON BOARD) YA QUE SON LOS QUE VAN DE PUNTO A PUNTO Y ADEMÁS SI COGEMOS DEPARTURE Y ARRIVAL ESTARÍAMOS DUPLICANDO LOS RESULTADOS CUANDO UTILICEMOS EL PAIS O CIUDAD DESTINO

    indexNames = df[df['second'] == 'PAS_BRD'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'PAS_BRD_ARR'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'PAS_BRD_DEP'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'PAS_CRD_ARR'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'PAS_CRD'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'CAF_PAS'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'CAF_PAS_ARR'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'ST_PAS'].index
    df.drop(indexNames, inplace=True)

    indexNames = df[df['second'] == 'ST_PAS_ARR'].index
    df.drop(indexNames, inplace=True)

    #print(df.columns)

    c_years = [s for s in df.columns if "20" in s]
    columns_years = c_years
    years = ['2018', '2019', '2020', '2021', '2022']
    #years = ['2021', '2022']
    year_del =[]
    for column_year in c_years:
        #print(column_year[0:4])
        if column_year[0:4] not in years:
            year_del.append(column_year)
    for year in year_del:
        del (df[year])
        
    

    #print('Leemos: ', columns_years)

    df.reset_index(drop=True, inplace=True)  # reindexamos para que las filas tengan el orden correcto
    #print(len(df))
    #print(df.head(20))

    #Delete the string ' b' from the digits and replace ':' to 0:
    columns_years = [s for s in df.columns if "20" in s]
    for column_year in columns_years:
        print('column_year ', column_year)
        for i in df.index:
            if df[column_year][i] == ': ':
                df[column_year][i] = 0
            if (type(df[column_year][i]) == str) and ('b' in df[column_year][i]) :
                #print('Borro la b')
                df[column_year][i] = df[column_year][i].rstrip(' b')

    #Creamos un nuevo dataframe y añadimos los valores que deseamos a su respectiva columna:
    df_BQ = pd.DataFrame()
    for column_year in columns_years:
        for i in df.index:
            j = {}
            j['ORIGIN_COUNTRY'] = df['ORIGIN_COUNTRY'][i]
            j['ORIGIN_AIRPORT'] = df['ORIGIN_AIRPORT'][i]
            j['DEST_COUNTRY'] = df['DEST_COUNTRY'][i]
            j['DEST_AIRPORT'] = df['DEST_AIRPORT'][i]
            j['YEAR'] = column_year[0:4]
            j['MONTH'] = column_year[5:7]
            fill_row = False
            if df['category'][i] == 'FLIGHT':
                j['NUM_FLIGHTS'] = df[column_year][i]
                fill_row = True
            else:
                j['NUM_FLIGHTS'] = 0

            if df['category'][i] == 'PAS':
                j['NUM_PASSENGERS'] = df[column_year][i]
                fill_row = True
            else:
                j['NUM_PASSENGERS'] = 0

            if df['category'][i] == 'SEAT':
                j['NUM_SEATS'] = df[column_year][i]
                fill_row = True
            else:
                j['NUM_SEATS'] = 0    

            if fill_row and df[column_year][i] != 0:
                index_df_BQ = len(df_BQ.index) + 1
                #print(index_df_BQ)
                dfj = pd.DataFrame(j, index={index_df_BQ})
                
                df_list = [df_BQ, dfj]
                df_BQ = pd.concat(df_list)
                #print('columna ', column_year, ' insertada')
            #else:
                #print('No inserta')
    #print('fin bucle')
    '''data = df_BQ.columns
    print(data)'''
    #Esta cloud function es de solo lectura, por ello comentamos esta parte que es para ver el csv que crea: df_BQ.to_csv('df_BQ.csv', index=False)

    df_BQ_wo_duplicity = pd.DataFrame()
    for k in df_BQ.groupby(['ORIGIN_COUNTRY', 'ORIGIN_AIRPORT', 'DEST_COUNTRY', 'DEST_AIRPORT', 'YEAR', 'MONTH']).groups.keys():
        #print(k[0])
        #data = df_BQ.groupby(['ORIGIN_COUNTRY', 'ORIGIN_AIRPORT', 'DEST_COUNTRY', 'DEST_AIRPORT', 'YEAR', 'MONTH']).groups.values()

        data = pd.DataFrame()
        data = df_BQ[(df_BQ['ORIGIN_COUNTRY']==k[0]) & (df_BQ['ORIGIN_AIRPORT']==k[1]) & (df_BQ['DEST_COUNTRY']==k[2]) & (df_BQ['DEST_AIRPORT']==k[3]) & (df_BQ['YEAR']==k[4]) & (df_BQ['MONTH']==k[5])]
        #print('suma',data[])
        #print('data', data)

        data_flights = 0
        data_seats = 0
        data_pas = 0
        for i in data.index:
            data_flights = int(data['NUM_FLIGHTS'][i]) + data_flights
            data_pas = int(data['NUM_PASSENGERS'][i]) + data_pas
            data_seats = int(data['NUM_SEATS'][i]) + data_seats


        j = {}
        j['ORIGIN_COUNTRY'] = k[0]
        #print('Imprime el valor de k: ', k)
        #print(k[0])
        j['ORIGIN_AIRPORT'] = k[1]
        j['DEST_COUNTRY'] = k[2]
        j['DEST_AIRPORT'] = k[3]
        j['YEAR'] = k[4]
        j['MONTH'] = k[5]
        j['NUM_FLIGHTS'] = data_flights
        j['NUM_PASSENGERS'] = data_pas
        j['NUM_SEATS'] = data_seats
        j['TONNES'] = 0
        #print(j)
        index_df_BQ_wo_duplicity = len(df_BQ_wo_duplicity.index) + 1
        # print(index_df_BQ)
        dfj = pd.DataFrame(j, index={index_df_BQ_wo_duplicity})
        
        df_list = [df_BQ_wo_duplicity, dfj]
        df_BQ_wo_duplicity = pd.concat(df_list)

    print('se crea fichero csv con los datos a insertar en bigquery')
    df_BQ_wo_duplicity.to_csv('gs://upload_csv_data_europe/df_BQ_wo_duplicity.csv', index=False)
    print('creado')
    #df_BQ_wo_duplicity.to_csv('df_BQ_wo_duplicity.csv', index=False)
    credentials_path = 'private_key_gcp_tfm.json'
    table_id = 'EUROPE_DATA.FLIGHTS'
    p_id = 'fine-nimbus-359211'
    #mode = 'append'
    print('se lanza inserción en bigquery')
    df_toBQ('gs://upload_csv_data_europe/df_BQ_wo_duplicity.csv', credentials_path, table_id, p_id)
    print('Finalizado ', filename)