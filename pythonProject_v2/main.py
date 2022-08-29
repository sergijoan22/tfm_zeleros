# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


import os
import pandas as pd


def read_files_to_bbdd():
    folder_files = '/Users/jaimetorresnatividad/Desktop/prueba'
    files = os.listdir(folder_files)
    years = ['2019', '2020', '2021', '2022']
    for filename in files:
        print("Leyendo", filename)
        print(os.path.join(folder_files, filename))
        df = pd.read_csv(os.path.join(folder_files, filename), sep='\t')
        # columns_values = df.columns.values
        columns_delete = [s for s in df.columns if "Q" in s]
        for col in columns_delete:
            del (df[col])
        # df.drop(columns_delete, axis=1) #axis = 1 para eliminar las columnas, 0 para eliminar filas
        print(df.columns)
        # columns_names_list = list(columns_names)
        # print(columns_names)
        columns_delete = [s for s in df.columns if "M" not in s]
        print(columns_delete)
        columns_delete = columns_delete[1:]
        print(columns_delete)
        for col in columns_delete:
            del (df[col])

        first_col = df.columns[0]
        df[['category', 'second', 'third']] = df[first_col].str.split(pat=',', expand=True)
        df[['ORIGIN_COUNTRY', 'ORIGIN_AIRPORT', 'DEST_COUNTRY', 'DEST_AIRPORT']] = df['third'].str.split(pat='_', expand=True)
        print(df.head)

        # VAMOS A QUEDARNOS CON LOS PASAJEROS PAS_BRD_DEP (ON BOARD) YA QUE SON LOS QUE VAN DE PUNTO A PUNTO Y ADEMÁS SI COGEMOS DEPARTURE Y ARRIVAL ESTARÍAMOS DUPLICANDO LOS RESULTADOS CUANDO UTILICEMOS EL PAIS O CIUDAD DESTINO

        indexNames = df[df['second']=='PASS_BRD'].index
        df.drop(indexNames, inplace=True)

        indexNames = df[df['second'] == 'PASS_BRD_ARR'].index
        df.drop(indexNames, inplace=True)

        indexNames = df[df['second'] == 'CAF_PAS'].index
        df.drop(indexNames, inplace=True)

        indexNames = df[df['second'] == 'CAF_PAS_ARR'].index
        df.drop(indexNames, inplace=True)

        indexNames = df[df['second'] == 'ST_PAS'].index
        df.drop(indexNames, inplace=True)

        indexNames = df[df['second'] == 'ST_PAS_ARR'].index
        df.drop(indexNames, inplace=True)

        print(df.columns)

        columns_years = [s for s in df.columns if "20" in s]
        print('Leemos: ', columns_years)

        df.reset_index(drop=True, inplace=True) #reindexamos para que las filas tengan el orden correcto
        print(len(df))
        print(df.head(20))
        #print('Vemos un extracto: ', df.head(20))
        # Build a new dataframe in order to upload it to the database:

        #df_bbdd = ['ORIGIN_COUNTRY', 'ORIGIN_AIRPORT', 'DEST_COUNTRY', 'DEST_AIRPORT', 'NUM_FLIGHTS', 'YEAR', 'MONTH','NUM_PASSENGERS', 'NUM_SEATS']
        # for column_year in columns_years:

        # Press the green button in the gutter to run the script.


if __name__ == '__main__':
    read_files_to_bbdd()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
