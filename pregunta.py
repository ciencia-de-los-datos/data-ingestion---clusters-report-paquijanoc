"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    cols= ['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave']
    cluster1=pd.read_fwf(
        "clusters_report.txt",
        colspecs="infer",
        widths=[9, 16, 16],
        names=cols,
    )

    df=cluster1[3:]

    return df
