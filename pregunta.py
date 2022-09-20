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
    
    cluster1=pd.read_fwf(
        "https://raw.githubusercontent.com/ciencia-de-los-datos/data-ingestion---clusters-report-paquijanoc/main/clusters_report.txt",
        widths=[9, 16, 16,84],
    )

    #Se eligen los datos unicamente y se ponen los titulos
    df=cluster1[2:]
    df.columns=['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave','principales_palabras_clave']

    #Se reemplazan los nan con los valores necesarios para cluster
    clusterlist=list(df.cluster)

    lista=[]
    for x in clusterlist:
      y=x
      if str(y) != 'nan':
        lista.append(int(x))
        z=y
      else:
          lista.append(int(z))

    df=df.assign(cluster=lista)


    #Se reemplazan los nan con los valores necesarios para cantidad_de_palabras_clave
    cantidad_de_palabras_clavelist=list(df.cantidad_de_palabras_clave)

    lista=[]
    for x in cantidad_de_palabras_clavelist:
      y=x
      if str(y) != 'nan':
        lista.append(int(x))
        z=y
      else:
          lista.append(int(z))

    df=df.assign(cantidad_de_palabras_clave=lista)

    #Se reemplazan los nan con los valores necesarios para porcentaje_de_palabras_clave
    porcentaje_de_palabras_clavelist=list(df.porcentaje_de_palabras_clave)

    lista=[]
    for x in porcentaje_de_palabras_clavelist:
      y=x
      if str(y) != 'nan':
        lista.append(x)
        z=y
      else:
          lista.append(z)

    df=df.assign(porcentaje_de_palabras_clave=lista)

    df = df.groupby(['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave'])['principales_palabras_clave'].apply(' '.join).reset_index()
    df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.strip('%')
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(",", ".").astype(float)

    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace(".", "", regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("   "," ",regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("  "," ",regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace("  "," ",regex=True)
    return df
