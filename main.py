import json
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from decouple import config
from sodapy import Socrata


def extraccion() -> pd.DataFrame:
    """
    intento 1 directamente
    """
    try:
        response = requests.get('https://datos.gov.co/resource/7cci-nggb.json')
        response.raise_for_status()
        api_response = response.json()
        return pd.DataFrame.from_records(api_response)

    except Exception as e:
        print('no fue posible extraer la base')

    """
    intento 2 con webscraping
    """
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get('https://datos.gov.co/resource/7cci-nggb.json')
        return pd.DataFrame.from_records(driver.page_source)

    except Exception as e:
        print('no fue posible extraer la base')

    """
    intento 3 con documentacion del portal datos.gov.co
    """
    try:
        MyAppToken = config('TOKEN')
        nombre_usuario = config('USUARIO')
        contrasenia = config('PASS')
        client = Socrata("www.datos.gov.co",
                        MyAppToken,
                        username=nombre_usuario,
                        password=contrasenia)
        results = client.get("7cci-nggb")
        return pd.DataFrame.from_records(results)
    
    except Exception as e:
        print('no fue posible extraer la base')

    """
    finalmente archivo descargado en csv
    """
    return pd.read_csv(r'7cci-nqqb.csv',sep=';')


def limpieza(accidentes_df) -> pd.DataFrame:
    accidentes_df['fecha'] = pd.to_datetime(accidentes_df['fecha'], errors='coerce', dayfirst=True)
    accidentes_df[['mes_numero', 'mes_texto']] = accidentes_df['mes'].str.split('. ', expand=True)
    accidentes_df['mes_numero'] = pd.to_numeric(accidentes_df['mes_numero'], errors='coerce')
    accidentes_df[['dia_numero', 'dia_semana']] = accidentes_df['d_a'].str.split('. ', expand=True)
    accidentes_df['dia_numero'] = pd.to_numeric(accidentes_df['dia_numero'], errors='coerce')

    return accidentes_df


def transformacion(accidentes_df) -> pd.DataFrame:
    df_gravedad = pd.crosstab(accidentes_df["dia_semana"], accidentes_df["gravedad"])

    df_vehiculos = accidentes_df.loc[:,'automovil':'otro']
    df_vehiculos['orden'] = accidentes_df['orden']
    df_vehiculos.set_index('orden',drop=True,inplace=True)

    return df_vehiculos, df_gravedad


if __name__ == '__main__':
    df_accidentes = extraccion()
    df_accidentes = limpieza(df_accidentes)
    table_vahiculos, table_gravedad = transformacion(df_accidentes)