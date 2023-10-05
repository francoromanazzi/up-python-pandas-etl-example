import configparser
import pandas as pd
from sqlalchemy import create_engine, text
from src.extract.extract_csv import extract_csv
from src.load.full_load import full_load
from src.load.incremental_load import incremental_load


def extract():
    print('Extrayendo datos de los CSV...')

    empresa_df = extract_csv('datasets/empresa.csv')
    encabezado_pagos_df = extract_csv('datasets/encabezado_pagos.csv')
    items_pagos_df = extract_csv('datasets/items_pagos.csv')
    medio_pago_df = extract_csv('datasets/medio_pago.csv')
    
    print(empresa_df)
    print(encabezado_pagos_df)
    print(items_pagos_df)
    print(medio_pago_df)

    return (
        empresa_df,
        encabezado_pagos_df,
        items_pagos_df,
        medio_pago_df
    )


def transform(config, empresa_df, encabezado_pagos_df, items_pagos_df, medio_pago_df):
    print('Aplicando transformaciones...')

    empresa_df = empresa_df.rename(columns={
        'id_Empresa': 'sk_empresa',
        'Descripcion': 'descripcion'
    })
    print(empresa_df)

    medio_pago_df = medio_pago_df.rename(columns={
        'id_MedioPago': 'sk_medio_pago',
        'Descripcion': 'descripcion'
    })
    print(medio_pago_df)

    start_date = '2020-01-01'
    end_date = '2030-12-31'
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    fecha_df = pd.DataFrame({'sk_fecha': date_range})
    fecha_df['sk_fecha'] = fecha_df['sk_fecha'].dt.strftime('%Y%m%d')

    engine = create_engine(config['DB']['CONN_STR'])
    with engine.connect() as con:
        last_date_ingested = con.execute(text(f"SELECT MAX(sk_fecha) FROM {config['DB']['SCHEMA']}.bt_pago")).scalar()
        last_date_ingested = last_date_ingested or '19000101'
        print(f'last_date_ingested for bt_pago: {last_date_ingested}')
        
    encabezado_pagos_df = encabezado_pagos_df[encabezado_pagos_df['FechaPago'].astype(str) > last_date_ingested]
    pago_df = pd.merge(encabezado_pagos_df, items_pagos_df, on='id_Pago', how='inner')
    pago_df = pago_df.rename(columns={
        'id_Pago': 'sk_pago',
        'id_Item': 'sk_item',
        'id_Empresa': 'sk_empresa',
        'FechaPago': 'sk_fecha',
        'id_MedioPago': 'sk_medio_pago'
    })
    print(pago_df)

    return (
        empresa_df,
        medio_pago_df,
        fecha_df,
        pago_df
    )


def load(config, empresa_df, medio_pago_df, fecha_df, pago_df):
    print('Cargando tablas del DW...')
    engine = create_engine(config['DB']['CONN_STR'])
    with engine.connect() as con:
        full_load(empresa_df, config['DB']['SCHEMA'], 'lk_empresa', con)
        full_load(medio_pago_df, config['DB']['SCHEMA'], 'lk_medio_pago', con)
        full_load(fecha_df, config['DB']['SCHEMA'], 'lk_fecha', con)
        incremental_load(pago_df, config['DB']['SCHEMA'], 'bt_pago', con)


def main():
    # Leer config
    config = configparser.ConfigParser()
    config.read('config.cfg')

    # Paso 1) Extraer CSVs
    (
        empresa_df,
        encabezado_pagos_df,
        items_pagos_df,
        medio_pago_df
    ) = extract()

    # Paso 2) Transformar los dataframes
    (
        empresa_df,
        medio_pago_df,
        fecha_df,
        pago_df
    ) = transform(
        config,
        empresa_df,
        encabezado_pagos_df,
        items_pagos_df,
        medio_pago_df
    )
    
    # Paso 3) Cargar en DW
    load(
        config,
        empresa_df,
        medio_pago_df,
        fecha_df,
        pago_df
    )
    
    print('El proceso ETL ha finalizado exitosamente')

if __name__== '__main__':
    main()
