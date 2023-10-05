def incremental_load(df, schema_name, table_name, con):
    # Escribo datos nuevos
    df.to_sql(table_name, schema=schema_name, con=con, if_exists='append', index=False)

    print(f"Se escribieron {len(df)} lineas en la tabla {table_name}")
