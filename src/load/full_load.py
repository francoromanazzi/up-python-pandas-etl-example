from sqlalchemy import text


def full_load(df, schema_name, table_name, con):
    # Deshabilito temporalmente chequeo de integridad referencial
    con.execution_options(isolation_level="AUTOCOMMIT").execute(text("SET session_replication_role = replica"))
    con.commit()

    # Borro datos existentes
    con.execute(text(f'DELETE FROM {schema_name}.{table_name};'))
    con.commit()

    # Escribo datos nuevos
    df.to_sql(table_name, schema=schema_name, con=con, if_exists='append', index=False)

    # Vuelvo a habilitar chequeo de integridad referencial
    con.execution_options(isolation_level="AUTOCOMMIT").execute(text("SET session_replication_role = DEFAULT"))
    con.commit()

    print(f"Se escribieron {len(df)} lineas en la tabla {table_name}")
