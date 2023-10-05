# up-python-pandas-etl-example

Ejemplo de ETL que lee CSVs y carga un DW modelado dimensionalmente

## Instrucciones

### Ejecutar container postgresql

```
docker run --name postgres-up --env=POSTGRES_PASSWORD=password --volume=postgres-up-volume:/var/lib/postgresql/data -p 5432:5432 -d postgres
```

### Conectarse a postgresql con algun cliente (ej DBeaver)

```
host: localhost
port: 5432
username: postgres
password: password
database: postgres
```
### Ejecutar sentencias SQL de creacion de tablas

Disponible en este repositorio en `sql/ddl_create_tables.sql`

### Crear entorno virtual de python

```
python -m venv .venv
```

### Activar entorno virtual de python

Windows:
```
.venv\Scripts\activate
```

Linux:
```
source .venv/bin/activate
```

### Instalar dependencias

```
pip install -r requirements.txt
```

### Ejecutar ETL

```
python main.py
```