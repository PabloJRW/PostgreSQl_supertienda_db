import psycopg2


# Configuracion de la conexion a PostgreSQL
HOST='localhost',
USER='postgres',
PASSWORD='base',
DATABASE='Supertienda'

try:
    conn=psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    conn.cursor()
    print("Conexión Exitosa...")

except Exception as e:
    print("Error al conectar a PostgreSQL: {e}")
    exit()


