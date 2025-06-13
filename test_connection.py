# main.py
from database import Database

# Crear una instancia de la clase Database
db = Database()
db.connect()

# Verificar si la conexión fue exitosa
if db.is_connected():
    print("La conexión a la base de datos fue exitosa.")
else:
    print("No se pudo establecer la conexión a la base de datos.")

# Ejemplo de una consulta a la base de datos
if db.is_connected():
    try:
        db.cursor.execute("SELECT version();")
        db_version = db.cursor.fetchone()
        print(f"Versión de PostgreSQL: {db_version[0]}")
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

# Cerrar la conexión
db.disconnect()