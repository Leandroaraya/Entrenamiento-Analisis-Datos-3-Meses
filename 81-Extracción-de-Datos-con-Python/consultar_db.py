import sqlite3

# Conectarse a la base existente
conn = sqlite3.connect("mi_base.db")
cursor = conn.cursor()

# Ejecutar consulta
cursor.execute("SELECT * FROM usuarios")

# Mostrar resultados
print(cursor.fetchall())

conn.close()
