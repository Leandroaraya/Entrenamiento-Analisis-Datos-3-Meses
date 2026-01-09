import sqlite3

# 1. Conectarse (crea el archivo si no existe)
conn = sqlite3.connect("mi_base.db")

# 2. Crear cursor
cursor = conn.cursor()

# 3. Crear tabla
cursor.execute("""
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    edad INTEGER
)
""")

# 4. Guardar cambios
conn.commit()

# 5. Cerrar conexión
conn.close()

print("Base de datos y tabla creadas correctamente")
