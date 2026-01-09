import sqlite3

# 1. Conectar a la base de datos existente
conn = sqlite3.connect("mi_base.db")
cursor = conn.cursor()

# 2. Datos a insertar (lista de tuplas)
usuarios = [
    ("Ana", 25),
    ("Carlos", 30),
    ("María", 28),
    ("Juan", 35),
    ("Sofía", 22)
]

# 3. Insertar múltiples registros
cursor.executemany("""
INSERT INTO usuarios (nombre, edad)
VALUES (?, ?)
""", usuarios)

# 4. Guardar cambios
conn.commit()

# 5. Cerrar conexión
conn.close()

print("Datos insertados correctamente")
