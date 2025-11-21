#**Ejercicio**: Implementar carga completa con validaciones y estrategias avanzadas

#Crear esquema de base de datos destino: aqui se creara un archivo .db 
# que es la base de dato en donde se guardara la informacion limpia


import sqlite3
import pandas as pd
import numpy as np

# Crear base de datos
conn = sqlite3.connect('ventas_etl.db')

# Crear tablas con constraints:  se usara IF NOT EXISTS para evitar problemas al ejecutar varias veces al ir por parte
conn.execute('''
    CREATE TABLE IF NOT EXISTS clientes ( 
        id_cliente INTEGER PRIMARY KEY,
        nombre TEXT NOT NULL,
        email TEXT UNIQUE,
        ciudad TEXT,
        fecha_registro DATE
    )
''')

conn.execute('''
    CREATE TABLE IF NOT EXISTS productos (
        id_producto INTEGER PRIMARY KEY,
        nombre TEXT NOT NULL,
        precio REAL NOT NULL,
        categoria TEXT
    )
''')

conn.execute('''
    CREATE TABLE IF NOT EXISTS ventas (
        id_venta INTEGER PRIMARY KEY,
        id_cliente INTEGER,
        id_producto INTEGER,
        cantidad INTEGER NOT NULL,
        precio_unitario REAL NOT NULL,
        fecha_venta DATE,
        FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente),
        FOREIGN KEY (id_producto) REFERENCES productos(id_producto)
    )
''')

conn.commit()


"""
¿Qué significa “Crear esquema de base de datos destino”?
Cuando hacemos un proceso ETL, normalmente tomamos datos desde una fuente (archivos, APIs, etc.) y los cargamos dentro
de una base de datos destino (un “Data Warehouse” o una BD limpia y estructurada).
👉 “Crear el esquema de la base de datos destino” significa:
Crear la base de datos donde se guardarán los datos transformados.
Crear las tablas que almacenarán esa data.
Definir las columns, tipos de datos y constraints (reglas de integridad).

En otras palabras:
📦 Es el “lugar final” donde vas a guardar tus datos ya limpios.
🏗️ Y el esquema es la “estructura” de ese lugar.

hasta ahora se crean las tablas formateadas finales que se le añadiran la informacion limpia y asi pasar a dashboards,etc.
"""
#-------------------------------------------------------------------------------------------------------

#Crear datos de ejemplo para carga: en este caso se crean datos limpios para pasar a las tablas
# Datos de clientes
clientes_df = pd.DataFrame({
    'id_cliente': range(1, 6),
    'nombre': ['Ana García', 'Carlos López', 'María Rodríguez', 'Juan Pérez', 'Luis Martín'],
    'email': ['ana@email.com', 'carlos@email.com', 'maria@email.com', 'juan@email.com', 'luis@email.com'],
    'ciudad': ['Madrid', 'Barcelona', 'Madrid', 'Valencia', 'Sevilla'],
    'fecha_registro': pd.date_range('2023-01-01', periods=5, freq='MS')
})

# Datos de productos
productos_df = pd.DataFrame({
    'id_producto': range(101, 106),
    'nombre': ['Laptop', 'Mouse', 'Teclado', 'Monitor', 'Audífonos'],
    'precio': [1200, 25, 80, 300, 150],
    'categoria': ['Electrónica', 'Accesorios', 'Accesorios', 'Electrónica', 'Audio']
})

# Datos de ventas (con algunos errores intencionales)
np.random.seed(42)
ventas_df = pd.DataFrame({
    'id_venta': range(1, 21),
    'id_cliente': np.random.choice(range(1, 8), 20),  # Algunos IDs inexistentes
    'id_producto': np.random.choice(range(101, 108), 20),  # Algunos IDs inexistentes
    'cantidad': np.random.randint(1, 5, 20),
    'precio_unitario': np.random.choice([1200, 25, 80, 300, 150], 20),
    'fecha_venta': pd.date_range('2024-01-01', periods=20, freq='D')
})
#aqui solo tenemos dataframe con la estructura para añadirla a las tablas anteriores
#-------------------------------------------------------------------------------------

#Implementar carga con validaciones:

# Función para cargar con validaciones
def cargar_con_validacion(df, tabla, conn, claves_foraneas=None):
    try:
        # Validar claves foráneas si se especifican
        if claves_foraneas:
            for columna, tabla_ref, columna_ref in claves_foraneas:
                valores_validos = pd.read_sql(f'SELECT {columna_ref} FROM {tabla_ref}', conn)
                valores_validos = valores_validos[columna_ref].tolist()
                
                invalidos = ~df[columna].isin(valores_validos)
                if invalidos.any():
                    print(f"Advertencia: {invalidos.sum()} registros en {columna} no existen en {tabla_ref}")
                    # Opción: filtrar inválidos o marcar como NULL
                    df = df[~invalidos]  # Filtrar inválidos
        
        # Cargar datos
        df.to_sql(tabla, conn, index=False, if_exists='append')
        print(f"✓ Cargados {len(df)} registros en {tabla}")
        return True
        
    except Exception as e:
        print(f"✗ Error cargando {tabla}: {e}")
        return False

# Cargar tablas base (sin dependencias)
exito_clientes = cargar_con_validacion(clientes_df, 'clientes', conn)
exito_productos = cargar_con_validacion(productos_df, 'productos', conn)

# Cargar ventas con validaciones de FK
if exito_clientes and exito_productos:
    claves_ventas = [
        ('id_cliente', 'clientes', 'id_cliente'),
        ('id_producto', 'productos', 'id_producto')
    ]
    cargar_con_validacion(ventas_df, 'ventas', conn, claves_ventas)


#-------------------------------------------------------------------------------------------

#Verificar carga y ejecutar consultas:

# Verificar conteos
for tabla in ['clientes', 'productos', 'ventas']:
    count = pd.read_sql(f'SELECT COUNT(*) FROM {tabla}', conn).iloc[0,0]
    print(f"{tabla}: {count} registros")

# Consulta de ejemplo: ventas por cliente
query_result = pd.read_sql('''
    SELECT c.nombre, COUNT(v.id_venta) as num_ventas, 
           SUM(v.cantidad * v.precio_unitario) as total_ventas
    FROM clientes c
    LEFT JOIN ventas v ON c.id_cliente = v.id_cliente
    GROUP BY c.id_cliente, c.nombre
    ORDER BY total_ventas DESC
''', conn)

print("\nVentas por cliente:")
print(query_result)

#------------------------------------------------------------------------------------------
#formas de verificacion

for tabla in ['clientes', 'productos', 'ventas']:
    count = pd.read_sql(f'SELECT COUNT(*) FROM {tabla}', conn).iloc[0,0]
    print(f"{tabla}: {count} registros")








def verificar_integridad(conn):
    errores = 0

    # Clientes faltantes
    invalid_cli = pd.read_sql("""
        SELECT COUNT(*) as invalidos
        FROM ventas v
        LEFT JOIN clientes c ON v.id_cliente = c.id_cliente
        WHERE c.id_cliente IS NULL
    """, conn)['invalidos'][0]

    # Productos faltantes
    invalid_prod = pd.read_sql("""
        SELECT COUNT(*) as invalidos
        FROM ventas v
        LEFT JOIN productos p ON v.id_producto = p.id_producto
        WHERE p.id_producto IS NULL
    """, conn)['invalidos'][0]

    print("🔎 Verificación de integridad referencial:")
    print(f" - Ventas con cliente inexistente: {invalid_cli}")
    print(f" - Ventas con producto inexistente: {invalid_prod}")

    if invalid_cli == 0 and invalid_prod == 0:
        print(" Integridad correcta: No hay claves inválidas.")
    else:
        print(" Hay problemas de integridad.")

verificar_integridad(conn)








conn.close()

