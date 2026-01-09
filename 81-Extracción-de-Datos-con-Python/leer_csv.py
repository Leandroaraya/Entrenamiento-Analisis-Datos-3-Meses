import csv

# Leer archivo CSV
with open('datos.csv', 'r') as archivo:
    lector = csv.DictReader(archivo)
    for fila in lector:
        print(f"Nombre: {fila['nombre']}, Edad: {fila['edad']}")