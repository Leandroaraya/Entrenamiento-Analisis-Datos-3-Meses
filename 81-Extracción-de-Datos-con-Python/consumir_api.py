import requests

# Llamada simple a API
response = requests.get('https://jsonplaceholder.typicode.com/users')
if response.status_code == 200:
    datos = response.json()
    print(f"Obtenidos {len(datos)} registros")
print(datos[0])
