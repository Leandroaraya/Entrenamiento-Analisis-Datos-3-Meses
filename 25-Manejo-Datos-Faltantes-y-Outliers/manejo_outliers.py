#**Ejercicio**: Pipeline completo de manejo de datos faltantes y outliers
import pandas as pd
import numpy as np
from scipy import stats

# Crear dataset con missing values y outliers
np.random.seed(42)
n = 1000

datos = pd.DataFrame({
    'id': range(1, n+1),
    'edad': np.random.normal(35, 10, n).clip(18, 80),  # Normal con límites. media 35, desv stand 10, si menor a 18 lo pasa a 18, mayor a 80 pasa a ser 80
    'salario': np.random.lognormal(10, 0.5, n),  # Distribución log-normal. media 10, des stand 0.5
    'horas_trabajo': np.random.normal(40, 5, n).clip(20, 60),
    'satisfaccion': np.random.randint(1, 6, n),#valores enteros aleatorios dentro de un rango específico.
    'departamento': np.random.choice(['IT', 'Ventas', 'Marketing', 'HR'], n) # valores aleatorios elegidos de una lista de opciones
})
datos_describe1=datos.describe()
datos1=datos
# Introducir missing values
mask_missing = np.random.random(n) < 0.1  # 10% missing
datos.loc[mask_missing, 'salario'] = np.nan

mask_missing_horas = np.random.random(n) < 0.05  # 5% missing
datos.loc[mask_missing_horas, 'horas_trabajo'] = np.nan

# Introducir outliers
outlier_indices = np.random.choice(n, 20, replace=False)
datos.loc[outlier_indices[:10], 'salario'] = datos.loc[outlier_indices[:10], 'salario'] * 10  # Salarios extremos altos
datos.loc[outlier_indices[10:], 'horas_trabajo'] = np.random.choice([80, 90, 100], 10)  # Horas imposibles

print(f"Dataset creado: {datos.shape}")
print(f"Valores faltantes por columna:\n{datos.isnull().sum()}")






import pandas as pd
import missingno as msno
import matplotlib.pyplot as plt

# -----------------------------
# 1. Porcentaje de datos faltantes por columna
# -----------------------------
print("Porcentaje de datos faltantes por columna:")
print((datos.isnull().sum() / len(datos) * 100).round(2))

# -----------------------------
# 2. Visualización de missing values
# -----------------------------
msno.matrix(datos)
plt.show()

# -----------------------------
# 3. Missing values por departamento
# -----------------------------
# Seleccionar columnas a analizar (excluyendo 'departamento')
cols_a_analizar = datos.columns.difference(['departamento'])

# Calcular NaN por departamento
missing_por_depto = datos.groupby('departamento')[cols_a_analizar].apply(lambda x: x.isnull().sum())

print("\nMissing values por departamento:")
print(missing_por_depto)

print("------------------------------------------------------")
print(datos)

#Imputación de valores faltantes:
# Imputación por media para horas_trabajo
media_horas = datos['horas_trabajo'].mean()
datos['horas_trabajo'] = datos['horas_trabajo'].fillna(media_horas)

# Imputación por mediana para salario (más robusto a outliers)
mediana_salario = datos['salario'].median()
datos['salario'] = datos['salario'].fillna(mediana_salario)

# Verificar que no queden missing values
print(f"\nValores faltantes después de imputación: {datos.isnull().sum().sum()}")
print((datos.isnull().sum() / len(datos) * 100).round(2))







#Detección de outliers:
# Función para detectar outliers usando IQR
def detectar_outliers_iqr(data, columna):
    Q1 = data[columna].quantile(0.25)
    Q3 = data[columna].quantile(0.75)
    IQR = Q3 - Q1
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR
    return (data[columna] < limite_inferior) | (data[columna] > limite_superior)

# Detectar outliers en salario y horas
outliers_salario = detectar_outliers_iqr(datos, 'salario')
outliers_horas = detectar_outliers_iqr(datos, 'horas_trabajo')

print(f"\nOutliers detectados:")
print(f"Salario: {outliers_salario.sum()} ({outliers_salario.mean()*100:.1f}%)")
print(f"Horas trabajo: {outliers_horas.sum()} ({outliers_horas.mean()*100:.1f}%)")







# Para horas_trabajo: cap at reasonable maximum
max_horas_normales = 60
datos.loc[datos['horas_trabajo'] > max_horas_normales, 'horas_trabajo'] = max_horas_normales

# Para salario: transformar usando log (más robusto)
datos['salario_log'] = np.log1p(datos['salario'])

# Comparar estadísticas antes y después
print(f"\nEstadísticas de salario original:")
print(datos['salario'].describe().round(2))

print(f"\nEstadísticas de salario transformado (log):")
print(datos['salario_log'].describe().round(2))

# Verificar reducción de outliers
outliers_salario_log = detectar_outliers_iqr(datos, 'salario_log')
print(f"\nOutliers en salario log-transformado: {outliers_salario_log.sum()}")


#comparar estadisticas antes y despues
datos_describe2=datos.describe()
datos2=datos

print("+++++++++++++++++++++++++++++*******+++++++++++++++++++++++++++++++++")
print(datos_describe1)
print(datos_describe2)
#salario y horas de trabajo cambiaron sus estadisticas al ahora tener valores
import pandas as pd

import pandas as pd
import matplotlib.pyplot as plt

# Suponiendo que 'df' tiene varias columnas numéricas
datos1.hist(bins=15, figsize=(12, 8), color='skyblue', edgecolor='black')
plt.suptitle("Distribución de columnas numéricas")
plt.show()
# Suponiendo que 'df' tiene varias columnas numéricas
datos2.hist(bins=15, figsize=(12, 8), color='skyblue', edgecolor='black')
plt.suptitle("Distribución de columnas numéricas")
plt.show()


"""
para graficar lo anterior pero todo en una sola imagen
import matplotlib.pyplot as plt

# Columnas numéricas (si quieres seleccionar automáticamente)
num_cols1 = datos1.select_dtypes(include='number').columns
num_cols2 = datos2.select_dtypes(include='number').columns

# Número total de filas para los subplots
n1 = len(num_cols1)
n2 = len(num_cols2)

# Crear figura grande
fig, axes = plt.subplots(nrows=max(n1,n2), ncols=2, figsize=(15, max(n1,n2)*3))

# Ajuste si hay solo 1 fila
if max(n1,n2) == 1:
    axes = [axes]

# Graficar datos1 en la primera columna
for i, col in enumerate(num_cols1):
    axes[i][0].hist(datos1[col], bins=15, color='skyblue', edgecolor='black')
    axes[i][0].set_title(f'datos1: {col}')

# Graficar datos2 en la segunda columna
for i, col in enumerate(num_cols2):
    axes[i][1].hist(datos2[col], bins=15, color='lightgreen', edgecolor='black')
    axes[i][1].set_title(f'datos2: {col}')

plt.tight_layout()
plt.suptitle("Distribución de columnas numéricas de datos1 y datos2", y=1.02)
plt.show()

"""