import numpy as np
import matplotlib.pyplot as plt

n = 1000

# Distribución normal
normal_data = np.random.normal(35, 10, n)

# Distribución log-normal
lognormal_data = np.random.lognormal(10, 0.5, n)

# Crear histogramas
plt.figure(figsize=(12,5))

plt.subplot(1,2,1)
plt.hist(normal_data, bins=30, color='skyblue', edgecolor='black')
plt.title('Distribución Normal')
plt.xlabel('Valor')
plt.ylabel('Frecuencia')

plt.subplot(1,2,2)
plt.hist(lognormal_data, bins=30, color='salmon', edgecolor='black')
plt.title('Distribución Log-Normal')
plt.xlabel('Valor')
plt.ylabel('Frecuencia')

plt.tight_layout()

# Guardar la figura como PNG
plt.savefig("comparacion_normal_lognormal.png", dpi=300)

# Mostrar la figura
plt.show()
