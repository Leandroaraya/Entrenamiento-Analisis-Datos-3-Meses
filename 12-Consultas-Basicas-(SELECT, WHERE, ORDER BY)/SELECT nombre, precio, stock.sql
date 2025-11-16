SELECT nombre, precio, stock
FROM productos
WHERE categoria = 'Electrónica'
  AND (precio < 200 OR stock > 10);

SELECT nombre, stock
FROM productos
ORDER BY stock DESC;

SELECT nombre, precio, categoria
FROM productos
WHERE categoria = 'Electrónica'
  AND precio > 100;