SELECT * FROM clientes;

CREATE TABLE pedidos (
    id INTEGER PRIMARY KEY,
    cliente_id INTEGER,
    fecha_pedido DATE NOT NULL,
    total REAL,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

INSERT INTO pedidos VALUES
(1, 1, '2024-01-15', 150.50),
(2, 1, '2024-01-20', 89.99),
(3, 2, '2024-01-18', 299.99);

SELECT * FROM clientes;
SELECT * FROM pedidos;

-- INNER JOIN: Solo clientes con pedidos
SELECT c.nombre, p.fecha_pedido, p.total
FROM clientes c
INNER JOIN pedidos p ON c.id = p.cliente_id;


-- LEFT JOIN: Todos los clientes, con pedidos si existen
SELECT c.nombre, COUNT(p.id) as num_pedidos, SUM(p.total) as total_compras
FROM clientes c
LEFT JOIN pedidos p ON c.id = p.cliente_id
GROUP BY c.id, c.nombre;

-- Clientes de Madrid con sus pedidos
SELECT c.nombre, c.ciudad, p.fecha_pedido, p.total
FROM clientes c
LEFT JOIN pedidos p ON c.id = p.cliente_id
WHERE c.ciudad = 'Madrid';

PRAGMA foreign_keys = ON;
INSERT INTO pedidos VALUES (10, 99, '2024-05-10', 50);
--aqui lanza error porque cliente_id esta relacionado con clientes y id=99 no existe en clientes
-- por lo tanto no puede agregarlo a id_cliente de pedidos
DELETE FROM pedidos WHERE id = 10;
-- para eliminar una fila de pedidos al no estar habilitada la llave forania.