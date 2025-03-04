-- 1. Listar los usuarios que cumplan años el día de hoy cuya cantidad de ventas realizadas
-- en enero 2020 sea superior a 1500.
SELECT c.id_customer, c.nombre, c.apellido
FROM Customer AS c
JOIN Order AS o
    ON c.id_customer = o.id_customer
WHERE o.fecha_venta BETWEEN '2020-01-01' AND '2020-01-31'
    AND c.fecha_de_nacimiento = CURRENT_DATE() --dependiendo del DBMS, va con o sin paréntesis. Otra alternativa, NOW()::DATE
GROUP BY c.id_customer, c.nombre, c.apellido
HAVING COUNT(o.id_order) > 1500;

-- 2. Por cada mes del 2020, se solicita el top 5 de usuarios que más vendieron($) en la
-- categoría Celulares. Se requiere el mes y año de análisis, nombre y apellido del
-- vendedor, cantidad de ventas realizadas, cantidad de productos vendidos y el monto
-- total transaccionado.
WITH ventas_usuarios_categoria_celulares AS (
    SELECT EXTRACT('MONTH' FROM o.fecha_venta) AS mes, 
        EXTRACT('YEAR' FROM o.fecha_venta) AS ano, 
        cu.nombre, 
        cu.apellido,
        COUNT(o.id_order) AS cant_ventas,
        SUM(o.cantidad) AS cant_prod_vendidos,
        SUM(o.costo_total) AS monto
    FROM Order AS o
    JOIN Item AS i
        ON o.id_item = i.id_item
    JOIN Category AS c
        ON i.id_category = c.id_category
    JOIN Customer AS cu
        ON o.id_customer = cu.id_customer
    WHERE c.nombre_cat = 'Celulares'
        AND EXTRACT('YEAR' FROM o.fecha_venta) = 2020
    GROUP BY mes, ano, cu.nombre, cu.apellido
),
top_vendedores AS (
    SELECT *, RANK() OVER(PARTITION BY mes, ano ORDER BY monto DESC) AS rank
    FROM ventas_usuarios_categoria_celulares
)
SELECT mes, ano, nombre, apellido, cant_ventas, cant_prod_vendidos, monto
FROM top_vendedores
WHERE rank <= 5
ORDER BY mes, ano, rank;

-- 3. Calcular el % de venta ($) que representa cada categoría respecto del total vendido ($)
-- por día. Traer en la misma query la venta máxima y mínima de la fecha.
WITH total_vendido_por_dia AS (
    SELECT fecha_venta AS fecha, SUM(costo_total) AS total_vendido
    FROM Order
    GROUP BY fecha_venta
),
venta_por_categoria_por_dia AS (
    SELECT o.fecha_venta AS fecha, c.nombre_cat, SUM(o.costo_total) AS total_vendido_por_cat
    FROM Order AS o
    JOIN Item AS i
        ON o.id_item = i.id_item
    JOIN Category AS c
        ON i.id_category = c.id_category
    GROUP BY fecha_venta, c.nombre_cat
),
ventas_min_max_por_dia AS (
    SELECT fecha_venta AS fecha, MAX(costo_total) AS venta_maxima, MIN(costo_total) AS venta_minima
    FROM Order
    GROUP BY fecha_venta
)
SELECT vc.fecha, 
       vc.nombre_cat, 
       (vc.total_vendido_por_cat / t.total_vendido * 100) AS venta_porcentaje, 
       mm.venta_maxima, 
       mm.venta_minima 
FROM ventas_min_max_por_dia AS mm 
JOIN venta_por_categoria_por_dia AS vc
    ON mm.fecha = vc.fecha
JOIN total_vendido_por_dia AS t
    ON mm.fecha = t.fecha
ORDER BY vc.fecha, vc.nombre_cat

-- 4. Se solicita poblar una nueva tabla con el precio y estado de los Ítems a fin del día.
-- Tener en cuenta que debe ser reprocesable, y se ejecutará todos los días para obtener
-- la información evolutiva.
CREATE TABLE Status(
    id_status SERIAL PRIMARY KEY,
    id_item INT REFERENCES Item (id_item),
    nombre_item VARCHAR(200) NOT NULL,
    precio DECIMAL(12, 2),
    estado VARCHAR(20),
    fecha DATE
);
--Se insertan en la tabla estado, con lógica para reprocesar. En caso de perder días, se crean variables llamadas BUFFER_SIZE y EXECUTION_DATE.
--EXECUTION_DATE permite seleccionar el día de ejecución, en caso de necesitar evaluar algún día en particular.
--Por otro lado, BUFFER_SIZE permite armar una ventana de tiempo más amplia, en caso de necesitar evaluar varios días, hasta la fecha definida en EXECUTION_DATE.
--Por default, BUFFER_SIZE se hardcodea a 0, mientras que EXECUTION_DATE puede dejarse como CURRENT_DATE() en algún script que sea llamado por el orquestador. 
INSERT INTO Status
    SELECT id_item, nombre_item, precio, estado, CURRENT_DATE() AS fecha --Se toma la fecha actual
    FROM Item
    WHERE fecha_de_baja BETWEEN (${EXECUTION_DATE}::DATE - INTERVAL '${BUFFER_SIZE} DAYS') AND ${EXECUTION_DATE}::DATE--logica para reprocesar en Matillion, para variables de Airflow usar {{ BUFFER_SIZE }} o {{ EXECUTION_DATE }}
        -- para Snowflake usar DATEADD(${EXECUTION_DATE}::DATE, - ${BUFFER_SIZE} 'DAYS') AND ${EXECUTION_DATE}::DATE 

--Cron: 59 23 * * * UTC-3