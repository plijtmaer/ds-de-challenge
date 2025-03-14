CREATE TABLE Customer(
    id_customer SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    nombre VARCHAR(255) NOT NULL,
    apellido VARCHAR(255) NOT NULL,
    sexo CHAR(1),
    direccion TEXT,
    fecha_de_nacimiento DATE,
    telefono VARCHAR(14) --admite casos con +5411... y +54911...
);

CREATE TABLE Item(
    id_item SERIAL PRIMARY KEY,
    nombre_item VARCHAR(200) NOT NULL,
    precio DECIMAL(12, 2), --hasta 999999999.99
    estado VARCHAR(20),
    fecha_de_baja DATE,
    id_category INT REFERENCES Category (id_category)
);

CREATE TABLE Category(
    id_category SERIAL PRIMARY KEY,
    nombre_cat VARCHAR(50) NOT NULL,
    descripcion TEXT,
    path VARCHAR(300) NOT NULL
);

CREATE TABLE Order(
    id_order SERIAL PRIMARY KEY,
    fecha_venta DATE,
    costo_total DECIMAL(12, 2),
    id_customer INT REFERENCES Customer (id_customer)
);

--Creación de tabla intermedia entre Order y Item, para resolver relación N a M
CREATE TABLE Order_Item(
    id_order INT REFERENCES Order (id_order),
    id_item INT REFERENCES Item (id_item),
    cantidad INT NOT NULL,
    costo_total DECIMAL(12, 2),
    PRIMARY KEY (id_order, id_item)
);

--Tabla nueva para la pregunta 4
CREATE TABLE Status(
    id_status SERIAL PRIMARY KEY,
    id_item INT REFERENCES Item (id_item),
    nombre_item VARCHAR(200) NOT NULL,
    precio DECIMAL(12, 2),
    estado VARCHAR(20),
    fecha DATE
);