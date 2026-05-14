-- Tablas de prueba Oracle
CREATE TABLE empleados (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre     VARCHAR2(100) NOT NULL,
  apellido   VARCHAR2(100) NOT NULL,
  cargo      VARCHAR2(100),
  departamento VARCHAR2(100),
  salario    NUMBER(10,2) DEFAULT 0,
  activo     NUMBER(1) DEFAULT 1,
  fecha_ingreso DATE
);

CREATE TABLE departamentos (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre     VARCHAR2(100) NOT NULL,
  ubicacion  VARCHAR2(100),
  presupuesto NUMBER(12,2) DEFAULT 0
);

CREATE TABLE productos (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codigo     VARCHAR2(20) NOT NULL,
  nombre     VARCHAR2(150) NOT NULL,
  categoria  VARCHAR2(100),
  precio     NUMBER(10,2),
  stock      NUMBER(10) DEFAULT 0,
  proveedor  VARCHAR2(100)
);

CREATE TABLE ventas (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fecha      DATE,
  cliente    VARCHAR2(150),
  producto_id NUMBER,
  cantidad   NUMBER(10),
  total      NUMBER(12,2),
  vendedor   VARCHAR2(100),
  pagado     NUMBER(1) DEFAULT 0
);

-- Datos departamentos
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES ('TI', 'Piso 3', 50000);
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES ('Finanzas', 'Piso 2', 80000);
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES ('RRHH', 'Piso 1', 30000);
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES ('Ventas', 'Piso 4', 60000);
INSERT INTO departamentos (nombre, ubicacion, presupuesto) VALUES ('Gerencia', 'Piso 5', 120000);

-- Datos empleados
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Ana', 'Torres', 'Desarrolladora', 'TI', 2800, 1, DATE '2021-03-15');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Luis', 'Mora', 'Analista', 'Finanzas', 2200, 1, DATE '2020-07-01');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Maria', 'Paz', 'Gerente', 'RRHH', 4500, 1, DATE '2019-01-10');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Carlos', 'Rios', 'DBA', 'TI', 3200, 1, DATE '2022-06-20');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Sofia', 'Leon', 'Contadora', 'Finanzas', 2600, 0, DATE '2018-11-05');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Pedro', 'Salas', 'Vendedor', 'Ventas', 1900, 1, DATE '2023-02-01');
INSERT INTO empleados (nombre, apellido, cargo, departamento, salario, activo, fecha_ingreso) VALUES ('Elena', 'Cruz', 'Directora', 'Gerencia', 6000, 1, DATE '2017-05-20');

-- Datos productos
INSERT INTO productos (codigo, nombre, categoria, precio, stock, proveedor) VALUES ('P001', 'Laptop Dell XPS', 'Electronica', 1299.99, 15, 'Dell Inc');
INSERT INTO productos (codigo, nombre, categoria, precio, stock, proveedor) VALUES ('P002', 'Monitor Samsung 27', 'Electronica', 399.99, 30, 'Samsung');
INSERT INTO productos (codigo, nombre, categoria, precio, stock, proveedor) VALUES ('P003', 'Teclado Mecanico', 'Perifericos', 89.99, 50, 'Logitech');
INSERT INTO productos (codigo, nombre, categoria, precio, stock, proveedor) VALUES ('P004', 'Mouse Inalambrico', 'Perifericos', 45.99, 80, 'Logitech');
INSERT INTO productos (codigo, nombre, categoria, precio, stock, proveedor) VALUES ('P005', 'Disco SSD 1TB', 'Almacenamiento', 119.99, 25, 'Samsung');

-- Datos ventas
INSERT INTO ventas (fecha, cliente, producto_id, cantidad, total, vendedor, pagado) VALUES (DATE '2026-01-10', 'Empresa ABC', 1, 3, 3899.97, 'Ana Torres', 1);
INSERT INTO ventas (fecha, cliente, producto_id, cantidad, total, vendedor, pagado) VALUES (DATE '2026-01-15', 'Tech Corp', 2, 10, 3999.90, 'Pedro Salas', 1);
INSERT INTO ventas (fecha, cliente, producto_id, cantidad, total, vendedor, pagado) VALUES (DATE '2026-02-01', 'Startup XYZ', 5, 5, 599.95, 'Ana Torres', 0);
INSERT INTO ventas (fecha, cliente, producto_id, cantidad, total, vendedor, pagado) VALUES (DATE '2026-03-05', 'Empresa ABC', 3, 20, 1799.80, 'Pedro Salas', 1);

COMMIT;
