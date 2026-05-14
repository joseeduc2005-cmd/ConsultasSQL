db.empleados.insertMany([
  { nombre: "Ana Torres", cargo: "Desarrolladora", departamento: "TI", salario: 2800, activo: true, fecha_ingreso: new Date("2021-03-15") },
  { nombre: "Luis Mora", cargo: "Analista", departamento: "Finanzas", salario: 2200, activo: true, fecha_ingreso: new Date("2020-07-01") },
  { nombre: "Maria Paz", cargo: "Gerente", departamento: "RRHH", salario: 4500, activo: true, fecha_ingreso: new Date("2019-01-10") },
  { nombre: "Carlos Rios", cargo: "DBA", departamento: "TI", salario: 3200, activo: true, fecha_ingreso: new Date("2022-06-20") },
  { nombre: "Sofia Leon", cargo: "Contadora", departamento: "Finanzas", salario: 2600, activo: false, fecha_ingreso: new Date("2018-11-05") }
]);

db.productos.insertMany([
  { codigo: "P001", nombre: "Laptop Dell XPS", categoria: "Electronica", precio: 1299.99, stock: 15, proveedor: "Dell Inc" },
  { codigo: "P002", nombre: "Monitor Samsung 27", categoria: "Electronica", precio: 399.99, stock: 30, proveedor: "Samsung" },
  { codigo: "P003", nombre: "Teclado Mecanico", categoria: "Perifericos", precio: 89.99, stock: 50, proveedor: "Logitech" },
  { codigo: "P004", nombre: "Mouse Inalambrico", categoria: "Perifericos", precio: 45.99, stock: 80, proveedor: "Logitech" },
  { codigo: "P005", nombre: "Disco SSD 1TB", categoria: "Almacenamiento", precio: 119.99, stock: 25, proveedor: "Samsung" },
  { codigo: "P006", nombre: "RAM 16GB DDR5", categoria: "Componentes", precio: 79.99, stock: 40, proveedor: "Kingston" }
]);

db.ventas.insertMany([
  { fecha: new Date("2026-01-10"), cliente: "Empresa ABC", producto_codigo: "P001", cantidad: 3, total: 3899.97, vendedor: "Ana Torres", pagado: true },
  { fecha: new Date("2026-01-15"), cliente: "Tech Corp", producto_codigo: "P002", cantidad: 10, total: 3999.90, vendedor: "Luis Mora", pagado: true },
  { fecha: new Date("2026-02-01"), cliente: "Startup XYZ", producto_codigo: "P005", cantidad: 5, total: 599.95, vendedor: "Ana Torres", pagado: false },
  { fecha: new Date("2026-02-20"), cliente: "MiniMarket", producto_codigo: "P003", cantidad: 20, total: 1799.80, vendedor: "Carlos Rios", pagado: true },
  { fecha: new Date("2026-03-05"), cliente: "Empresa ABC", producto_codigo: "P006", cantidad: 8, total: 639.92, vendedor: "Luis Mora", pagado: false }
]);

db.auditoria.insertMany([
  { accion: "LOGIN", usuario: "admin", ip: "192.168.1.10", fecha: new Date() },
  { accion: "EXPORT", usuario: "ana.torres", ip: "192.168.1.15", fecha: new Date() }
]);

print("MongoDB seed OK: empleados, productos, ventas, auditoria");
