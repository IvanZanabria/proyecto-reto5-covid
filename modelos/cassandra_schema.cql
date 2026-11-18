CREATE KEYSPACE IF NOT EXISTS covid_operational 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 2. Usar el keyspace
USE covid_operational;

-- 3. Tabla para consultas rápidas por Municipio y Fecha de Reporte
CREATE TABLE IF NOT EXISTS casos_por_municipio (
    cod_muni text,
    fecha_reporte text,
    id_caso text,
    departamento text,
    municipio text,
    edad int,
    sexo text,
    tipo_contagio text,
    estado text,
    PRIMARY KEY ((cod_muni), fecha_reporte, id_caso)
);