# FleetLogix — Proyecto Integrador M2

**Data Science · Soy Henry · Proyecto individual**  
**Estudiante:** Lucas Magrino  
**Tecnologías:** Python, Pandas, Faker, PostgreSQL, SQL, Snowflake y AWS (arquitectura conceptual)

> **Proyecto en una mirada.** FleetLogix analiza una operación logística sintética de extremo a extremo: generación de datos, consultas SQL y optimización, Data Warehouse en Snowflake y diseño de una arquitectura AWS para procesar eventos de entregas. El repositorio diferencia explícitamente lo **ejecutado y verificado**, lo **simulado localmente** y lo **propuesto**.

| Datos operacionales | Viajes | Entregas en Snowflake | Pruebas Lambda |
|:--:|:--:|:--:|:--:|
| **505.650** registros | **100.000** | **400.000** hechos | **7 / 7** correctas |

> **Cómo visualizar el proyecto:** en GitHub, abrí este `README.md`; los diagramas son imágenes **PNG permanentes** y no requieren ejecutar Python. Para acceder al desarrollo, abrí los cuatro notebooks enlazados a continuación.

---

## 1. Los cuatro avances

| Etapa | Propósito | Archivo |
|:--|:--|:--|
| **Avance 1 · Generación de datos** | Modelado de tablas, datos sintéticos y controles de calidad | [Ver notebook 1](1.%20Notebooks/Avance_1_DataGeneration.ipynb) |
| **Avance 2 · SQL y optimización** | Consultas de negocio, índices y mediciones con `EXPLAIN ANALYZE` | [Ver notebook 2](1.%20Notebooks/Avance_2_SQLAnalysis.ipynb) |
| **Avance 3 · Data Warehouse** | Esquema en estrella, ETL Python → Snowflake, calidad, SCD2 y gobierno | [Ver notebook 3](1.%20Notebooks/Avance_3_DataWarehouse.ipynb) |
| **Avance 4 · Arquitectura AWS** | API Gateway, Lambda, S3, RDS, DynamoDB y pruebas locales | [Ver notebook 4](1.%20Notebooks/Avance_4_CloudArchitecture.ipynb) |

Los scripts SQL y Python están en [`2. Scripts/`](2.%20Scripts/). Las imágenes de esta página se encuentran en [`3. Documentación/`](3.%20Documentaci%C3%B3n/).

## 2. Avance 1 — Datos e integridad relacional

**Objetivo:** crear un modelo de datos sintético que represente rutas, viajes, entregas y mantenimiento de una flota logística.

![Modelo relacional FleetLogix](3.%20Documentaci%C3%B3n/01_modelo_relacional.png)

*Figura 1. Diagrama lógico de las relaciones principales.* Se utiliza `trips` para conectar conductor, vehículo y ruta; cada `delivery` se asocia a un viaje. El registro de mantenimiento se vincula al vehículo. Para consultar los campos detallados en el diagrama original de pgAdmin: [ver captura original](3.%20Documentaci%C3%B3n/08_esquema_pgadmin_original.png).

### Volumen comprobado del origen

![Volumen de datos en PostgreSQL](3.%20Documentaci%C3%B3n/02_volumen_datos.png)

*Figura 2. Registros del origen; la longitud de las barras usa escala logarítmica. Las cantidades impresas son las observadas.*

| Tabla | Filas | Propósito |
|:--|--:|:--|
| `vehicles` | 200 | Vehículos, capacidad y estado |
| `drivers` | 400 | Conductores |
| `routes` | 50 | Rutas, ciudades y distancias |
| `trips` | 100.000 | Viajes |
| `deliveries` | 400.000 | Entregas |
| `maintenance` | 5.000 | Mantenimientos |
| **Total** | **505.650** | **Datos sintéticos del origen** |

**Validaciones documentadas:** existencia y unicidad de identificadores, claves foráneas, controles temporales y reglas de peso/capacidad. El notebook registra la metodología de verificación y los límites observados. **No se publican los datos personales sintéticos como si fueran registros de clientes reales.**

## 3. Avance 2 — Análisis y optimización SQL

El avance comprende **ocho consultas oficiales seleccionadas** (1, 2, 3, 4, 5, 6, 9 y 11) y el estudio de **cinco índices** en `trips`, `deliveries`, `maintenance`, `drivers` y `routes`.

### Mediciones de rendimiento

![Tiempos SQL registrados para las consultas 1 a 6](3.%20Documentaci%C3%B3n/04_rendimiento_sql.png)

*Figura 3. Comparación de ejecuciones individuales documentadas en el Avance 2. No es una prueba controlada de causalidad: los cambios pueden estar afectados por caché, estadísticas y planificación de PostgreSQL.*

| Consulta | Antes (ms) | Después (ms) | Variación observada |
|:--:|--:|--:|--:|
| 1 | 0,377 | 0,149 | −60,5 % |
| 2 | 0,212 | 0,197 | −7,1 % |
| 3 | 36,988 | 40,813 | +10,3 % |
| 4 | 143,159 | 88,208 | −38,4 % |
| 5 | 88,767 | 45,308 | −49,0 % |
| 6 | 202,356 | 199,673 | −1,3 % |

**Consulta 9:** se documentó una versión original de **1.511,757 ms** y una reescritura con agregaciones separadas de **43,576 ms**. **No es una mejora atribuible a índices**, porque cambió la lógica de la consulta. Para las consultas 9 y 11 no se cuenta con una comparación antes/después de crear los índices bajo condiciones equivalentes.

**Lectura de negocio:** los índices y el diseño de consultas ofrecen oportunidades de rendimiento, pero deben validarse mediante planes reales, repetición de ensayos y análisis de selectividad. Los textos SQL se conservan en `2. Scripts/` y los detalles en el notebook.

## 4. Avance 3 — Data Warehouse en Snowflake

El origen transaccional PostgreSQL se transforma en un **modelo OLAP en estrella** alojado en `FLEETLOGIX_DW.ANALYTICS`. El grano de `FACT_DELIVERIES` es **una fila por entrega**.

![Modelo estrella Snowflake](3.%20Documentaci%C3%B3n/03_modelo_estrella_snowflake.png)

*Figura 4. Tabla central y seis dimensiones. `DIM_TIME` tiene 24 filas, una por hora; `DIM_CUSTOMER` identifica combinaciones de nombre y dirección, no necesariamente personas únicas.*

### Pipeline ETL y automatización diseñada

![Flujo ETL PostgreSQL Snowflake](3.%20Documentaci%C3%B3n/05_flujo_etl.png)

*Figura 5. Flujo implementado en los scripts de Python. La programación de las 02:00 requiere que el proceso permanezca activo; la ejecución nocturna completa no fue demostrada en esta entrega.*

### Resultados verificados en Snowflake

| Objeto / verificación | Valor registrado |
|:--|--:|
| `FACT_DELIVERIES` | **400.000** |
| `DELIVERY_ID` distintos | **400.000** |
| `DIM_DATE` | 4.018 |
| `DIM_TIME` | 24 |
| `DIM_VEHICLE` | 200 |
| `DIM_DRIVER` | 400 |
| `DIM_ROUTE` | 50 |
| `DIM_CUSTOMER` | 399.998 |
| Duraciones negativas en hechos | **0** |
| Claves esenciales nulas verificadas | **0** |
| Suma de `AGG_ENTREGAS_HORA` | **400.000** |

**Mecanismos desarrollados:** carga ETL, controles de calidad, agregado `SP_REFRESH_AGGREGATES()`, vistas seguras para Operaciones y Ventas, roles y usuario de servicio para autenticación no interactiva. Se verificó una retención Time Travel de **30 días** sobre la tabla de hechos.

**Precisiones importantes:** la detección SCD2 de conductores se probó sin cambios (400 registros actuales, 0 nuevas versiones históricas en esa ejecución); la creación de versiones ante un cambio real sigue pendiente de una prueba integrada. `06_etl_automation.py --check` comprobó conexiones y lectura, **no** una carga nocturna completa. No hay tarifas de venta en el origen: **los ingresos estimados no se inventan** y quedan nulos donde corresponde.

## 5. Avance 4 — Arquitectura AWS conceptual

El último avance propone evolucionar el sistema hacia procesamiento de eventos de entregas. **No se realizó despliegue en AWS:** el diagrama ilustra responsabilidades futuras de cada servicio.

![Arquitectura AWS propuesta](3.%20Documentaci%C3%B3n/06_arquitectura_aws.png)

*Figura 6. API Gateway recibe eventos desde la app; Lambda aplica lógica de negocio; DynamoDB contendría el estado actual, S3 guardaría historial y RDS alojaría PostgreSQL administrado.*

### Funciones Lambda probadas localmente

| Función Python | Entrada principal | Comportamiento |
|:--|:--|:--|
| Verificar entrega | `delivery_id`, `status` | Informa si la entrega está `delivered` / `completed` |
| Calcular ETA | Distancia restante y velocidad media | Devuelve ETA en minutos y valida entradas |
| Detectar desvío | Distancia de desvío y umbral | Devuelve una alerta lógica cuando excede el límite |

![Pruebas locales de las funciones Lambda](3.%20Documentaci%C3%B3n/07_pruebas_lambda.png)

*Figura 7. Resultado de las siete pruebas ejecutadas en el notebook con **eventos simulados**. No equivalen a integración real con DynamoDB, SNS, API Gateway ni otros servicios AWS.*

### Propuesta de migración y recuperación

- **Amazon RDS for PostgreSQL:** inventariar dependencias, respaldar con `pg_dump`, restaurar con `pg_restore`, comparar conteos e integridad y planificar el cambio de conexiones.
- **Amazon DynamoDB:** guardar el estado más reciente por `delivery_id`, con fecha de actualización, ETA y bandera de desvío.
- **Amazon S3:** conservar eventos históricos en particiones por año, mes y día.
- **Backups:** activar copias automáticas de RDS, recuperación a un punto en el tiempo, PITR en DynamoDB, versionado/cifrado en S3 y pruebas periódicas de restauración.

Estas son **decisiones de arquitectura**, no recursos creados o probados en una cuenta AWS.

## 6. Estructura de entrega

```text
ProyectoM2_LucasMagrino/
├── 1. Notebooks/
│   ├── Avance_1_DataGeneration.ipynb
│   ├── Avance_2_SQLAnalysis.ipynb
│   ├── Avance_3_DataWarehouse.ipynb
│   └── Avance_4_CloudArchitecture.ipynb
├── 2. Scripts/
│   ├── 02_queries_analysis.sql
│   ├── 03_optimization_indexes.sql
│   ├── 04_dimensional_model.sql
│   ├── 05_etl_pipeline.py
│   ├── 06_etl_automation.py
│   └── 07_driver_scd2.py
├── 3. Documentación/
│   ├── 01_modelo_relacional.png
│   ├── 02_volumen_datos.png
│   ├── 03_modelo_estrella_snowflake.png
│   ├── 04_rendimiento_sql.png
│   ├── 05_flujo_etl.png
│   ├── 06_arquitectura_aws.png
│   ├── 07_pruebas_lambda.png
│   └── 08_esquema_pgadmin_original.png
├── .gitignore
└── README.md
```

**Presentación:** este `README.md` está en la raíz del proyecto. Todas las imágenes usan rutas relativas al directorio `3. Documentación`; por eso es imprescindible conservar ese nombre de carpeta y los nombres de archivo. Los gráficos se muestran directamente en GitHub y en la vista previa Markdown de VS Code, **sin kernel Python**.

**Entrega oficial:** según la captura de instrucciones, se debe compartir el enlace de **Google Drive** a una carpeta denominada `ProyectoM2_NombreEstudiante`, con los accesos habilitados. GitHub puede servir como repositorio consultable, pero no sustituye el enlace requerido por esa instrucción.

## 7. Reproducibilidad y precauciones

**Visualización:** para consultar los avances, no es necesario ejecutar código; bastan GitHub o un visor Jupyter que admita notebooks. Para **reproducir** las ejecuciones sí se requieren dependencias y bases de datos accesibles.

```bash
python -m pip install pandas matplotlib numpy faker psycopg2-binary "snowflake-connector-python[pandas]" schedule
```

La base `fleetlogix` en PostgreSQL y los objetos de Snowflake tienen que existir y estar autorizados. El DDL `04_dimensional_model.sql` **documenta objetos del entorno** y no debe ejecutarse indiscriminadamente como un instalador. Para validar sintaxis local de scripts sin escribir en bases:

```powershell
py -m py_compile "05_etl_pipeline.py" "06_etl_automation.py" "07_driver_scd2.py"
```

> **Seguridad:** nunca incluir credenciales, claves privadas RSA, contraseñas de PostgreSQL/Snowflake, tokens o archivos locales de autenticación. Verificar los notebooks, scripts y `.gitignore` antes de publicar.

### Alcance y limitaciones

- **Observado:** conteos del origen, estructura dimensional, carga inicial, verificaciones básicas, mediciones SQL registradas y siete pruebas locales de funciones.
- **Implementado con validación parcial:** estrategia SCD2, ejecución programable de ETL, permisos granulares y controles históricos.
- **Conceptual:** infraestructura AWS, conexiones a sus servicios, migración RDS y políticas de respaldo.
- **Límites de datos:** no se incluyen precios verificables; la estimación de kilómetros y combustible por entrega es una asignación proporcional del viaje.

---

### Conclusión

FleetLogix integra cuatro etapas de una solución de ingeniería y análisis de datos: **construcción de un origen confiable, consultas SQL, analítica dimensional y arquitectura cloud**. El resultado es una entrega trazable con figuras permanentes y un registro honesto de lo efectivamente verificado, las simulaciones y el trabajo propuesto para una futura implementación.
