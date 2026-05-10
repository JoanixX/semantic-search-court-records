# Diccionario de Datos: processed_records.csv

Este documento detalla las columnas presentes en el dataset procesado de expedientes del Tribunal Constitucional. El dataset es el resultado de la limpieza, estandarización y anonimización de datos mediante procesamiento paralelo.

## Estructura del Dataset

| Columna | Descripción Detallada | Uso Principal | Contribución al Objetivo |
| :--- | :--- | :--- | :--- |
| **FEC_INGRESO** | Fecha en la que el expediente fue ingresado formalmente al Tribunal Constitucional. | Análisis de series temporales y cálculo de carga procesal anual. | Permite identificar tendencias históricas y filtrar casos por antigüedad en la búsqueda semántica. |
| **PROCEDENCIA** | Entidad o distrito judicial de origen desde donde se eleva el recurso al TC. | Identificación de la ruta judicial previa del caso. | Ayuda a segmentar la jurisprudencia según la corte de origen (Corte Superior, etc.). |
| **CDES_TIPOPROCESO** | Descripción del tipo de proceso constitucional (ej. Amparo, Habeas Corpus, Habeas Data, Cumplimiento). | Categorización fundamental del registro judicial. | Es el filtro primario para la búsqueda semántica, permitiendo agrupar casos por su naturaleza jurídica. |
| **SALA_ORIGEN** | Identifica la sala o juzgado específico en la instancia inferior que resolvió antes del TC. | Trazabilidad del historial del expediente. | Permite análisis de consistencia judicial entre instancias previas y el TC. |
| **TIPO_DEMANDANTE** | Clasificación del sujeto que interpone la demanda (Persona Natural, Jurídica, Institución Pública). | Análisis demográfico y de actores procesales. | Facilita búsquedas específicas (ej. "casos presentados por sindicatos" vs "personas naturales"). |
| **TIPO_DEMANDADO** | Clasificación de la entidad o persona contra quien se dirige la acción constitucional. | Identificación de entidades más demandadas. | Permite el análisis de conflictos contra el Estado o sectores específicos (ej. AFP, ONP, Ministerios). |
| **SALA** | Sala del Tribunal Constitucional asignada para resolver el caso (Sala 1, Sala 2 o Pleno). | Distribución interna de la carga procesal en el TC. | Crucial para estudiar discrepancias de criterios o jurisprudencia vinculante por sala. |
| **FEC_VISTA** | Fecha en la que se realizó la vista de la causa (audiencia pública o informe oral). | Hito cronológico del proceso judicial. | Permite medir tiempos muertos entre el ingreso y la primera audiencia. |
| **MATERIA** | Especialidad jurídica macro a la que pertenece la controversia (ej. Civil, Penal, Laboral). | Indexación de alto nivel para motores de búsqueda. | Clave para el "clustering" semántico de expedientes por especialidad legal. |
| **SUB_MATERIA** | Categoría intermedia que precisa el conflicto legal dentro de una materia general. | Refinamiento de la búsqueda jurídica. | Mejora la precisión de la búsqueda semántica al reducir el ruido de resultados genéricos. |
| **ESPECIFICA** | El nivel más detallado de clasificación del caso (ej. Despido arbitrario, Pensión de viudez). | Identificación del núcleo del conflicto. | Es el metadato más rico para alimentar modelos de embeddings para búsqueda por similitud. |
| **PUB_PAGWEB** | Fecha en la que la resolución fue publicada en el portal institucional del TC. | Cálculo del tiempo total de resolución del caso. | Permite generar la métrica de eficiencia "Tiempo de Respuesta" usada en el análisis de rendimiento. |
| **PUB_PERUANO** | Fecha de publicación de la sentencia en el Diario Oficial El Peruano. | Verificación de eficacia y vigencia de la norma/sentencia. | Útil para determinar la oponibilidad de la sentencia frente a terceros. |
| **TIPO_RESOLUCION** | El carácter jurídico del documento final emitido (ej. Sentencia, Auto, Resolución). | Categorización jerárquica del pronunciamiento. | Permite diferenciar entre decisiones de fondo (sentencias) y procesales (autos). |
| **FALLO** | El sentido final de la decisión (ej. Fundada, Infundada, Improcedente). | Análisis de éxito judicial y sentido de la jurisprudencia. | Clave para realizar analítica predictiva sobre el resultado de procesos similares. |
| **FEC_DEVPJ** | Fecha de devolución del expediente físico al Poder Judicial para su ejecución. | Control de flujo de salida de expedientes. | Indica la finalización del ciclo de vida del expediente dentro del TC. |
| **FEC_DEVPJ_1** | Fecha secundaria o de rectificación de devolución al Poder Judicial. | Auditoría de procesos administrativos. | Asegura la integridad de los datos de despacho judicial. |
| **DEPARTAMENTO** | Ubicación geográfica (Nivel 1) de donde proviene el conflicto. | Análisis geográfico de la conflictividad constitucional. | Permite generar para mapas de calor y detectar regiones con mayor carga procesal. |
| **PROVINCIA** | Ubicación geográfica (Nivel 2) del origen del caso. | Granularidad en el análisis de procedencia. | Ayuda a identificar focos específicos de litigiosidad a nivel provincial. |
| **DISTRITO** | Ubicación geográfica (Nivel 3) o distrito judicial específico. | Máximo nivel de detalle geográfico. | Permite filtrado hiper-localizado de jurisprudencia. |
| **RESUMEN_SENTENCIA** | Extracto textual o metadatos de auditoría del proceso de extracción. | Fuente principal para modelos de lenguaje y resúmenes automáticos. | Contiene la información más densa para el buscador semántico y trazabilidad de origen del scraper. |

---

## Relación con los Objetivos del Proyecto

### 1. Limpieza y Procesamiento Paralelo
Columnas como `FEC_INGRESO`, `PUB_PAGWEB` y las de ubicación geográfica requieren limpieza de formatos y estandarización para ser procesables. El uso de Go permite realizar estas transformaciones y la **anonimización de DNIs** (presentes en descripciones relacionadas con estas columnas) de forma masiva sobre más de un millón de registros en tiempos reducidos.

### 2. Búsqueda Semántica
La combinación de `CDES_TIPOPROCESO`, `MATERIA`, `SUB_MATERIA` y `ESPECIFICA` forma la "firma semántica" de cada registro. Estos metadatos permiten que el motor de búsqueda no solo encuentre palabras exactas, sino conceptos legales relacionados, facilitando la labor de investigación de abogados y ciudadanos.

### 3. Generación de Evidencia (EDA)
Estas columnas son la base de los gráficos generados por `scripts/analisis_eda.py`. Sin la estructura clara de este diccionario, no sería posible interpretar la evolución histórica de casos o la distribución de carga procesal por departamento.
