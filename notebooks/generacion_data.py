import csv
import random

# Datos semilla para generar expedientes
procedencias = ["LIMA", "AREQUIPA", "CUSCO", "PIURA", "LA LIBERTAD", "PUNO"]
procesos = ["AMPARO", "HABEAS CORPUS", "HABEAS DATA", "CUMPLIMIENTO", "INCONSTITUCIONALIDAD"]
demandantes = ["NATURAL", "JURIDICA"]
fallos = ["FUNDADO", "INFUNDADO", "IMPROCEDENTE"]

plantillas_texto = [
    "El demandante, identificado con DNI {dni}, interpone recurso de agravio constitucional contra la resolución.",
    "Se verifica vulneración de derechos de la ciudadana con DNI {dni} por parte del ente demandado.",
    "El representante legal (DNI {dni}) solicita la inaplicación del artículo 2 de la norma.",
    "La entidad no entregó la información pública solicitada por el usuario con DNI {dni} en el plazo legal.",
    "Fallo a favor del demandante con documento {dni} por despido arbitrario y reposición laboral."
]

num_registros = 50000
nombre_archivo = "expedientes_tc_masivo.csv"

with open(nombre_archivo, mode="w", newline="", encoding="utf-8") as archivo:
    writer = csv.writer(archivo)
    
    # Escribir cabeceras
    cabeceras = ["FEC_INGRESO", "PROCEDENCIA", "CDES_TIPOPROCESO", "SALA_ORIGEN", 
                 "TIPO_DEMANDANTE", "TIPO_DEMANDADO", "SALA", "FEC_VISTA", "MATERIA", 
                 "SUB_MATERIA", "ESPECIFICA", "PUB_PAGWEB", "PUB_PERUANO", "TIPO_RESOLUCION", 
                 "FALLO", "FEC_DEVPJ", "FEC_DEVPJ_1", "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "RESUMEN_SENTENCIA"]
    writer.writerow(cabeceras)
    
    # Generar filas sintéticas
    for i in range(num_registros):
        # Generar DNI aleatorio de 8 dígitos
        dni_falso = str(random.randint(10000000, 99999999))
        texto_generado = random.choice(plantillas_texto).format(dni=dni_falso)
        
        fila = [
            f"20{random.randint(10,25)}-01-01", # FEC_INGRESO
            random.choice(procedencias),       # PROCEDENCIA
            random.choice(procesos),           # CDES_TIPOPROCESO
            "SALA 1",                          # SALA_ORIGEN
            random.choice(demandantes),        # TIPO_DEMANDANTE
            "JURIDICA",                        # TIPO_DEMANDADO
            "PLENO",                           # SALA
            "2023-01-01",                      # FEC_VISTA
            "CONSTITUCIONAL",                  # MATERIA
            "DERECHOS",                        # SUB_MATERIA
            "N/A", "URL1", "URL2", "SENTENCIA", 
            random.choice(fallos),             # FALLO
            "N/A", "N/A", "LIMA", "LIMA", "LIMA",
            texto_generado                     # RESUMEN_SENTENCIA (Índice 20)
        ]
        writer.writerow(fila)

print(f"¡Dataset sintético '{nombre_archivo}' con {num_registros} registros generado exitosamente!")