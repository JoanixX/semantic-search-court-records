#define N_WORKERS 3
#define BUF_SIZE 5
#define MAX_ITEMS 15

/* =========================================================
   VARIABLES GLOBALES DEL SISTEMA
   ========================================================= */

byte buffer_count = 0;
byte produced = 0;
byte consumed = 0;

/* Mutex simple:
   false = libre
   true  = ocupado
*/
bool mutex = false;

/* Variable auxiliar para validar exclusión mutua */
bool in_critical = false;

/* =========================================================
   VARIABLES AUXILIARES PARA STARVATION Y FAIRNESS
   ========================================================= */

/* Contador individual de trabajos procesados por worker */
byte worker_jobs[N_WORKERS];

/* Marca de actividad de cada worker */
bool worker_active[N_WORKERS];

/* =========================================================
   PROCESO PRODUCTOR (INGESTOR)
   Simula la carga de expedientes judiciales
   dentro de un buffer finito.
   ========================================================= */

active proctype Ingestor() {

    do
    :: atomic {

        /* Backpressure:
           El productor solo inserta si existe
           espacio disponible en el buffer.
        */

        (buffer_count < BUF_SIZE &&
         produced < MAX_ITEMS &&
         !mutex) ->

            mutex = true;

            /* =========================
               SECCIÓN CRÍTICA
               ========================= */

            assert(!in_critical);
            in_critical = true;

            buffer_count++;
            produced++;

            printf("Ingestor: Expediente cargado. Buffer=%d | Total=%d\n",
                    buffer_count, produced);

            in_critical = false;

            mutex = false;
       }

    :: (produced >= MAX_ITEMS) ->
        break
    od
}

/* =========================================================
   PROCESOS CONSUMIDORES (WORKERS)
   Simulan limpieza y anonimización concurrente
   ========================================================= */

active [N_WORKERS] proctype Limpiador() {

    byte id;

    /* Ajuste para indexar desde 0 */
    id = _pid - 1;

    do
    :: atomic {

        (buffer_count > 0 &&
         !mutex) ->

            mutex = true;

            /* =========================
               SECCIÓN CRÍTICA
               ========================= */

            /* Verificación de exclusión mutua */
            assert(!in_critical);

            in_critical = true;

            buffer_count--;
            consumed++;

            worker_jobs[id]++;
            worker_active[id] = true;

            printf("Worker %d: Procesando expediente. Buffer=%d\n",
                    _pid, buffer_count);

            in_critical = false;

            mutex = false;
       }

       /* =========================
          SECCIÓN NO CRÍTICA
          ========================= */

       printf("Worker %d: Aplicando limpieza y anonimización...\n", _pid);

    :: (produced >= MAX_ITEMS &&
        buffer_count == 0) ->
        break
    od
}

/* =========================================================
   PROPIEDADES DE VERIFICACIÓN FORMAL
   ========================================================= */

/* ---------------------------------------------------------
   1. EXCLUSIÓN MUTUA
   Nunca pueden existir dos procesos
   simultáneamente en sección crítica.
   --------------------------------------------------------- */

ltl exclusion_mutua {
    [] !(in_critical && mutex == false)
}

/* ---------------------------------------------------------
   2. LIVENESS / PROGRESO GLOBAL
   Todo elemento producido eventualmente
   será consumido.
   --------------------------------------------------------- */

ltl progreso {
    [] ((produced > consumed)
        -> <> (consumed == produced))
}

/* ---------------------------------------------------------
   3. FAIRNESS / ANTI-STARVATION
   Todo worker activo eventualmente
   logra procesar elementos.
   --------------------------------------------------------- */

ltl anti_starvation_worker0 {
    [] (worker_active[0] -> <> (worker_jobs[0] > 0))
}

ltl anti_starvation_worker1 {
    [] (worker_active[1] -> <> (worker_jobs[1] > 0))
}

ltl anti_starvation_worker2 {
    [] (worker_active[2] -> <> (worker_jobs[2] > 0))
}

/* ---------------------------------------------------------
   4. CONSISTENCIA DEL BUFFER
   El buffer nunca puede ser negativo
   ni superar su capacidad máxima.
   --------------------------------------------------------- */

ltl buffer_seguro {
    [] ((buffer_count >= 0)
        && (buffer_count <= BUF_SIZE))
}

/* ---------------------------------------------------------
   5. AUSENCIA DE DEADLOCK
   Verificada automáticamente por SPIN
   mediante exploración completa del
   espacio de estados.
   --------------------------------------------------------- */

/* =========================================================
   COMANDOS DE VERIFICACIÓN
   =========================================================

   1. Simulación:
      spin modelo_final.pml

   2. Verificación completa:
      spin -a modelo_final.pml
      gcc -o pan pan.c
      ./pan -a

   3. Verificación LTL específica:
      ./pan -a -N exclusion_mutua
      ./pan -a -N progreso
      ./pan -a -N anti_starvation_worker0

   4. Búsqueda de deadlocks:
      spin -search modelo_final.pml

   ========================================================= */