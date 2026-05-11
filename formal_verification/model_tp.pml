/* Definición de constantes basadas en tu arquitectura real */
#define N_WORKERS 3    /* Escalado para verificación de estados */
#define BUF_SIZE 5     /* Simula el buffer de tus canales en Go */
#define MAX_ITEMS 15   /* Límite de carga para la prueba */

byte buffer_count = 0;
byte processed_items = 0;
bool mutex = false;    /* Para verificar exclusión mutua en secciones críticas */

/* PROPIEDADES LTL (Linear Temporal Logic) */

/* Liveness: Siempre que haya ítems pendientes, eventualmente se procesarán todos */
ltl liveness_property { [] ( (processed_items < MAX_ITEMS) -> <> (processed_items == MAX_ITEMS) ) }

/* Safety: El buffer nunca debe exceder su capacidad máxima */
ltl safety_buffer { [] (buffer_count <= BUF_SIZE) }

/* Proceso Ingestor (Productor) */
active proctype Ingestor() {
    byte items_produced = 0;
    do
    :: items_produced < MAX_ITEMS ->
        /* Bloqueo si el buffer está lleno (Backpressure) */
        (buffer_count < BUF_SIZE);

        atomic {
            buffer_count++;
            items_produced++;
            printf("INGESTOR: Enviado item %d. Buffer actual: %d\n", items_produced, buffer_count);
        }
    :: items_produced == MAX_ITEMS ->
        printf("INGESTOR: Finalizado. No más registros.\n");
        break;
    od
}

/* Proceso Worker (Consumidor) */
active [N_WORKERS] proctype Worker() {
    do
    :: (processed_items < MAX_ITEMS) ->
        /* El worker espera si el buffer está vacío */
        (buffer_count > 0) ->

        atomic {
            /* Simulación de Exclusión Mutua para el contador atómico */
            !mutex;
            mutex = true;

            buffer_count--;
            processed_items++;
            printf("WORKER %d: Procesando. Total procesado: %d\n", _pid, processed_items);

            mutex = false;
        }
    :: (processed_items == MAX_ITEMS) ->
        break;
    od
}