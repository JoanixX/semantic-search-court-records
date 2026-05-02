#define N_WORKERS 3
#define BUF_SIZE 5
#define MAX_ITEMS 15

byte buffer_count = 0;
byte produced = 0;

bool mutex = false; // false = libre, true = ocupado

active proctype Ingestor() {
    do
    :: atomic { (buffer_count < BUF_SIZE && produced < MAX_ITEMS && !mutex) ->
            mutex = true;

            // SECCIÓN CRÍTICA
            buffer_count++;
            produced++;
            printf("Ingestor: Expediente cargado. Total en buffer: %d\n", buffer_count);

            mutex = false;
       }

    :: (produced >= MAX_ITEMS) ->
        break
    od
}

active [N_WORKERS] proctype Limpiador() {
    do
    :: atomic { (buffer_count > 0 && !mutex) ->
            mutex = true;

            // SECCIÓN CRÍTICA
            buffer_count--;
            printf("Worker %d: Procesando limpieza/anonimización. Quedan: %d\n", _pid, buffer_count);

            mutex = false;
       }

        // SECCIÓN NO CRÍTICA
        printf("Worker %d: Aplicando Regex...\n", _pid);

    :: (produced >= MAX_ITEMS && buffer_count == 0) ->
        break
    od
}
