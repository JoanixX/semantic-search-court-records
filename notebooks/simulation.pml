#define N_WORKERS 4
#define TOTAL_RECORDS 20

byte contador_global = 0;
byte expedientes_procesados = 0;

active [N_WORKERS] proctype WorkerPool() {
    byte lectura_temporal;
    do
    :: true ->
        // Simulamos la instrucción a nivel de hardware: atomic.AddInt64
        atomic {
            if
            :: expedientes_procesados < TOTAL_RECORDS ->
                expedientes_procesados++;
                
                // Zona de peligro: Lectura y escritura de variable compartida
                lectura_temporal = contador_global;
                contador_global = lectura_temporal + 1;
            :: else -> 
                break;
            fi
        }
    od
}

// Proceso monitor que verifica el estado final
active proctype Monitor() {
    // Esperar a que los workers terminen (_nr_pr cuenta procesos activos)
    (_nr_pr == 2); 
    
    // Si la aserción falla, existe una condición de carrera
    assert(contador_global == TOTAL_RECORDS);
    printf("Verificacion exitosa. Contador exacto: %d\n", contador_global);
}