r"""
Demostración de interbloqueo (deadlock) con dos procesos y dos recursos.

Cada recurso está protegido por un lock (multiprocessing.Lock).

Uso (desde una terminal: cmd, PowerShell o la terminal de VS Code):
    python interbloqueo.py             # sin argumento = modo "bloqueado" (botón Run de VS Code)
    comando: python3.15.exe interbloqueo.py bloqueado   # provoca el deadlock y lo detecta por timeout
    comando: python3.15.exe interbloqueo.py corregido   # mismo escenario, pero con orden global de locks

Escenario:
    - Proceso A necesita R1 y luego R2.
    - Proceso B necesita R2 y luego R1   (modo "bloqueado")
                 R1 y luego R2           (modo "corregido": MISMO orden que A)

La única diferencia entre ambos modos es el orden en que B pide los recursos.
"""
import sys
import time
import multiprocessing as mp
from threading import BrokenBarrierError

TIMEOUT = 3.0   # segundos máximos esperando un lock antes de sospechar deadlock
TRABAJO = 0.5   # segundos que simula "usar" un recurso
ARRANQUE = 15.0 # margen para que ambos procesos arranquen (en Windows tardan más)


def trabajador(nombre, orden, locks, inicio, mitad, salida, cola, t0, modo):
    """Proceso que necesita dos recursos, pedidos en el orden indicado."""
    # Desempaqueta el orden en que este proceso debe pedir los recursos
    primero, segundo = orden
    # Lista de recursos que este proceso ya retiene (para liberarlos al final)
    tomados = []

    def log(mensaje):
        # Envía el evento a la cola con su tiempo relativo para armar el registro
        cola.put(("evento", time.time() - t0, nombre, mensaje))

    def tomar(recurso):
        """Pide un lock con timeout. Devuelve True si lo obtuvo."""
        log(f"pide {recurso}")
        # Intenta adquirir el lock; si pasa el TIMEOUT se asume posible deadlock
        if locks[recurso].acquire(timeout=TIMEOUT):
            tomados.append(recurso)
            log(f"obtuvo {recurso}")
            return True
        log(f"TIMEOUT esperando {recurso} (retiene {', '.join(tomados)}) "
            f"-> posible deadlock")
        return False

    # Se asume bloqueado hasta que el proceso logre terminar su trabajo
    resultado = "BLOQUEADO"
    try:
        inicio.wait(ARRANQUE)                   # ambos arrancan al mismo tiempo
        if tomar(primero):
            time.sleep(TRABAJO)                 # usa el primer recurso
            if modo == "bloqueado":
                # Garantiza que AMBOS ya retienen su primer recurso antes de
                # pedir el segundo (así el deadlock no depende del azar).
                mitad.wait(TIMEOUT)
            # Pide el segundo recurso mientras retiene el primero (hold and wait)
            if tomar(segundo):
                time.sleep(TRABAJO)             # usa ambos recursos
                log("terminó su trabajo")
                resultado = "OK"
        if resultado == "BLOQUEADO" and modo == "bloqueado":
            # No soltar nada hasta que el otro también haya detectado el
            # timeout; si no, uno liberaría su lock y "destrabaría" al otro.
            salida.wait(TIMEOUT)
    except BrokenBarrierError:
        # Una barrera expiró (el otro proceso no llegó a tiempo): se ignora
        pass
    finally:
        for recurso in reversed(tomados):       # siempre liberar lo retenido
            locks[recurso].release()
            log(f"liberó {recurso}")
        # Avisa al proceso principal que terminó y con qué resultado
        cola.put(("fin", nombre, resultado))


def main(modo):
    # Un lock por recurso compartido
    locks = {"R1": mp.Lock(), "R2": mp.Lock()}
    # Barreras para sincronizar a los dos procesos: arranque, mitad y salida
    inicio, mitad, salida = mp.Barrier(2), mp.Barrier(2), mp.Barrier(2)
    # Cola por donde los procesos reportan eventos al proceso principal
    cola = mp.Queue()

    # Orden de pedido de recursos: si B invierte el orden se forma el ciclo
    ordenes = {
        "A": ["R1", "R2"],
        "B": ["R2", "R1"] if modo == "bloqueado" else ["R1", "R2"],
    }

    # Referencia de tiempo para los timestamps del registro
    t0 = time.time()
    # Crea un proceso por trabajador (A y B)
    procesos = [
        mp.Process(target=trabajador,
                   args=(n, ordenes[n], locks, inicio, mitad, salida, cola, t0, modo))
        for n in ("A", "B")
    ]
    for p in procesos:
        p.start()

    eventos, resultados = [], {}
    try:
        # Recolecta mensajes hasta que ambos procesos reporten su "fin"
        while len(resultados) < 2:
            item = cola.get(timeout=4 * TIMEOUT + 5)
            if item[0] == "fin":
                resultados[item[1]] = item[2]
            else:
                eventos.append(item[1:])
    except Exception:
        print("ERROR: los procesos no respondieron; se terminan a la fuerza.")
    # Espera a los procesos y mata a los que sigan vivos
    for p in procesos:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    # --- Registro de la secuencia de adquisiciones -------------------------
    # Ordena los eventos por tiempo y los formatea como líneas de log
    lineas = [f"[{t:5.2f}s] {proc}: {msg}" for t, proc, msg in sorted(eventos)]
    print(f"=== Modo: {modo} | orden A: {ordenes['A']} | orden B: {ordenes['B']} ===")
    print("\n".join(lineas))

    # Procesos que agotaron el timeout sin terminar su trabajo
    bloqueados = [n for n, r in resultados.items() if r == "BLOQUEADO"]
    # Si ambos quedaron bloqueados hay deadlock; si ninguno, todo salió bien
    if len(bloqueados) == 2:
        veredicto = "DEADLOCK detectado (ambos procesos agotaron el timeout)"
        ciclo = "Ciclo de espera: A -> R2 -> B -> R1 -> A"
    elif not bloqueados and len(resultados) == 2:
        veredicto = "SIN deadlock: ambos procesos terminaron correctamente"
        ciclo = "No hay ciclo de espera."
    else:
        veredicto, ciclo = f"Resultado inesperado: {resultados}", ""
    print(f"\n>>> {veredicto}\n>>> {ciclo}")

    # Guarda el registro completo en un archivo según el modo ejecutado
    with open(f"registro_{modo}.log", "w", encoding="utf-8") as f:
        f.write("\n".join(lineas) + f"\n\n{veredicto}\n{ciclo}\n")

    # Código de salida 0 si el resultado coincide con lo esperado para el modo
    esperado = len(bloqueados) == 2 if modo == "bloqueado" else not bloqueados
    return 0 if esperado and len(resultados) == 2 else 1


if __name__ == "__main__":
    # Toma el modo desde la línea de comandos (por defecto "bloqueado")
    modo = sys.argv[1] if len(sys.argv) > 1 else "bloqueado"
    # Argumentos inválidos: muestra el uso y sale con código 2
    if len(sys.argv) > 2 or modo not in ("bloqueado", "corregido"):
        print(__doc__)
        sys.exit(2)
    sys.exit(main(modo))
