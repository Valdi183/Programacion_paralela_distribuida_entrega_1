"""
Este script, trata de simular una bolsa de valores donde varios compradores y vendedores interactuan en paralelo de la siguiente forma:
Se generan de forma aleatoria y se procesan órdenes de compra y de venta de unidades (activos). Esto es un proceso en paralelo ya que mientras se lanza 
una orden de compra/venta y se espera a un comprador o vendedor para ejecutar la compra/venta, se estan generando constantemente nuevas ordenes.
Para ello, utilizo hilos que me permitan ejecutar las distintas acciones de forma concurrente, y, para evitar que los procesos colapsen, utilizo
como semaforo el order_book_log, de esta forma controlo que solo una acción, pueda acceder a cada recurso (lista, cola de prioridad...)
En el pdf guardado en el Zip, detallo más información acerca de cómo he planteado e ejercicio (tipo problema), y como he ido implementando la solución.
"""
import threading
import heapq
import time
import random
from queue import PriorityQueue

# Defino un Lock para sincronizar el acceso al libro de órdenes y poder manejar bloqueos
order_book_lock = threading.Lock()

# Cola de prioridad para almacenar órdenes de compra y venta
buy_orders = PriorityQueue()
sell_orders = PriorityQueue()

class Order:
    def __init__(self, order_id, price, quantity, order_type):
        self.order_id = order_id
        self.price = price
        self.quantity = quantity
        self.order_type = order_type  # tipo buy o tipo sell
    
    # Utilizo el método lt (less than) para ordenar las órdenes en PriorityQueue
    def __lt__(self, other):
        if self.order_type == "buy":
            return self.price > other.price  # Pone el máximo precio primero
        else:
            return self.price < other.price  # Pone el mínimo precio primero

# Función para procesar órdenes de compra y venta
def process_orders():
    while True:
        with order_book_lock: # Utilizo order_book_log para bloquear el acceso y que solo un hilo pueda modificar las órdenes a la vez
            if not buy_orders.empty() and not sell_orders.empty():
                buy_order = buy_orders.queue[0]  # Escoge el mejor comprador
                sell_order = sell_orders.queue[0]  # Escoge el mejor vendedor
                
                if buy_order.price >= sell_order.price:  # Si se cumple, se puede hacer la transacción y se eliminan de la cola
                    heapq.heappop(buy_orders.queue)
                    heapq.heappop(sell_orders.queue)
                    
                    trade_quantity = min(buy_order.quantity, sell_order.quantity)
                    print(f"Trade executed: {trade_quantity} units at {sell_order.price}")
                    
                    # Ajusto las cantidades restantes 
                    if buy_order.quantity > trade_quantity:
                        buy_order.quantity -= trade_quantity
                        heapq.heappush(buy_orders.queue, buy_order)
                    if sell_order.quantity > trade_quantity:
                        sell_order.quantity -= trade_quantity
                        heapq.heappush(sell_orders.queue, sell_order)
        
        time.sleep(3)  # Simulo la latencia pausando 3 segundos entre cada orden generada

# Función para generar y añadir órdenes
def place_order(order_type):
    order_id = random.randint(1000, 9999) # Se le asocia un id random a cada orden de compra/venta
    price = round(random.uniform(50, 150), 2) # Se redondea un precio random con un valor de entre 50 y 150
    quantity = random.randint(1, 10) # Se genera una cantidad random de unidades que  (entre 1 y 10 uds)
    order = Order(order_id, price, quantity, order_type) # Se asocian las variables definidas a la orden
    
    with order_book_lock: # Se añaden las ordenes según corresponda
        if order_type == "buy":
            buy_orders.put(order)
        else:
            sell_orders.put(order)
    
    print(f"New {order_type} order: {quantity} units at {price}")

# Utilizo este hilo para generar ordenes de compra (se va generando cada una entre 1-3 segundos)
def buyer_thread():
    while True:
        place_order("buy")
        time.sleep(random.uniform(1, 3))
# Utilizo este hilo para generar ordenes de venta (se va generando cada una entre 1-3 segundos)
def seller_thread():
    while True:
        place_order("sell")
        time.sleep(random.uniform(1, 3))

# Me permite ejecutar el programa solo si se hace desde el script original "main". inicializando cada hilo
if __name__ == "__main__":
    processor = threading.Thread(target=process_orders, daemon=True)
    buyer = threading.Thread(target=buyer_thread, daemon=True)
    seller = threading.Thread(target=seller_thread, daemon=True)
    
    processor.start()
    buyer.start()
    seller.start()
    
    # El programa se ejecuta constantemente en bucle, pero se puede detener escribiendo en la consola Ctrl + C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulation stopped.")

"""
El módulo time, se puede modificar libremente, para hacer que el programa genere las distintas órdenes de forma más rápida
o más lenta, según se prefiera.
"""