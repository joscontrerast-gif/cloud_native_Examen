# Tienda Retail Lumina - Microservicios

Plataforma de retail migrada de monolito a **Arquitectura de Microservicios** orientada a eventos para resolver problemas de saturacion en horas peak, incorporando mensajeria asincrona con **Apache Kafka** (clúster multi-broker con soporte de *Dead Letter Topics*) y **RabbitMQ**.

## Arquitectura

```
                    ┌─────────────┐
                    │   Frontend   │  React + Vite + Tailwind
                    │  :5173       │  + Lucide + Framer Motion
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ BFF Gateway │  Spring Boot :8080
                    └──────┬──────┘
           ┌───────┬───────┼───────┬───────┬───────┐
           │       │       │       │       │       │
      ┌────▼─┐ ┌──▼───┐ ┌─▼──┐   ┌─▼──┐ ┌──▼──┐  ┌─▼──┐
      │Users │ │Prods │ │Bod.│   │Cart│ │Deliv│  │Pago│
      │:8081 │ │:8082 │ │:8083│  │:8084│ │:8085│ │:8086│
      └──┬───┘ └──┬───┘ └─┬──┘   └ ─┬──┘ └──┬──┘ └─┬──┘
         │        │       │         │       │      │
      ┌──▼──┐ ┌──▼──┐ ┌──▼─┐ ┌   ─▼──┐ ┌──▼─┐ ┌─▼──┐
      │MySQL│ │MySQL│ │MySQL│ │MySQL│ │MySQL│ │MySQL│
      │:3307│ │:3308│ │:3309│ │:3310│ │:3311│ │:3312│
      └─────┘ └─────┘ └────┘  └────┘  └────┘  └────┘
                          │                     │
              ┌───────────▼─────────────────────▼───────────┐
              │       Clúster Apache Kafka (2 Brokers)      │
              │  kafka-1 :9092  │  kafka-2 :9093            │
              │  ZooKeeper :2181 │  Kafka UI :8090          │
              └──────────────────────┬──────────────────────┘
                                     │
              ┌──────────────────────▼──────────────────────┐
              │         Clúster RabbitMQ (2 Nodos)          │
              │  rabbitmq1 :5672 / :15672 │ rabbitmq2 :5673 │
              └─────────────────────────────────────────────┘
```

## Stack Tecnologico

| Capa | Tecnologia |
|------|-----------|
| Frontend | React, Vite, Tailwind CSS, Lucide React, Framer Motion |
| Backend | Java 17, Spring Boot 3.2, Spring Kafka, Spring AMQP, JPA/Hibernate |
| Base de Datos | MySQL 8.0 (una instancia por servicio) |
| Event Streaming y Mensajeria | Apache Kafka (2 Brokers + ZooKeeper), RabbitMQ (2 Nodos) |
| Monitoreo de Eventos | Kafka UI, RabbitMQ Management |
| Pagos | MercadoPago SDK |
| Infraestructura | Docker, Docker Compose (20 contenedores) |

## Microservicios

| Servicio | Puerto | BD Puerto | Descripcion |
|----------|--------|-----------|-------------|
| BFF Gateway | 8080 | - | API Gateway, consumidores de alertas/DLT y `KafkaAdminService` |
| ms-usuarios | 8081 | 3307 | Perfiles, autenticacion JWT |
| ms-productos | 8082 | 3308 | Catalogo y marcas |
| ms-bodega | 8083 | 3309 | Inventario, descuento por pago y alertas de stock critico |
| ms-carrito | 8084 | 3310 | Carrito de compras |
| ms-delivery | 8085 | 3311 | Seguimiento de entregas (consumidor de pagos exitosos) |
| ms-pagos | 8086 | 3312 | Integracion MercadoPago y publicacion de pagos exitosos |

## Infraestructura de Mensajeria y Monitoreo

| Contenedor | Puerto(s) | Descripcion |
|------------|-----------|-------------|
| lumina-zookeeper | 2181 | Coordinacion del clúster de Apache Kafka |
| lumina-kafka-1 | 9092 | Broker 1 del clúster Apache Kafka |
| lumina-kafka-2 | 9093 | Broker 2 del clúster Apache Kafka |
| lumina-kafka-ui | 8090 | Interfaz web para inspeccion de brokers, topicos, mensajes y consumer groups |
| lumina-rabbitmq1 | 5672 / 15672 | Nodo principal de RabbitMQ y panel web de administracion |
| lumina-rabbitmq2 | 5673 / 15673 | Nodo secundario en clúster de RabbitMQ |
| lumina-frontend | 5173 | Aplicacion web React en contenedor |

## Topologia de Topicos Kafka

| Topico | Particiones | Factor Replicacion | Productor | Consumidor(es) (`groupId`) |
|--------|-------------|--------------------|-----------|----------------------------|
| `lumina.pagos.exitoso` | 3 | 2 | `ms-pagos` | `ms-delivery` (`delivery-group`), `ms-bodega` (`bodega-pagos-group`) |
| `lumina.pagos.exitoso.dlt` | 3 | 2 | `DefaultErrorHandler` | `bff-gateway` (`bff-dlt-group`) |
| `lumina.stock.alertas` | 2 | 2 | `ms-bodega` | `bff-gateway` (`bff-alertas-group`) |

## Requisitos Previos

- **Docker Desktop** instalado y corriendo (con backend WSL2 activo en Windows)
- **Node.js** (v18 o superior) solo si deseas ejecutar el frontend fuera de Docker

## Inicio Rapido (desde cero)

### Paso 1: Abrir Docker Desktop

Asegurate de que Docker Desktop este abierto y corriendo. Puedes verificar con:

```bash
docker ps
```

Si no muestra error, Docker esta listo.

### Paso 2: Levantar infraestructura base (MySQL, ZooKeeper y RabbitMQ)

Para evitar condiciones de carrera y errores de replicacion (`InvalidReplicationFactorException`), inicia primero los servicios base desde la raiz del proyecto:

```bash
docker-compose up -d zookeeper rabbitmq1 rabbitmq2 mysql-usuarios mysql-productos mysql-bodega mysql-carrito mysql-delivery mysql-pagos
```

Espera **20 segundos** a que MySQL, ZooKeeper y RabbitMQ terminen de inicializar.

### Paso 3: Levantar ambos brokers de Apache Kafka

Inicia los dos brokers en paralelo para asegurar que el clúster registre `replication-factor = 2`:

```bash
docker-compose up -d kafka-1 kafka-2
```

Espera **15 segundos** a que ambos brokers se registren en ZooKeeper.

### Paso 4: Levantar BFF Gateway (creacion de topicos y colas)

```bash
docker-compose up -d --build bff-gateway
```

Espera **15 segundos** y confirma con `docker logs --tail=20 lumina-bff-gateway` que aparezca `Started BffGatewayApplication`.

### Paso 5: Levantar microservicios, Kafka UI y Frontend

```bash
docker-compose up -d --build
```

Espera ~1-2 minutos la primera vez mientras compilan las imagenes Maven/Docker. Verifica que los **20 contenedores** esten activos:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

*(Opcional)* Si prefieres levantar el frontend localmente fuera de Docker:

```bash
cd frontend
npm install
npm run dev
```

### Paso 6: Abrir la aplicacion y herramientas de monitoreo

- **Tienda:** http://localhost:5173
- **API Gateway:** http://localhost:8080
- **Kafka UI:** http://localhost:8090
- **RabbitMQ Management:** http://localhost:15672 (`guest` / `guest`)

### Credenciales de Admin

| Campo | Valor |
|-------|-------|
| Email | admin@lumina.cl |
| Password | admin123 |

### Credenciales de Usuario Prueba (Recomendado)

| Campo | Valor |
|-------|-------|
| Email | user@lumina.cl |
| Password | user1234 |

## Pruebas de Eventos Kafka y Dead Letter Topic (DLT)

1. **Alerta de stock bajo (`lumina.stock.alertas`):**
   - Ingresa con la cuenta admin en http://localhost:5173, ve a **Bodega** y descuenta el stock de un producto hasta dejarlo en **10 unidades o menos**.
   - Verifica el mensaje en **Kafka UI** (`http://localhost:8090` → **Topics** → `lumina.stock.alertas` → **Messages**) o consulta el endpoint administrativo con tu token JWT:
     - `GET http://localhost:8080/api/admin/kafka/gestion/alertas-stock`
     - `GET http://localhost:8080/api/admin/kafka/gestion/consumer-groups`

2. **Manejo de errores y Dead Letter Topic (`lumina.pagos.exitoso.dlt`):**
   - En **Kafka UI** (`http://localhost:8090`), entra en **Topics** → `lumina.pagos.exitoso` → **Produce Message** y envia un texto invalido (ej. `MENSAJE_INVALIDO_PRUEBA_DLT`).
   - Tras agotar los reintentos configurados en `DefaultErrorHandler`, el mensaje se redirige automaticamente a `lumina.pagos.exitoso.dlt` con las cabeceras de excepcion (`kafka_dlt-original-topic`, `kafka_dlt-exception-message`) y es consumido por `bff-dlt-group`.

## Comandos Utiles

```bash
# Ver logs de un servicio especifico
docker-compose logs -f ms-bodega

# Pausar todos los servicios sin perder datos de MySQL
docker-compose stop

# Reanudar todos los servicios pausados
docker-compose start

# Detener y eliminar contenedores (conservando volumenes de base de datos)
docker-compose down

# Detener solo un servicio
docker-compose stop ms-productos

# Reiniciar con rebuild de un servicio especifico
docker-compose up -d --build ms-bodega

# Ver estado de los 20 contenedores
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Panel de Administracion

Accede con la cuenta admin para:
- **Productos**: Crear, editar y eliminar productos del catalogo
- **Bodega**: Ver inventario, ajustar stock (agregar/descontar) y disparar alertas de stock critico hacia Kafka
