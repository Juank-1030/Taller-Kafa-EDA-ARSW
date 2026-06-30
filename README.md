# Taller Kafka - Arquitecturas Orientadas por Eventos (EDA) — ARSW

**Escuela Colombiana de Ingeniería Julio Garavito**  
Programa de Ingeniería de Sistemas — Arquitecturas de Software

Autores; Juan Carlos Bohorquez Monroy, Carlos Andres Uribe Vargas
---

## Tabla de Contenido

- [Introducción](#introducción)
- [Capítulo 1. Evolución hacia arquitecturas orientadas por eventos](#capítulo-1-evolución-hacia-arquitecturas-orientadas-por-eventos)
- [Capítulo 2. Apache Kafka: fundamentos y arquitectura interna](#capítulo-2-apache-kafka-fundamentos-y-arquitectura-interna)
- [Capítulo 3. Preparación del entorno de laboratorio](#capítulo-3-preparación-del-entorno-de-laboratorio)
- [Capítulo 4. Productores y consumidores con Spring Boot](#capítulo-4-productores-y-consumidores-con-spring-boot)
- [Capítulo 5. Caso de estudio: sistema de pedidos basado en eventos](#capítulo-5-caso-de-estudio-sistema-de-pedidos-basado-en-eventos)
- [Capítulo 6. Laboratorio guiado extendido](#capítulo-6-laboratorio-guiado-extendido)
- [Capítulo 7. Manejo de errores, reintentos y Dead Letter Topics](#capítulo-7-manejo-de-errores-reintentos-y-dead-letter-topics)
- [Capítulo 8. Buenas prácticas de diseño con Kafka](#capítulo-8-buenas-prácticas-de-diseño-con-kafka)
- [Capítulo 9. Actividades de consolidación](#capítulo-9-actividades-de-consolidación)
- [Capítulo 10. Reto final, entregables y rúbrica](#capítulo-10-reto-final-entregables-y-rúbrica)

---

## Introducción

Las arquitecturas modernas requieren mecanismos de comunicación que permitan desacoplar servicios, procesar grandes volúmenes de eventos y responder de forma resiliente ante fallos parciales. Apache Kafka es una plataforma de *event streaming* que permite publicar, almacenar y consumir eventos de manera distribuida. Esta guía introduce los fundamentos arquitectónicos, técnicos y prácticos necesarios para comprender cuándo y cómo utilizar Kafka en sistemas basados en eventos.

**Objetivo general:** Comprender los principios de las arquitecturas orientadas por eventos y aplicar Apache Kafka en un laboratorio práctico con Docker, Kafka UI y Spring Boot.

**Herramientas:** Docker, Docker Compose, Kafka UI, Java 21, Maven y Spring Boot.

---

## Capítulo 1. Evolución hacia arquitecturas orientadas por eventos

### Contexto

El desarrollo de software ha evolucionado desde arquitecturas monolíticas y cliente-servidor hacia estilos más desacoplados como SOA y microservicios. La comunicación síncrona (REST) introduce acoplamiento temporal y riesgos de fallo en cadena. En contraste, las arquitecturas orientadas por eventos (EDA) utilizan un broker como intermediario, donde los productores publican eventos y los consumidores reaccionan de forma asíncrona. Un **evento** representa un hecho relevante del dominio (ej. `order-created`), y los elementos clave son: **productor**, **consumidor**, **broker** y **topic**. A diferencia de las colas tradicionales, en *event streaming* los eventos persisten durante un periodo definido, permitiendo múltiples consumidores, reprocesamiento y auditoría.

### Actividad 1. Análisis de comunicación

Clasifique qué procesos deberían ser síncronos, asíncronos o híbridos para una tienda en línea: *consultar productos, crear pedido, validar pago, enviar notificación, actualizar analítica y registrar auditoría*. Justifique brevemente su decisión.

<details>
<summary><b>Desarrollo de la Actividad 1</b></summary>

| Proceso | Tipo | Justificación |
|---------|------|---------------|
| Consultar productos | Síncrono (REST) | |
| Crear pedido | Híbrido | |
| Validar pago | Asíncrono (Kafka) | |
| Enviar notificación | Asíncrono (Kafka) | |
| Actualizar analítica | Asíncrono (Kafka) | |
| Registrar auditoría | Asíncrono (Kafka) | |

**Justificación general:**

</details>

---

## Capítulo 2. Apache Kafka: fundamentos y arquitectura interna

### Contexto

Apache Kafka es una plataforma distribuida de *event streaming* que funciona como un **log distribuido de eventos**. Sus componentes principales incluyen: **broker** (servidor Kafka), **cluster** (conjunto de brokers), **topic** (categoría lógica), **partición** (división física para paralelismo), **offset** (posición dentro de una partición), **producer**, **consumer**, **consumer group** (grupo que reparte particiones), **replica** (copia de seguridad), **leader/follower**, **ISR** (réplicas sincronizadas) y **retención** (tiempo/tamaño de conservación). Kafka garantiza orden solo dentro de una misma partición. La **clave de particionamiento** permite enrutar eventos relacionados (ej. `orderId`) a la misma partición. Los **Consumer Groups** permiten escalar el consumo: dentro de un grupo, cada partición es procesada por un solo consumidor, y grupos distintos reciben todos los eventos de forma independiente. En producción se recomienda replicación > 1 para tolerancia a fallos.

### Actividad 2. Decisiones de configuración

Analice una configuración con un topic `orders`, **una partición**, **factor de replicación 1**, **mensajes sin clave** y **retención de 24 horas**. Identifique riesgos y proponga mejoras para un ambiente productivo.

<details>
<summary><b>Desarrollo de la Actividad 2</b></summary>

**Riesgos identificados:**

1.
2.
3.

**Mejoras propuestas:**

1.
2.
3.

</details>

---

## Capítulo 3. Preparación del entorno de laboratorio

### Contexto

El laboratorio utiliza **Docker** para ejecutar Kafka en modo **KRaft** (sin ZooKeeper) y **Kafka UI** como herramienta visual. Se proporciona un archivo `docker-compose.yml` con un broker Kafka y la interfaz web en el puerto `8080`. El broker queda expuesto en `localhost:9092`. Se incluyen comandos básicos para crear topics, describirlos, publicar y consumir eventos desde la terminal usando los scripts de Kafka.

### Actividad 3. Actividad práctica

Cree los topics `orders`, `payments` e `inventory`. Publique al menos cinco eventos JSON y verifique en Kafka UI su topic, partición, offset, clave y contenido.

<details>
<summary><b>Desarrollo de la Actividad 3</b></summary>

**Comandos utilizados:**

```bash
# Crear topics

```

**Eventos publicados:**

| # | Topic | Clave | Partición | Offset | Contenido |
|---|-------|-------|-----------|--------|-----------|
| 1 | orders | | | | |
| 2 | orders | | | | |
| 3 | payments | | | | |
| 4 | payments | | | | |
| 5 | inventory | | | | |

**Captura de Kafka UI:**

</details>

---

## Capítulo 4. Productores y consumidores con Spring Boot

### Contexto

Se implementa una aplicación Spring Boot con productor y consumidor Kafka. La configuración se define en `application.yml` con `bootstrap-servers: localhost:9092`, serializador JSON (`JsonSerializer`/`JsonDeserializer`) y `group-id: order-service`. Se define el evento de dominio `OrderCreatedEvent` con atributos `orderId`, `customerId`, `total`, `status` y `occurredAt`. El productor usa `KafkaTemplate` para publicar en el topic `orders` usando `orderId` como clave. El consumidor usa `@KafkaListener`. Un `@RestController` expone un endpoint `POST /orders` que recibe la solicitud HTTP y publica el evento.

### Actividad 4. Trazabilidad del evento

Documente el recorrido del evento desde la solicitud HTTP hasta el consumidor. Indique topic, clave, partición, consumidor, Consumer Group y evidencia en Kafka UI.

<details>
<summary><b>Desarrollo de la Actividad 4</b></summary>

**Recorrido del evento:**

1. **Solicitud HTTP:** `POST /orders` con body `{"customerId":"...", "total": ...}`
2. **Controlador:** `OrderController.createOrder()` genera `OrderCreatedEvent`
3. **Productor:** `OrderEventProducer.publishOrderCreated()` envía al topic `orders`
4. **Kafka:** Asigna partición según la clave (`orderId`)
5. **Consumidor:** `OrderEventConsumer.consume()` recibe el evento

| Elemento | Valor |
|----------|-------|
| Topic | `orders` |
| Clave | `orderId` |
| Partición | |
| Offset | |
| Consumidor | `OrderEventConsumer` |
| Consumer Group | `inventory-service` |
| Evidencia Kafka UI | |

**Comando curl de prueba:**

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUS-01","total":120000}'
```

</details>

---

## Capítulo 5. Caso de estudio: sistema de pedidos basado en eventos

### Contexto

Una plataforma de comercio electrónico desea desacoplar los procesos de pedidos, pagos, inventario, facturación, notificaciones, analítica y auditoría usando Kafka. Se proponen 7 servicios lógicos (order-service, payment-service, inventory-service, invoice-service, notification-service, analytics-service, audit-service) y 6 topics (`orders`, `payments`, `inventory`, `invoices`, `notifications`, `audit`). La comunicación asíncrona introduce **consistencia eventual**: el pedido avanza por estados (CREATED → PAYMENT_APPROVED → INVENTORY_RESERVED → CONFIRMED/CANCELLED). No todo debe ser asíncrono — consultas como catálogo o autenticación pueden mantenerse con REST.

### Actividad 5. Diseño del flujo

Proponga los eventos, topics, productores, consumidores, Consumer Groups y claves de particionamiento para el flujo de compra. Justifique por qué no conviene usar un único topic global llamado `events`.

<details>
<summary><b>Desarrollo de la Actividad 5</b></summary>

**Flujo de eventos:**

| Evento | Topic | Productor | Consumidor(es) | Consumer Group | Clave |
|--------|-------|-----------|----------------|----------------|-------|
| `order-created` | `orders` | order-service | payment-service, inventory-service | `payment-service`, `inventory-service` | `orderId` |
| `payment-approved` / `payment-rejected` | `payments` | payment-service | invoice-service, notification-service | `invoice-service`, `notification-service` | `orderId` |
| `inventory-reserved` / `inventory-rejected` | `inventory` | inventory-service | notification-service | `notification-service` | `orderId` |
| `invoice-generated` | `invoices` | invoice-service | notification-service | `notification-service` | `orderId` |
| `notification-sent` | `notifications` | notification-service | analytics-service | `analytics-service` | `orderId` |
| `audit-record-created` | `audit` | todos | audit-service | `audit-service` | `correlationId` |

**¿Por qué no usar un único topic `events`?**

1.
2.
3.

</details>

---

## Capítulo 6. Laboratorio guiado extendido

### Contexto

El estudiante extiende la aplicación para que el evento `order-created` genere eventos posteriores de pagos e inventario. Se definen 3 topics obligatorios (`orders`, `payments`, `inventory`) y 3 eventos (`OrderCreatedEvent`, `PaymentProcessedEvent`, `InventoryProcessedEvent`). El flujo esperado: cliente crea pedido → order-service publica → payment-service y inventory-service consumen y publican resultados → notification-service y analytics-service reaccionan. Los consumidores de pago e inventario tienen lógica de negocio simplificada (aprueban según umbrales de total).

### Actividad 6. Evidencia y análisis

Cree pedidos con valores diferentes y reconstruya el flujo de eventos en Kafka UI. Identifique eventos generados, topics, claves, Consumer Groups, offsets y lag.

<details>
<summary><b>Desarrollo de la Actividad 6</b></summary>

**Pedidos creados:**

| Pedido | Total | Resultado Pago | Resultado Inventario |
|--------|-------|----------------|---------------------|
| ORD-001 | 100000 | | |
| ORD-002 | 260000 | | |
| ORD-003 | 350000 | | |

**Flujo de eventos en Kafka UI:**

| Evento | Topic | Clave | Partición | Offset | Consumer Group | Lag |
|--------|-------|-------|-----------|--------|----------------|-----|
| `order-created` | `orders` | | | | `payment-service` | |
| `order-created` | `orders` | | | | `inventory-service` | |
| `payment-approved/rejected` | `payments` | | | | `notification-service` | |
| `inventory-reserved/rejected` | `inventory` | | | | `notification-service` | |

**Capturas de Kafka UI:**

</details>

---

## Capítulo 7. Manejo de errores, reintentos y Dead Letter Topics

### Contexto

En sistemas distribuidos los errores son inevitables. Se clasifican en: **transitorios** (reintentar con backoff), **permanentes** (enviar a DLT), **de negocio** (publicar evento de error) y **técnicos** (reintentar y luego DLT). Un **Dead Letter Topic (DLT)** almacena eventos que no pudieron procesarse tras reintentos; es una herramienta de diagnóstico y recuperación. En Spring Kafka se configura con `DeadLetterPublishingRecoverer`, `FixedBackOff` y `DefaultErrorHandler`. La **idempotencia** permite procesar el mismo evento múltiples veces sin inconsistencias, usando `eventId`, restricciones únicas o tablas de eventos procesados.

### Actividad 7. Estrategia de errores

Diseñe una estrategia para manejar eventos fallidos en `inventory-service`. Indique cuándo reintentar, cuándo enviar a DLT, qué información revisar y cómo evitar reprocesamientos infinitos.

<details>
<summary><b>Desarrollo de la Actividad 7</b></summary>

**Estrategia propuesta:**

| Condición | Acción |
|-----------|--------|
| Error transitorio (ej. BD caída) | Reintentar con backoff |
| Error permanente (ej. datos inválidos) | Enviar a DLT |
| Error de negocio (ej. producto agotado) | Publicar `inventory-rejected` |

**Configuración de reintentos:**

- Número máximo de reintentos:
- Intervalo entre reintentos:
- Backoff:

**Dead Letter Topic:**

- Nombre del DLT:
- Información a registrar en el DLT:

**Estrategia de idempotencia:**

**¿Cómo evitar reprocesamientos infinitos?**

</details>

---

## Capítulo 8. Buenas prácticas de diseño con Kafka

### Contexto

Kafka no debe usarse para todo: es adecuado para desacoplamiento, alto volumen, procesamiento asíncrono, múltiples consumidores, reprocesamiento, auditoría y analítica en tiempo real, pero no para consultas simples donde REST es suficiente. Los eventos deben nombrarse como **hechos ocurridos en pasado** (ej. `order-created`, no `create-order`). Se recomienda incluir metadatos como `eventId`, `eventType`, `eventVersion`, `occurredAt`, `source` y `correlationId`. Las buenas prácticas abarcan: organización de topics por dominio, claves según entidad, particiones según volumen y paralelismo, replicación > 1 en producción, retención según necesidades, consumidores idempotentes, reintentos controlados con DLT, monitoreo de lag y versionado de eventos.

### Actividad 8. Diagnóstico de buenas prácticas

Revise una arquitectura que usa un topic `events`, mensajes sin clave, factor de replicación 1, sin DLT y sin monitoreo de lag. Identifique problemas, atributos afectados y mejoras prioritarias.

<details>
<summary><b>Desarrollo de la Actividad 8</b></summary>

| Problema | Atributo de calidad afectado | Mejora prioritaria |
|----------|------------------------------|-------------------|
| Topic único `events` | | |
| Mensajes sin clave | | |
| Factor de replicación 1 | | |
| Sin DLT | | |
| Sin monitoreo de lag | | |

**Resumen de mejoras:**

1.
2.
3.

</details>

---

## Capítulo 9. Actividades de consolidación

### Contexto

Este capítulo cierra el laboratorio con decisiones arquitectónicas concretas. Agrupa tres actividades que integran todos los conceptos vistos: clasificación de procesos sincronos/asíncronos, diseño completo del flujo de eventos, y diagnóstico arquitectónico de una configuración problemática.

### 9.1 Actividad 1. Decisiones de comunicación

Clasifique los siguientes procesos como REST, Kafka o arquitectura híbrida: *consultar catálogo, crear pedido, validar pago, enviar correo, actualizar analítica, registrar auditoría, consultar estado del pedido y actualizar inventario*. Justifique según respuesta inmediata, asincronía, múltiples consumidores y reprocesamiento.

<details>
<summary><b>Desarrollo de la Actividad 9.1</b></summary>

| Proceso | Tipo | Justificación |
|---------|------|---------------|
| Consultar catálogo | REST | |
| Crear pedido | Híbrido | |
| Validar pago | Kafka | |
| Enviar correo | Kafka | |
| Actualizar analítica | Kafka | |
| Registrar auditoría | Kafka | |
| Consultar estado del pedido | REST | |
| Actualizar inventario | Kafka | |

</details>

### 9.2 Actividad 2. Diseño del flujo de eventos

Diseñe el flujo de eventos para el proceso de compra. Incluya eventos principales, productor, consumidores, topic, clave de particionamiento y Consumer Group. Responda por qué no conviene un único topic `events`, por qué los consumidores deben tener grupos distintos y por qué `orderId` puede ser una buena clave.

<details>
<summary><b>Desarrollo de la Actividad 9.2</b></summary>

**Diagrama de flujo:**

```
[Cliente] → POST /orders → [order-service] → orders topic
  → [payment-service] (group: payment-service) → payments topic
  → [inventory-service] (group: inventory-service) → inventory topic
  → [notification-service] (group: notification-service)
  → [analytics-service] (group: analytics-service)
  → [audit-service] (group: audit-service)
```

**Tabla de diseño:**

| Evento | Topic | Productor | Consumidores | Consumer Group | Clave |
|--------|-------|-----------|--------------|----------------|-------|
| | | | | | |

**Preguntas:**

1. **¿Por qué no conviene un único topic `events`?**
   - 
2. **¿Por qué los consumidores deben tener grupos distintos?**
   - 
3. **¿Por qué `orderId` es una buena clave?**
   - 

</details>

### 9.3 Actividad 3. Diagnóstico arquitectónico

Configuración propuesta:
- Topic principal: `events`
- Particiones: 1
- Factor de replicación: 1
- Retención: 12 horas
- Mensajes sin clave
- Sin `eventId`
- Sin `correlationId`
- Todos los consumidores en el mismo Consumer Group
- Sin Dead Letter Topics
- Sin monitoreo de lag

Realice un diagnóstico técnico breve: problemas identificados, atributos de calidad afectados, riesgos para producción, cambios prioritarios y propuesta de mejora.

<details>
<summary><b>Desarrollo de la Actividad 9.3</b></summary>

| Problema | Atributo afectado | Riesgo | Cambio prioritario |
|----------|-------------------|--------|-------------------|
| Topic único `events` | | | |
| 1 partición | | | |
| Sin clave | | | |
| Sin eventId / correlationId | | | |
| Mismo Consumer Group | | | |
| Sin DLT | | | |
| Sin monitoreo de lag | | | |

**Propuesta de mejora integral:**

</details>

---

## Capítulo 10. Reto final, entregables y rúbrica

### Contexto

El reto final integra todos los conceptos: diseñar una arquitectura basada en eventos para una plataforma de comercio electrónico con 7 servicios (order, payment, inventory, invoice, notification, analytics, audit), 8 eventos y sus respectivos topics. El entregable es un documento técnico breve que incluya: descripción de la solución, arquitectura propuesta, tabla de servicios, tabla de eventos y topics, claves de particionamiento, Consumer Groups, estrategia de errores, riesgos y justificación de decisiones arquitectónicas. La rúbrica evalúa diseño de eventos (20%), topics y Consumer Groups (20%), justificación arquitectónica (25%), errores y observabilidad (15%), claridad (10%) y consistencia (10%).

### Desarrollo del Reto Final

<details>
<summary><b>Documento técnico — Reto Final</b></summary>

#### Descripción de la solución

*(Complete aquí)*

#### Arquitectura propuesta

*(Diagrama o descripción)*

#### Tabla de servicios

| Servicio | Responsabilidad | Consumer Group | Topics que consume | Topics que produce |
|----------|----------------|----------------|-------------------|-------------------|
| order-service | | | | |
| payment-service | | | | |
| inventory-service | | | | |
| invoice-service | | | | |
| notification-service | | | | |
| analytics-service | | | | |
| audit-service | | | | |

#### Tabla de eventos y topics

| Evento | Topic | Clave | Descripción |
|--------|-------|-------|-------------|
| `order-created` | `orders` | `orderId` | |
| `payment-approved` | `payments` | `orderId` | |
| `payment-rejected` | `payments` | `orderId` | |
| `inventory-reserved` | `inventory` | `orderId` | |
| `inventory-rejected` | `inventory` | `orderId` | |
| `invoice-generated` | `invoices` | `orderId` | |
| `notification-sent` | `notifications` | `orderId` | |
| `audit-record-created` | `audit` | `correlationId` | |

#### Claves de particionamiento

| Topic | Clave | Justificación |
|-------|-------|---------------|
| `orders` | `orderId` | |
| `payments` | `orderId` | |
| `inventory` | `orderId` | |
| `invoices` | `orderId` | |
| `notifications` | `orderId` | |
| `audit` | `correlationId` | |

#### Estrategia de errores

| Tipo de error | Acción | Reintentos | DLT |
|---------------|--------|-----------|-----|
| Transitorio | | | |
| Permanente | | | |
| Negocio | | | |

#### Riesgos identificados

1.
2.
3.

#### Justificación de decisiones arquitectónicas

*(Complete aquí)*

#### Consideraciones sobre consistencia eventual

*(Complete aquí)*

</details>

---

## Cómo usar este README

Cada capítulo incluye una sección **Contexto** que resume los conceptos clave, seguida de una sección desplegable (`<details>`) para desarrollar la actividad correspondiente. Complete cada tabla y回答 las preguntas según lo trabajado en el laboratorio. Para las actividades prácticas, incluya comandos, capturas de Kafka UI y evidencia de la ejecución.

---

*Documento generado a partir de la guía de laboratorio "Apache Kafka y Arquitecturas Orientadas por Eventos" — ARSW, Escuela Colombiana de Ingeniería Julio Garavito.*
