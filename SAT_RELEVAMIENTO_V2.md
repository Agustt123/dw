# SAT - Relevamiento V2

## 1. Que monitoreamos hoy

Hoy SAT monitorea 3 grandes grupos:

- servidor
- microservicios
- base de datos

## 2. Que puede fallar

A nivel general, hoy puede fallar:

- el servidor
- un microservicio
- la base de datos
- la red o una integracion
- la obtencion de datos de monitoreo

## 3. Servidor

### Que monitoreamos hoy

- uso de CPU
- uso de RAM
- uso de disco
- latencia
- carga1m
- RAM del proceso
- CPU del proceso
- temperatura CPU
- estado OK o error
- codigo HTTP

### Que puede fallar

- CPU alta
- RAM alta
- disco en zona critica
- latencia alta
- carga alta
- exceso de consumo de un proceso puntual
- temperatura elevada
- servidor inaccesible

### Que servicios tenemos hoy

- `dw`
- `asignaciones`
- `backgps`
- `colecta`
- `aplanta`
- `produccion`
- `fulfillment`
- `ffmobile`
- `callback`
- `lightdatito`
- `websocket_mail`
- `etiquetas`
- `estados`
- `apimovil`

## 4. Microservicios

### Que monitoreamos hoy

- disponibilidad
- tiempo de respuesta
- fallas consecutivas
- microservicios afectados

### Que puede fallar

- servicio caido
- servicio lento
- timeout
- error de aplicacion
- acumulacion de fallas consecutivas
- degradacion sostenida

## 5. Base de datos

### Que monitoreamos hoy

- cantidad de procesos
- tiempo total de procesos
- tiempo promedio
- tiempo maximo
- latencia
- estado OK o error
- codigo HTTP

### Que puede fallar

- errores de conexion
- procesos bloqueados
- demasiados procesos activos
- lentitud sostenida
- tiempos maximos altos
- tiempos promedio altos
- instancia degradada

### Que bases o servicios tenemos hoy

- `asignaciones`
- `backgps`
- `colecta`
- `aplanta`
- `produccion`
- `fulfillment`
- `callback`
- `lightdatito`
- `estados`
- `apimovil`

### Que hoy no tenemos monitoreado

- `ffmobile`
- `websocket_mail`
- `etiquetas`
- `ftp varios`
- `app vieja`
- `integraciones con ecommerce`

## 6. Que informacion tenemos hoy

Hoy ya tenemos:

- metricas de servidor
- estado de microservicios
- procesos de base de datos
- tiempos de respuesta
- errores
- codigos HTTP
- fallas consecutivas
- microservicios afectados
- severidad general
- peor porcentaje
- alertas

## 7. Que informacion nos faltaria

Todavia faltaria ordenar mejor:

- todo lo que puede fallar
- todo lo que deberia monitorearse
- que esta monitoreado y que no
- umbrales claros para cada foco
- cobertura completa de todos los servicios
- mas claridad sobre lo que hoy no esta incluido
