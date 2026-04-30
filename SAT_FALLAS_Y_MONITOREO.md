# SAT - Relevamiento Actual

## 1. Servicios que hoy se toman para metricas de recursos

Estos servicios se consultan desde `controller/monitoreoServidores/monitoreoMetricas.js`
mediante sus endpoints `_sat/metrics`:

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

Ademas se arma un registro especial:

- `conjunto`

Ese registro `conjunto` resume los valores maximos de los servicios monitoreados
para CPU, RAM, disco, latencia y otras metricas.

## 2. Servicios que hoy se toman para monitoreo de base de datos

Estos servicios se consultan desde `controller/monitoreoServidores/monitoreoBd.js`
mediante sus endpoints `_sat/procesos`:

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

no se toman en ese proceso:

- `ffmobile : (base de datos) ` 
- `websocket_mail : (base de datos)`
- `etiquetas : (base de datos)`
- `ftp varios (app, .com, files, cuentas arg, preenvios tn, asignaciones, registrarVisita)`
- `app vieja que solo usa una empresa`
- `integraciones con ecommerce`


Hoy no estan incluidos en el monitoreo de procesos Esto puede deberse a que:

- no cuentan con base propia en uso
- no exponen hoy un endpoint de procesos
- no aplica monitoreo de procesos DB para ese servicio
- todavia no fueron incorporados a ese flujo
- dificultad al medir como testear procesos del ftp o php: distinguir cuales son funcionales y cuales no
- la configuracion incluye procesos manuales (ej vincular ecommerce)
- desconocimiento de la apk en circulacion o proceso del ftp de la app movil vieja

Tambien se arma un registro especial:

- `conjunto`

Ese registro consolida la informacion total o maxima del monitoreo de procesos DB.

## 3. Metricas que hoy tenemos para recursos de servidor

Desde los endpoints `_sat/metrics` hoy se toman estas metricas:

- `usoRam`
- `usoCpu`
- `usoDisco`
- `temperaturaCpu`
- `carga1m`
- `ramProcesoMb`
- `cpuProceso`
- `latenciaMs`
- `codigoHttp`
- `error`
- `ok`

## 4. Metricas que hoy tenemos para base de datos

Desde los endpoints `_sat/procesos` hoy se toman estas metricas:

- `procesos`
- `total_segundos`
- `promedio_segundos`
- `max_segundos`
- `latenciaMs`
- `codigoHttp`
- `error`
- `ok`

## 5. Tablas principales que hoy participan en SAT dentro de DataWarehouse

### Tablas de monitoreo SAT

- `sat_monitoreo_recursos`
- `sat_monitoreo_db`
- `notificaciones_detalle`
- `alertas`
- `notificaciones_peor`

### Tablas usadas para cantidad operativa

- `home_app_resumen`
- `home_app`

## 6. Que guarda cada tabla principal

### `sat_monitoreo_recursos`

Guarda el monitoreo tecnico de recursos por servicio:

- servidor
- endpoint
- ok
- codigoHttp
- latenciaMs
- error
- usoRam
- usoCpu
- usoDisco
- temperaturaCpu
- carga1m
- ramProcesoMb
- cpuProceso
- did

### `sat_monitoreo_db`

Guarda el monitoreo de procesos de base de datos por servicio:

- servidor
- endpoint
- ok
- codigoHttp
- latenciaMs
- error
- procesos
- total_segundos
- promedio_segundos
- max_segundos
- did

### `notificaciones_detalle`

Guarda el detalle del estado analizado para la app o para notificaciones:

- fecha
- mes
- cantidad_dia
- cantidad_mes
- anio_cantidad
- hoy_movimiento
- sev
- max_streak
- afectados
- uso_cpu
- uso_ram
- uso_disco
- pct_max
- sat_sev
- sat_resumen
- sat_afectados
- peor_pct
- tiempo_imagen_ms
- enviada
- token
- image_url

### `alertas`

Guarda las alertas asociadas a una notificacion:

- id
- did_notificaciones
- autofecha
- sev
- color
- porcentaje_error
- titulo
- resumen_alerta
- que_fallo
- detalle_alerta
- token
- image_url
- origen

### `notificaciones_peor`

Guarda snapshots resumidos del peor porcentaje:

- autofecha
- cantidad_dia
- peor_pct
- tiempo_imagen_ms

## 7. Que datos toma IAOFICIAL desde DataWarehouse

Desde `IAOFICIAL` hoy se consumen estos endpoints de `DW`:

- `POST /cantidad`
- `GET /monitoreo`
- `GET /monitoreo/metricas`
- `GET /monitoreo/procesos-conjunto`
- `GET /monitoreo/notificaciones-ultima`
- `GET /monitoreo/alerta`
- `GET /monitoreo/peor-pct`

## 8. Que informacion usa IAOFICIAL

### Desde `/cantidad`

IAOFICIAL toma:

- `fecha`
- `mes`
- `hoy`
- `hoyMovimiento`
- `mesCantidad`
- `anioCantidad`
- `nombre`

### Desde `/monitoreo/metricas`

IAOFICIAL toma:

- `usoCpu`
- `usoRam`
- `usoDisco`
- `latenciaMs`
- `carga1m`
- `ramProcesoMb`
- `cpuProceso`
- `temperaturaCpu`
- `codigoHttp`
- `error`

### Desde `/monitoreo/procesos-conjunto`

IAOFICIAL toma:

- `servidor`
- `ok`
- `codigoHttp`
- `latenciaMs`
- `error`
- `procesos`
- `total_segundos`
- `promedio_segundos`
- `max_segundos`

### Desde `/monitoreo/notificaciones-ultima`

IAOFICIAL usa el detalle guardado para reconstruir estado actual o relacionarlo con alertas.

### Desde `/monitoreo/alerta`

IAOFICIAL usa:

- `id`
- `did_notificaciones`
- `sev`
- `color`
- `porcentaje_error`
- `titulo`
- `resumen_alerta`
- `que_fallo`
- `detalle_alerta`
- `token`
- `image_url`

### Desde `/monitoreo/peor-pct`

IAOFICIAL usa:

- `peor_pct`
- `cantidad_dia`
- `tiempo_imagen_ms`
- `autofecha`

## 9. Datos funcionales que hoy ya existen

Hoy ya existen en el sistema estos conceptos funcionales:

- severidad general
- microservicios afectados
- focos activos
- peor porcentaje
- detalle de notificacion
- alerta asociada a una notificacion
- riesgo funcional de disco
- detalle de metricas por servidor en `notificaciones-ultima/v2`

## 10. Punto importante para documentacion

Si hubiera que resumir SAT hoy de manera concreta, se puede decir que:

- monitorea microservicios
- monitorea recursos de servidores
- monitorea procesos de base de datos
- guarda snapshots y alertas
- expone informacion para la app movil
- resume el estado del sistema en niveles de severidad y porcentaje de riesgo

## 11. Que puede fallar

### Microservicios

- un microservicio puede caerse
- un microservicio puede responder lento
- un microservicio puede devolver timeout
- un microservicio puede devolver error
- un microservicio puede acumular fallas consecutivas
- un microservicio puede degradarse sin llegar a caerse por completo

### Servidores

- CPU alta
- RAM alta
- disco en zona critica
- latencia alta
- carga del sistema alta
- uso excesivo de CPU por proceso
- uso excesivo de RAM por proceso
- temperatura elevada
- servidor inaccesible

### Base de datos

- errores de conexion
- procesos bloqueados
- demasiados procesos activos
- lentitud sostenida
- tiempos maximos elevados
- tiempos promedio altos
- instancia degradada

### Infraestructura e integraciones

- DataWarehouse caido
- errores 500 o 502 en endpoints
- problemas de red
- jobs o cron detenidos
- errores al persistir informacion
- errores al consumir endpoints internos

### Notificaciones y visibilidad

- no se envia una push
- token invalido
- se detecta un problema pero no se informa
- se guarda informacion pero no se relaciona correctamente
- la app muestra informacion incompleta o atrasada

### FTP :disponibilidad del FTP
 
- procesos php dañados o colgados
- bloqueos
- errores de redireccion por cache


## 12. Que informacion nos faltaria

Hoy ya existe una base concreta de monitoreo. No se parte de cero. Sin embargo, todavia falta
ordenar mejor la cobertura del sistema y definir formalmente algunos puntos.

### Lo que ya tenemos bastante claro

- que servicios se monitorean hoy
- que metricas ya existen
- que tablas participan
- que endpoints se consumen
- que parte del estado general ya se calcula
- que existen alertas, snapshots y detalle de notificaciones
- que ya hay una interpretacion funcional de riesgo en varios casos

### Lo que todavia falta definir mejor

- totalidad de mediciones de toda la empresa
- listado completo de todo lo que puede fallar
- que fallas deben considerarse criticas automaticamente
- que fallas representan solo advertencia
- que recursos todavia no estan cubiertos
- que servicios hoy tienen cobertura parcial
- que metricas son solo informativas y cuales deben disparar alertas
- que informacion adicional falta para interpretar mejor la capacidad del servidor
- que casos de infraestructura e integracion aun no tienen buena trazabilidad


### Conclusion funcional

Hoy SAT ya tiene una base solida sobre:

- microservicios
- servidores
- base de datos
- alertas
- notificaciones

Lo que todavia falta no es construir el sistema desde cero, sino ordenar con mas claridad:

- todas las cosas que pueden fallar
- cuales ya se monitorean efectivamente
- que informacion adicional haria falta para completar la cobertura
