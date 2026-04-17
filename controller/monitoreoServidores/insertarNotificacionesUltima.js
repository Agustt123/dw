const { getConnectionLocalCdc, executeQuery } = require("../../db");
const MONITOREO_TIMEOUT_MS = 5000;

function toInt(value, fallback = 0) {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? fallback : parsed;
}

function toDecimal(value, fallback = null) {
    if (value === undefined || value === null || value === "") return fallback;

    const parsed = Number.parseFloat(value);
    return Number.isNaN(parsed) ? fallback : parsed;
}

function toJsonValue(value) {
    if (value === undefined || value === null || value === "") return null;

    if (typeof value === "string") {
        try {
            return JSON.stringify(JSON.parse(value));
        } catch {
            return JSON.stringify(value);
        }
    }

    return JSON.stringify(value);
}

async function insertarNotificacionesUltima(body = {}) {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const payload = {
            token: body.token || null,
            image_url: body.image_url || null,
            fecha: body.fecha || null,
            mes: body.mes || null,
            cantidad_dia: toInt(body.cantidad_dia, 0),
            cantidad_mes: toInt(body.cantidad_mes, 0),
            anio_cantidad: toInt(body.anio_cantidad, 0),
            hoy_movimiento: toInt(body.hoy_movimiento, 0),
            sev: body.sev || "verde",
            max_streak: toInt(body.max_streak, 0),
            afectados: toJsonValue(body.afectados),
            uso_cpu: toDecimal(body.uso_cpu),
            uso_ram: toDecimal(body.uso_ram),
            uso_disco: toDecimal(body.uso_disco),
            pct_max: toDecimal(body.pct_max),
            sat_sev: body.sat_sev || "verde",
            sat_resumen: body.sat_resumen || null,
            sat_afectados: toJsonValue(body.sat_afectados),
            peor_pct: toInt(body.peor_pct, 0),
            tiempo_imagen_ms: toInt(body.tiempo_imagen_ms, 0),
            enviada: toInt(body.enviada, 0),
        };

        const result = await executeQuery(
            db,
            `
                INSERT INTO notificaciones_detalle (
                    autofecha,
                    token,
                    image_url,
                    fecha,
                    mes,
                    cantidad_dia,
                    cantidad_mes,
                    anio_cantidad,
                    hoy_movimiento,
                    sev,
                    max_streak,
                    afectados,
                    uso_cpu,
                    uso_ram,
                    uso_disco,
                    pct_max,
                    sat_sev,
                    sat_resumen,
                    sat_afectados,
                    peor_pct,
                    tiempo_imagen_ms,
                    enviada
                ) VALUES (NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
                payload.token,
                payload.image_url,
                payload.fecha,
                payload.mes,
                payload.cantidad_dia,
                payload.cantidad_mes,
                payload.anio_cantidad,
                payload.hoy_movimiento,
                payload.sev,
                payload.max_streak,
                payload.afectados,
                payload.uso_cpu,
                payload.uso_ram,
                payload.uso_disco,
                payload.pct_max,
                payload.sat_sev,
                payload.sat_resumen,
                payload.sat_afectados,
                payload.peor_pct,
                payload.tiempo_imagen_ms,
                payload.enviada,
            ],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );

        return {
            estado: true,
            message: "Notificacion insertada correctamente",
            id: result?.insertId || 0,
            data: payload,
        };
    } catch (error) {
        console.error("Error en insertarNotificacionesUltima:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudo insertar la notificacion",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { insertarNotificacionesUltima };
