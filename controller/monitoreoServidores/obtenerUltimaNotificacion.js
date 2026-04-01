const { getConnectionLocalCdc, executeQuery } = require("../../db");
const MONITOREO_TIMEOUT_MS = 5000;

function parseJsonValue(value) {
    if (value === null || value === undefined || value === "") return null;

    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}

async function obtenerUltimaNotificacion() {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const rows = await executeQuery(
            db,
            `
                SELECT
                    id,
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
                    tiempo_imagen_ms
                FROM notificaciones_detalle
                ORDER BY id DESC
                LIMIT 1
            `,
            [],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );

        const row = rows?.[0];
        if (!row) return null;

        return {
            ...row,
            afectados: parseJsonValue(row.afectados),
            sat_afectados: parseJsonValue(row.sat_afectados),
        };
    } catch (error) {
        console.error("Error en obtenerUltimaNotificacion:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudo obtener la ultima notificacion",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { obtenerUltimaNotificacion };
