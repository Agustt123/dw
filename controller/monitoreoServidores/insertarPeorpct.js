const { getConnectionLocalCdc, executeQuery } = require("../../db");
const MONITOREO_TIMEOUT_MS = 5000;

function toInt(value, fallback = 0) {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? fallback : parsed;
}

async function insertarPeorPct(body = {}) {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const payload = {
            autofecha: new Date(),
            cantidad_dia: toInt(body.cantidad_dia, 0),
            peor_pct: toInt(body.peor_pct, 0),
            tiempo_imagen_ms: toInt(body.tiempo_imagen_ms, 0),
        };

        const result = await executeQuery(
            db,
            `
                INSERT INTO notificaciones_peor (
                    autofecha,
                    cantidad_dia,
                    peor_pct,
                    tiempo_imagen_ms
                ) VALUES (?, ?, ?, ?)
            `,
            [
                payload.autofecha,
                payload.cantidad_dia,
                payload.peor_pct,
                payload.tiempo_imagen_ms,
            ],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );

        return {
            estado: true,
            message: "Resumen insertado correctamente",
            id: result?.insertId || 0,
            data: payload,
        };
    } catch (error) {
        console.error("Error en insertarPeorPct:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudo insertar el resumen",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { insertarPeorPct };
