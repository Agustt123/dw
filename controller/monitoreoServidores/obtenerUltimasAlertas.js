const { getConnectionLocalCdc, executeQuery } = require("../../db");

function parseJsonValue(value) {
    if (value === null || value === undefined || value === "") return null;

    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}


function toLimit(value, fallback = 10) {
    const parsed = Number.parseInt(value, 10);

    if (Number.isNaN(parsed) || parsed <= 0) return fallback;

    return Math.min(parsed, 100);
}

async function obtenerUltimasAlertas(limitParam) {
    let db;

    try {
        db = await getConnectionLocalCdc();
        const limit = toLimit(limitParam, 10);

        const rows = await executeQuery(
            db,
            `
                SELECT
                    id,
                    did_notificaciones,
                    autofecha,
                    sev,
                    color,
                    porcentaje_error,
                    titulo,
                    resumen_alerta,
                    que_fallo,
                    detalle_alerta,
                    token,
                    image_url,
                    origen
                FROM alertas
                ORDER BY id DESC
                LIMIT ?
            `
            ,
            [limit]
        );

        return (rows || []).map((row) => ({
            ...row,
            detalle_alerta: parseJsonValue(row.detalle_alerta),
        }));
    } catch (error) {
        console.error("Error en obtenerUltimasAlertas:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudieron obtener las alertas",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { obtenerUltimasAlertas };
