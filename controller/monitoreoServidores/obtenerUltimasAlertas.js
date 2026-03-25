const { getConnectionLocalCdc, executeQuery } = require("../../db");

function parseJsonValue(value) {
    if (value === null || value === undefined || value === "") return null;

    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}

async function obtenerUltimasAlertas() {
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
                    origen,
                    modulo,
                    servicio,
                    tipo_alerta,
                    sev,
                    titulo,
                    mensaje,
                    fecha_evento,
                    image_url,
                    error_json,
                    contexto_json
                FROM alertas
                ORDER BY id DESC
                LIMIT 50
            `
        );

        return (rows || []).map((row) => ({
            ...row,
            error_json: parseJsonValue(row.error_json),
            contexto_json: parseJsonValue(row.contexto_json),
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
