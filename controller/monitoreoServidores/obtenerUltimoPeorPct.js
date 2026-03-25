const { getConnectionLocalCdc, executeQuery } = require("../../db");

async function obtenerUltimoPeorPct() {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const rows = await executeQuery(
            db,
            `
                SELECT
                    id,
                    autofecha,
                    cantidad_dia,
                    peor_pct,
                    tiempo_imagen_ms
                FROM notificaciones_peor
                ORDER BY id DESC
                LIMIT 30
            `
        );

        return rows || null;
    } catch (error) {
        console.error("Error en obtenerUltimoPeorPct:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudo obtener el ultimo peor_pct",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { obtenerUltimoPeorPct };
