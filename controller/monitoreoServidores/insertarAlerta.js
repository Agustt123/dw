const { getConnectionLocalCdc, executeQuery } = require("../../db");

function toInt(value, fallback = null) {
    const parsed = Number.parseInt(value, 10);
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

async function insertarAlerta(body = {}) {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const payload = {
            did_notificaciones: toInt(body.did_notificaciones, null),
            autofecha: body.autofecha || null,
            sev: body.sev || null,
            color: body.color || body.sev || "rojo",
            porcentaje_error: toInt(body.porcentaje_error, null),
            titulo: body.titulo || "Alerta de monitoreo",
            resumen_alerta: body.resumen_alerta || null,
            que_fallo: body.que_fallo || null,
            detalle_alerta: toJsonValue(body.detalle_alerta),
            origen: body.origen || null,
            image_url: body.image_url || null,
            token: body.token || null,
        };

        const result = await executeQuery(
            db,
            `
                INSERT INTO alertas (
                    did_notificaciones,
                    autofecha,
                    sev,
                    color,
                    porcentaje_error,
                    titulo,
                    resumen_alerta,
                    que_fallo,
                    detalle_alerta,
                    origen,
                    image_url,
                    token
                ) VALUES (?, COALESCE(?, NOW()), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
                payload.did_notificaciones,
                payload.autofecha,
                payload.sev,
                payload.color,
                payload.porcentaje_error,
                payload.titulo,
                payload.resumen_alerta,
                payload.que_fallo,
                payload.detalle_alerta,
                payload.origen,
                payload.image_url,
                payload.token,
            ]
        );

        const rows = await executeQuery(
            db,
            `
                SELECT id, autofecha
                FROM alertas
                WHERE id = ?
                LIMIT 1
            `,
            [result?.insertId || 0]
        );

        const inserted = rows?.[0] || null;

        return {
            estado: true,
            message: "Alerta insertada correctamente",
            id: result?.insertId || 0,
            autofecha: inserted?.autofecha || payload.autofecha || null,
            data: payload,
        };
    } catch (error) {
        console.error("Error en insertarAlerta:", error);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || "No se pudo insertar la alerta",
            },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { insertarAlerta };
