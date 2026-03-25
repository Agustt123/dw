const { getConnectionLocalCdc, executeQuery } = require("../../db");

function toJsonValue(value) {
    if (value === undefined || value === null || value === "") return null;

    if (typeof value === "string") {
        try {
            return JSON.stringify(JSON.parse(value));
        } catch {
            return JSON.stringify({ raw: value });
        }
    }

    return JSON.stringify(value);
}

async function insertarAlerta(body = {}) {
    let db;

    try {
        db = await getConnectionLocalCdc();

        const payload = {
            token: body.token || null,
            origen: body.origen || null,
            modulo: body.modulo || null,
            servicio: body.servicio || null,
            tipo_alerta: body.tipo_alerta || body.tipo || "error",
            sev: body.sev || "rojo",
            titulo: body.titulo || "Alerta de monitoreo",
            mensaje: body.mensaje || body.descripcion || null,
            fecha_evento: body.fecha_evento || null,
            image_url: body.image_url || null,
            error_json: toJsonValue(body.error_json ?? body.error ?? body.detalle_error),
            contexto_json: toJsonValue(body.contexto_json ?? body.contexto ?? body.payload),
        };

        const result = await executeQuery(
            db,
            `
                INSERT INTO alertas (
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
                ) VALUES (NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
                payload.token,
                payload.origen,
                payload.modulo,
                payload.servicio,
                payload.tipo_alerta,
                payload.sev,
                payload.titulo,
                payload.mensaje,
                payload.fecha_evento,
                payload.image_url,
                payload.error_json,
                payload.contexto_json,
            ]
        );

        return {
            estado: true,
            message: "Alerta insertada correctamente",
            id: result?.insertId || 0,
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
