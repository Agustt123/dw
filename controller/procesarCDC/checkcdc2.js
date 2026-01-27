const { getConnectionLocalCdc, executeQuery } = require("../../db.js");

// helper: cerrar conn sea pool o createConnection
async function closeConn(conn) {
    try {
        if (conn?.release) conn.release();
        else if (conn?.end) await conn.end();
        else if (conn?.destroy) conn.destroy();
    } catch (e) {
        console.error("❌ Error cerrando conexión:", e?.message || e);
    }
}

// ==============================
// ESTADOS → CDC
// ==============================
async function EnviarcdcEstado(didOwner) {
    let connection;
    try {
        connection = await getConnectionLocalCdc();

        // ✅ didCliente siempre (activo si existe, sino fallback a cualquiera)
        const selectQuery = `
      SELECT e.didOwner,
             e.didEnvio,
             e.estado,
             e.autofecha,
             e.quien,
             e.didCadete,
             COALESCE(v_act.didCliente, v_any.didCliente) AS didCliente
      FROM estado e
      LEFT JOIN envios v_act
        ON v_act.didOwner = e.didOwner
       AND v_act.didEnvio = e.didEnvio
       AND v_act.elim = 0
       AND v_act.superado = 0
      LEFT JOIN envios v_any
        ON v_any.didOwner = e.didOwner
       AND v_any.didEnvio = e.didEnvio
      WHERE e.cdc = 0
        AND e.didOwner = ?
      LIMIT 500
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);
        if (rows.length === 0) return;

        const insertQuery = `
      INSERT IGNORE INTO cdc
        (didOwner, didPaquete, ejecutar, estado, disparador, didCliente, fecha, didChofer, quien)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const updateQuery = `
      UPDATE estado SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        const ejecutadores = ["verificarCierre", "estado"];
        const disparador = "estado";

        for (const row of rows) {
            const { didOwner, didEnvio, estado, autofecha, quien, didCadete, didCliente } = row;

            // ✅ si el cliente es obligatorio, podés decidir qué hacer cuando falta:
            // - o "continue" para no marcar cdc (y reintentar más tarde)
            // - o insertar igual con null (y que el consumer lo resuelva)
            // acá lo dejo insertando (pero podés cambiarlo)
            for (const ejecutar of ejecutadores) {
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    estado,
                    disparador,
                    didCliente ?? null,
                    autofecha,
                    didCadete || 0,
                    quien || 0,
                ]);
            }

            const result = await executeQuery(connection, updateQuery, [didOwner, didEnvio], true);
            if (result.affectedRows === 0) continue;
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdcEstado para didOwner ${didOwner}:`, error);
    } finally {
        await closeConn(connection);
    }
}

// ==============================
// ASIGNACIONES → CDC
// ==============================
async function EnviarcdAsignacion(didOwner) {
    let connection;
    try {
        connection = await getConnectionLocalCdc();

        // ✅ didCliente siempre (activo si existe, sino fallback a cualquiera)
        const selectQuery = `
      SELECT a.didOwner,
             a.didEnvio,
             a.operador,
             a.autofecha,
             a.estado,
             COALESCE(v_act.didCliente, v_any.didCliente) AS didCliente
      FROM asignaciones a
      LEFT JOIN envios v_act
        ON v_act.didOwner = a.didOwner
       AND v_act.didEnvio = a.didEnvio
       AND v_act.elim = 0
       AND v_act.superado = 0
      LEFT JOIN envios v_any
        ON v_any.didOwner = a.didOwner
       AND v_any.didEnvio = a.didEnvio
      WHERE a.cdc = 0
        AND a.didOwner = ?
      LIMIT 500
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);
        if (rows.length === 0) return;

        const insertQuery = `
      INSERT IGNORE INTO cdc
        (didOwner, didPaquete, ejecutar, didChofer, fecha, disparador, didCliente, estado)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const updateQuery = `
      UPDATE asignaciones SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        const ejecutadores = ["verificarCierre", "estado"];
        const disparador = "asignaciones";

        for (const row of rows) {
            const { didOwner, didEnvio, operador, autofecha, estado, didCliente } = row;
            const valorEstado = (estado !== undefined) ? estado : null;

            for (const ejecutar of ejecutadores) {
                // ✅ ACÁ estaba el bug: ahora SIEMPRE manda didCliente
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    operador || 0,
                    autofecha,
                    disparador,
                    didCliente ?? null,
                    valorEstado,
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}:`, error);
    } finally {
        await closeConn(connection);
    }
}

EnviarcdcEstado(164);

module.exports = {
    EnviarcdcEstado,
    EnviarcdAsignacion
};
