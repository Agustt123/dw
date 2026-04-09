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
             COALESCE(v_act.didCliente, v_any.didCliente) AS didCliente,
             COALESCE(v_act.fecha_inicio, v_any.fecha_inicio) AS fecha_inicio
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
      LIMIT 10000
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);
        if (rows.length === 0) return;

        const insertQuery = `
      INSERT IGNORE INTO cdc
  (didOwner, didPaquete, ejecutar, estado, disparador, didCliente, fecha, fecha_inicio, didChofer, quien)
VALUES
  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const updateQuery = `
      UPDATE estado SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        const ejecutadores = ["estado"];
        const disparador = "estado";

        for (const row of rows) {
            const { didOwner, didEnvio, estado, autofecha, quien, didCadete, didCliente, fecha_inicio } = row;

            // ✅ si el cliente es obligatorio, podés decidir qué hacer cuando falta:
            // - o "continue" para no marcar cdc (y reintentar más tarde)
            // - o insertar igual con null (y que el consumer lo resuelva)
            // acá lo dejo insertando (pero podés cambiarlo)
            for (const ejecutar of ejecutadores) {
                try {
                    await executeQuery(connection, insertQuery, [
                        didOwner,
                        didEnvio,
                        ejecutar,
                        estado,
                        disparador,
                        didCliente ?? null,
                        autofecha,
                        fecha_inicio ?? null,
                        didCadete || 0,
                        quien || 0,
                    ]);
                } catch (insertErr) {
                    console.error(`❌ [CDC] Error insertando estado en cdc didOwner=${didOwner}, didEnvio=${didEnvio}:`, insertErr.message);
                    continue; // No marcar cdc=1 si insert falló
                }
            }

            const result = await executeQuery(connection, updateQuery, [didOwner, didEnvio], true);
            if (result.affectedRows === 0) {
                console.warn(`⚠️ [CDC] No se pudo marcar cdc=1 para estado didOwner=${didOwner}, didEnvio=${didEnvio}`);
                continue;
            }
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
             COALESCE(v_act.didCliente, v_any.didCliente) AS didCliente,
             COALESCE(v_act.fecha_inicio, v_any.fecha_inicio) AS fecha_inicio
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
      LIMIT 10000
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);
        if (rows.length === 0) return;

        const insertQuery = `
      INSERT IGNORE INTO cdc
        (didOwner, didPaquete, ejecutar, didChofer, fecha, disparador, didCliente, estado, fecha_inicio)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const updateQuery = `
      UPDATE asignaciones SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        const ejecutadores = ["estado"];
        const disparador = "asignaciones";

        for (const row of rows) {
            const { didOwner, didEnvio, operador, autofecha, estado, didCliente, fecha_inicio } = row;
            const valorEstado = (estado !== undefined) ? estado : null;

            for (const ejecutar of ejecutadores) {
                try {
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
                        fecha_inicio ?? null,
                    ]);
                } catch (insertErr) {
                    console.error(`❌ [CDC] Error insertando asignacion en cdc didOwner=${didOwner}, didEnvio=${didEnvio}:`, insertErr.message);
                    continue; // No marcar cdc=1 si insert falló
                }
            }

            try {
                await executeQuery(connection, updateQuery, [didOwner, didEnvio]);
            } catch (updateErr) {
                console.error(`❌ [CDC] Error marcando asignacion cdc=1 didOwner=${didOwner}, didEnvio=${didEnvio}:`, updateErr.message);
            }
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}:`, error);
    } finally {
        await closeConn(connection);
    }
}

module.exports = {
    EnviarcdcEstado,
    EnviarcdAsignacion
};
