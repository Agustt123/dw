const { getConnectionLocal, executeQuery } = require("../../db.js");

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
        connection = await getConnectionLocal();

        // ✅ Trae didCliente en el mismo SELECT (sin N+1)
        const selectQuery = `
      SELECT e.didOwner,
             e.didEnvio,
             e.estado,
             e.autofecha,
             e.quien,
             e.didCadete,
             v.didCliente
      FROM estado e
      LEFT JOIN envios v
        ON v.didOwner = e.didOwner
       AND v.didEnvio  = e.didEnvio
       AND v.elim = 0
       AND v.superado = 0
      WHERE e.cdc = 0 AND e.didOwner = ?
      LIMIT 50
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            //     console.log(`ℹ️ No hay estados pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

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

            for (const ejecutar of ejecutadores) {
                // En tu código estaba siempre didCliente, lo dejo igual para no cambiar lógica
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

            const result = await executeQuery(connection, updateQuery, [didOwner, didEnvio]);
            if (result.affectedRows === 0) {
                //   console.log(`ℹ️ No se pudo actualizar el estado para didEnvio ${didEnvio}`);
                continue;
            }

            //  console.log(`✅ CDC insertado x${ejecutadores.length} (estado) y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
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
        connection = await getConnectionLocal();

        // ✅ Trae didCliente en el mismo SELECT (sin N+1)
        const selectQuery = `
      SELECT a.didOwner,
             a.didEnvio,
             a.operador,
             a.autofecha,
             a.estado,
             v.didCliente
      FROM asignaciones a
      LEFT JOIN envios v
        ON v.didOwner = a.didOwner
       AND v.didEnvio  = a.didEnvio
       AND v.elim = 0
       AND v.superado = 0
      WHERE a.cdc = 0
        AND a.didOwner = ?
      
      LIMIT 500
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            //       console.log(`ℹ️ No hay asignaciones pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

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
                const clienteInsertar = (ejecutar === "estado") ? (didCliente ?? null) : null;

                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    operador || 0,
                    autofecha,
                    disparador,
                    clienteInsertar,
                    valorEstado,
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);

            //      console.log(`✅ CDC insertado x${ejecutadores.length} (asignaciones) y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
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
