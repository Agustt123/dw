const { getConnectionLocal, executeQuery } = require("../../db.js");

// ==============================
// ESTADOS → CDC
// ==============================
async function EnviarcdcEstado(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
      SELECT didOwner, didEnvio, estado, autofecha,quien,didCadete
      FROM estado
      WHERE cdc = 0 AND didOwner = ? AND autofecha >= '2025-10-10 00:00:00'
      LIMIT 50
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay estados pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // Inserta en CDC (con columna 'estado' y 'didCliente' solo cuando ejecutar = 'estado')
        const insertQuery = `
      INSERT IGNORE INTO cdc (didOwner, didPaquete, ejecutar, estado, disparador, didCliente, fecha,didChofer,quien)
      VALUES (?, ?, ?, ?, ?, ?, ?,?,?)
    `;

        const updateQuery = `
      UPDATE estado SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        // Solo estos dos ejecutores
        const ejecutadores = ["verificarCierre", "estado"];
        const disparador = "estado";

        for (const row of rows) {
            const { didOwner, didEnvio, estado, autofecha, quien, didCadete } = row;

            // Buscar didCliente del envío
            const getDidClienteQuery = `
        SELECT didCliente
        FROM envios
        WHERE didEnvio = ? AND didOwner = ? AND elim = 0 AND superado = 0
        LIMIT 1
      `;
            const rowsCliente = await executeQuery(connection, getDidClienteQuery, [didEnvio, didOwner]);
            const didCliente = rowsCliente.length > 0 ? rowsCliente[0].didCliente : null;

            for (const ejecutar of ejecutadores) {
                const clienteInsertar = (ejecutar === "estado") ? didCliente : didCliente;
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    estado,         // solo tiene sentido para 'estado'; para 'verificarCierre' queda info contextual
                    disparador,
                    clienteInsertar,
                    autofecha,
                    didCadete || 0,
                    quien || 0
                ], true);
            }

            const result = await executeQuery(connection, updateQuery, [didOwner, didEnvio], true);
            if (result.affectedRows === 0) {
                console.log(`ℹ️ No se pudo actualizar el estado para el didEnvio ${didEnvio}`);
                continue;
            }

            console.log(`✅ CDC insertado x${ejecutadores.length} (estado) y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdcEstado para didOwner ${didOwner}: `, error);
    }
}

// ==============================
// ASIGNACIONES → CDC
// ==============================
async function EnviarcdAsignacion(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        // traigo estado desde asignaciones (si tu tabla no lo tiene, lo podés resolver por fecha)
        const selectQuery = `
      SELECT didOwner, didEnvio, operador, autofecha, estado
      FROM asignaciones
      WHERE cdc = 0 AND didOwner = ? AND autofecha >= '2025-10-10 00:00:00'
      LIMIT 500
    `;
        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay asignaciones pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // AHORA sí: 8 columnas y 8 placeholders
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
            const { didOwner, didEnvio, operador, autofecha, estado } = row;

            // didCliente solo cuando ejecutar = 'estado'
            const getDidClienteQuery = `
        SELECT didCliente
        FROM envios
        WHERE didEnvio = ? AND didOwner = ? AND elim = 0 AND superado = 0
        LIMIT 1
      `;
            const rowsCliente = await executeQuery(connection, getDidClienteQuery, [didEnvio, didOwner]);
            const didCliente = rowsCliente.length > 0 ? rowsCliente[0].didCliente : null;

            // si tu tabla 'asignaciones' no tiene 'estado', podés setearlo a NULL o resolverlo por fecha
            const valorEstado = (estado !== undefined) ? estado : null;

            for (const ejecutar of ejecutadores) {
                const clienteInsertar = (ejecutar === "estado") ? didCliente : null;

                await executeQuery(connection, insertQuery, [
                    didOwner,           // didOwner
                    didEnvio,           // didPaquete
                    ejecutar,           // 'verificarCierre' o 'estado'
                    operador || 0,      // didChofer
                    autofecha,          // fecha
                    disparador,         // 'asignaciones'
                    clienteInsertar,    // didCliente (solo para ejecutar='estado')
                    valorEstado         // estado (NUM o NULL)
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);

            console.log(`✅ CDC insertado x${ejecutadores.length} (asignaciones) y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}: `, error);
    }
}

EnviarcdcEstado(164);

module.exports = {
    EnviarcdcEstado,
    EnviarcdAsignacion
};
895340