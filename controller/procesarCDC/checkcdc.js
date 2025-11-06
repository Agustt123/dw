const { getConnectionLocal, executeQuery } = require("../../db.js");

// Procesar CDC desde la tabla 'envios'
async function EnviarcdcEstado(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
            SELECT didOwner, didEnvio, estado,autofecha,quien
            FROM estado 
            WHERE cdc = 0 AND didOwner = ? and autofecha >= '2025-10-10 00:00:00'
            LIMIT 50
        `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay estados pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // Agregamos didCliente en el insert
        const insertQuery = `
            INSERT  INTO cdc (didOwner, didPaquete, ejecutar, estado, disparador, didCliente,fecha)
            VALUES (?, ?, ?, ?, ?, ?,?)
        `;

        const updateQuery = `
            UPDATE estado SET cdc = 1 WHERE didOwner = ? AND didEnvio = ?
        `;

        const ejecutador = [
            "verificarCierre",
            "pendientesHoy",
            "cerradosHoy",
            "enCaminosHoy",
            "entregadosHoy",
            "entregasHoy"
        ];

        const disparador = "estado";
        console.log("entramosss1");


        for (const row of rows) {
            const { didOwner, didEnvio, estado } = row;
            console.log("entramosss2");
            // Obtenemos didCliente para este didEnvio
            const getDidClienteQuery = `
                SELECT didCliente 
                FROM envios 
                WHERE didEnvio = ? AND didOwner = ? AND elim = 0 AND superado = 0 
                LIMIT 1
            `;
            const rowsCliente = await executeQuery(connection, getDidClienteQuery, [didEnvio, didOwner]);
            const didCliente = rowsCliente.length > 0 ? rowsCliente[0].didCliente : null;

            for (const ejecutar of ejecutador) {
                console.log("entramosss3");
                // Si es pendientesHoy, agregamos didCliente, sino null
                const clienteInsertar = (ejecutar === "pendientesHoy") ? didCliente : null;

                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    estado,
                    disparador,
                    clienteInsertar,
                    row.autofecha
                ], true);
            }

            const result = await executeQuery(connection, updateQuery, [didOwner, didEnvio]);
            console.log("entramosss4");

            if (result.affectedRows === 0) {
                console.log(`ℹ️ No se pudo actualizar el estado para el didEnvio ${didEnvio}`);
                continue;
            }

            console.log(`✅ CDC insertado x${ejecutador.length} y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdcEstado para didOwner ${didOwner}: `, error);
    }
}



// Procesar CDC desde la tabla 'asignaciones'
// Procesar CDC desde la tabla 'asignaciones'
// Procesar CDC desde la tabla 'asignaciones'
async function EnviarcdAsignacion(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
      SELECT didOwner, didEnvio, operador, autofecha
      FROM asignaciones
      WHERE cdc = 0 AND didOwner = ? AND autofecha >= '2025-08-22 00:00:00'
      LIMIT 500
    `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay asignaciones pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // Orden de columnas -> ¡importante mantenerlo!
        const insertQuery = `
      INSERT  INTO cdc (didOwner, didPaquete, ejecutar, didChofer, fecha, disparador, didCliente)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;

        const updateQuery = `
      UPDATE asignaciones SET cdc = 1
      WHERE didOwner = ? AND didEnvio = ?
    `;

        const ejecutadores = [
            "asignacionesHoy",
            "pendientesHoy",
            "cerradosHoy",
            "enCaminosHoy",
            "entregadosHoy",
            "entregasHoy"
        ];

        const disparador = "asignaciones"; // este es el canal/disparador correcto

        for (const row of rows) {
            const { didOwner, didEnvio, operador, autofecha } = row;

            // Traer didCliente (opcionalmente lo usás solo para pendientesHoy)
            const getDidClienteQuery = `
        SELECT didCliente
        FROM envios
        WHERE didEnvio = ? AND didOwner = ? AND elim = 0 AND superado = 0
        LIMIT 1
      `;
            const rowsCliente = await executeQuery(connection, getDidClienteQuery, [didEnvio, didOwner]);
            const didCliente = rowsCliente.length > 0 ? rowsCliente[0].didCliente : null;

            for (const ejecutar of ejecutadores) {
                const clienteInsertar = (ejecutar === "estado") ? didCliente : null;

                // Mantener el MISMO orden que el INSERT:
                // (didOwner, didPaquete, ejecutar, didChofer, fecha, disparador, didCliente)
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    operador,
                    autofecha,       // <-- antes estabas pasando "canal" acá
                    disparador,      // <-- acá va 'asignaciones'
                    clienteInsertar  // <-- y acá el didCliente (o null)
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);

            console.log(`✅ CDC insertado x${ejecutadores.length} y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}: `, error);
    }
}



module.exports = {
    EnviarcdcEstado,
    EnviarcdAsignacion
};
