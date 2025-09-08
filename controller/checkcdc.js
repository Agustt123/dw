const { getConnectionLocal, executeQuery } = require("../db.js");

// Procesar CDC desde la tabla 'envios'
async function EnviarcdcEstado(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
            SELECT didOwner, didEnvio, estado
            FROM estado 
            WHERE cdc = 0 AND didOwner = ? AND didEnvio > 800000  
            LIMIT 50
        `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay estados pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // Agregamos didCliente en el insert
        const insertQuery = `
            INSERT IGNORE INTO cdc (didOwner, didPaquete, ejecutar, estado, disparador, didCliente)
            VALUES (?, ?, ?, ?, ?, ?)
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

        for (const row of rows) {
            const { didOwner, didEnvio, estado } = row;

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
                // Si es pendientesHoy, agregamos didCliente, sino null
                const clienteInsertar = (ejecutar === "pendientesHoy") ? didCliente : null;

                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    estado,
                    disparador,
                    clienteInsertar
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);

            console.log(`✅ CDC insertado x${ejecutador.length} y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdcEstado para didOwner ${didOwner}: `, error);
    }
}



// Procesar CDC desde la tabla 'asignaciones'
// Procesar CDC desde la tabla 'asignaciones'
async function EnviarcdAsignacion(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
            SELECT didOwner, didEnvio, operador, autofecha
            FROM asignaciones
            WHERE cdc = 0 AND didOwner = ? and autofecha >= '2025-08-22 00:00:00'
            LIMIT 500
        `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay asignaciones pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        // Ahora el insert incluye didCliente
        const insertQuery = `
            INSERT IGNORE INTO cdc(didOwner, didPaquete, ejecutar, didChofer, autofecha, disparador, didCliente)
            VALUES(?, ?, ?, ?, ?, ?, ?)
        `;

        const updateQuery = `
            UPDATE asignaciones SET cdc = 1 WHERE didOwner = ? AND didEnvio = ?
        `;

        const disparadores = [
            "asignacionesHoy",
            "pendientesHoy",
            "cerradosHoy",
            "enCaminosHoy",
            "entregadosHoy",
            "entregasHoy"
        ];

        const canal = "asignaciones";

        for (const row of rows) {
            const { didOwner, didEnvio, operador, autofecha } = row;

            // Obtener didCliente desde envios (igual que en EnviarcdcEstado)
            const getDidClienteQuery = `
                SELECT didCliente
                FROM envios
                WHERE didEnvio = ? AND didOwner = ? AND elim = 0 AND superado = 0
                LIMIT 1
            `;
            const rowsCliente = await executeQuery(connection, getDidClienteQuery, [didEnvio, didOwner]);
            const didCliente = rowsCliente.length > 0 ? rowsCliente[0].didCliente : null;

            for (const disparador of disparadores) {
                const clienteInsertar = (disparador === "pendientesHoy") ? didCliente : null;

                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    disparador,
                    operador,
                    autofecha,
                    canal,
                    clienteInsertar
                ]);
            }

            await executeQuery(connection, updateQuery, [didOwner, didEnvio]);

            console.log(`✅ CDC insertado x${disparadores.length} y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}: `, error);
    }
}


(async () => {
    //    await EnviarcdcEstado(164);
    await EnviarcdAsignacion(164);
    await EnviarcdAsignacion(154);
})();

module.exports = {
    EnviarcdcEstado,
    EnviarcdAsignacion
};
