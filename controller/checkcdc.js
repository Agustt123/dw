const { getConnectionLocal, executeQuery } = require("../db.js");

// Procesar CDC desde la tabla 'envios'
async function EnviarcdcEstado(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner);

        const selectQuery = `
            SELECT didOwner, didEnvio, estado
            FROM estado
            WHERE cdc = 0 AND didOwner = ? and estado in (5,9,14)
            LIMIT 5
        `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay estados pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        const insertQuery = `
            INSERT INTO cdc (didOwner, didPaquete, ejecutar, estado, disparador)
            VALUES (?, ?, ?, ?, ?)
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

            for (const ejecutar of ejecutador) {
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    ejecutar,
                    estado,
                    disparador
                ]);
            }

            await executeQuery(connection, updateQuery, [
                didOwner,
                didEnvio
            ]);

            console.log(`✅ CDC insertado x5 y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdcEstado para didOwner ${didOwner}:`, error);
    }
}


// Procesar CDC desde la tabla 'asignaciones'
async function EnviarcdAsignacion(didOwner) {
    try {
        const connection = await getConnectionLocal(didOwner); // Te recomiendo que uses el didOwner directamente aquí

        const selectQuery = `
            SELECT didOwner, didEnvio, operador, autofecha
            FROM asignaciones
            WHERE cdc = 0 AND didOwner = ?
            LIMIT 5
        `;

        const rows = await executeQuery(connection, selectQuery, [didOwner]);

        if (rows.length === 0) {
            console.log(`ℹ️ No hay asignaciones pendientes de CDC para el didOwner ${didOwner}`);
            return;
        }

        const insertQuery = `
            INSERT INTO cdc (didOwner, didPaquete, ejecutar, didChofer, autofecha, disparador)
            VALUES (?, ?, ?, ?, ?, ?)
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

            for (const disparador of disparadores) {
                await executeQuery(connection, insertQuery, [
                    didOwner,
                    didEnvio,
                    disparador,
                    operador,
                    autofecha,
                    canal
                ]);
            }

            await executeQuery(connection, updateQuery, [
                didOwner,
                didEnvio
            ]);

            console.log(`✅ CDC insertado x5 y actualizado → didOwner: ${didOwner}, didPaquete: ${didEnvio}`);
        }
    } catch (error) {
        console.error(`❌ Error en EnviarcdAsignacion para didOwner ${didOwner}:`, error);
    }
}


(async () => {
    await EnviarcdAsignacion(164);
})();

module.exports = {
    // EnviarcdcEstado,
    //EnviarcdAgisnacion
};
