const { getConnectionLocal, executeQuery } = require("../db");



async function procesarCDC() {
    try {
        const connection = await getConnectionLocal(164);

        const rows = await executeQuery(connection, `
            SELECT id, didOwner, didPaquete, disparador, data, fecha, estado
            FROM cdc
            WHERE procesado = 0 AND disparador = "estado" AND ejecutar = "verificarCierre"
            LIMIT 20
        `);

        for (const row of rows) {
            const { id, didPaquete, autofecha, estado } = row;
            const estadoNum = Number(estado);

            // Validamos que el estado sea de cierre
            if ([5, 9, 13].includes(estadoNum)) {
                await executeQuery(connection, `
                    UPDATE cdc
                    SET procesado = 1,  fProcesado = NOW()
                    WHERE id = ?
                `, [id]);
                console.log(`CDC ${id}: estado ${estado} no es de cierre. Marcado como procesado sin acción.`);
                continue;
            }

            // Verificar si existe envío activo
            const envios = await executeQuery(connection, `
                SELECT id FROM envios
                WHERE didEnvio = ? AND superado = 0 AND elim = 0
                LIMIT 1
            `, [didPaquete]);

            if (envios.length === 0) {
                await executeQuery(connection, `
                    UPDATE cdc
                    SET procesado = 1,  fProcesado = NOW()
                    WHERE id = ?
                `, [id]);
                console.log(`CDC ${id}: no se encontró envío activo para didPaquete ${didPaquete}.`);
                continue;
            }

            // 1. Actualizar el envío (fechaCierre)
            await executeQuery(connection, `
                UPDATE envios
                SET fecha_cierre = ?
                WHERE didEnvio = ? AND superado = 0 AND elim = 0
            `, [autofecha, didPaquete]);

            // 2. Marcar CDC como correctamente procesado
            await executeQuery(connection, `
                UPDATE cdc
                SET procesado = 1, fProcesado = NOW()
                WHERE id = ?
            `, [id]);

            console.log(`CDC ${id}: fechaCierre actualizada para didPaquete ${didPaquete}.`);
        }

        console.log("✅ Procesamiento CDC finalizado.");

    } catch (error) {
        console.error("❌ Error al procesar CDC:", error);
    }
}
(async () => {
    await procesarCDC();
})();

module.exports = {
    procesarCDC,
    //EnviarcdAgisnacion
};
