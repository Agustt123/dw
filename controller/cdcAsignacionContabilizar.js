const { getConnectionLocal, executeQuery } = require("../db");

async function procesarCDC() {
    try {
        const connection = await getConnectionLocal(164);

        const rows = await executeQuery(connection, `
            SELECT id, didOwner, didPaquete, disparador, data, fecha, estado, ejecutar
            FROM cdc
            WHERE procesado = 0 AND disparador = "asignaciones" AND ejecutar = "entregasHoy"
            LIMIT 20
        `);

        for (const row of rows) {
            const { id, didPaquete, ejecutar, didOwner } = row;

            console.log(`📦 CDC ${id} (${ejecutar}) → didPaquete ${didPaquete}`);

            const envios = await executeQuery(connection, `
                SELECT id, choferAsignado,didOwner
                FROM envios
                WHERE didEnvio = ? AND superado = 0 AND elim = 0
                LIMIT 1
            `, [didPaquete, didOwner]);

            if (envios.length === 0) {
                await executeQuery(connection, `
                    UPDATE cdc
                    SET procesado = 1, fProcesado = NOW()
                    WHERE id = ?
                `, [id]);
                console.log(`❌ CDC ${id}: no se encontró envío activo.`);
                continue;
            }

            const envio = envios[0];

            if (envio.choferAsignado !== 1) {
                await executeQuery(connection, `
                    UPDATE envios
                    SET facturacion = 1
                    WHERE didEnvio = ? AND didOwner = ? AND superado = 0 AND elim = 0
                `, [didPaquete, didOwner]);

                console.log(`✅ CDC ${id}: asignado → facturacion = 1`);
            } else {
                console.log(`🟡 CDC ${id}: choferAsignado = 1, no se factura.`);
            }

            await executeQuery(connection, `
                UPDATE cdc
                SET procesado = 1, fProcesado = NOW()
                WHERE id = ?
            `, [id]);
        }

        console.log("✅ Procesamiento CDC de asignaciones finalizado.");
    } catch (error) {
        console.error("❌ Error al procesar CDC:", error);
    }
}


(async () => {
    await procesarCDC();
})();

module.exports = {
    procesarCDC,
};
