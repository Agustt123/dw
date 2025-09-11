const { getConnectionLocal, executeQuery } = require("../../db");

async function procesarCDC() {
    try {
        const connection = await getConnectionLocal(164);

        const rows = await executeQuery(connection, `
            SELECT id, didOwner, didPaquete, disparador, data, fecha, estado
            FROM cdc
            WHERE procesado = 0 AND disparador = "estado" AND ejecutar = "entregasHoy"
            LIMIT 20
        `);

        for (const row of rows) {
            const { id, didPaquete, autofecha, didOwner } = row;

            console.log(`Procesando CDC ${id}: didPaquete ${didPaquete}, autofecha ${autofecha}`);

            // Verificar si existe envío activo
            const envios = await executeQuery(connection, `
                SELECT id, cantidadEstado
                FROM envios
                WHERE didEnvio = ? AND didOwner = ? AND superado = 0 AND elim = 0
                LIMIT 1
            `, [didPaquete, didOwner]);

            if (envios.length === 0) {
                await executeQuery(connection, `
                    UPDATE cdc
                    SET procesado = 1, fProcesado = NOW()
                    WHERE id = ?
                `, [id]);

                console.log(`CDC ${id}: no se encontró envío activo para didPaquete ${didPaquete}.`);
                continue;
            }

            const envio = envios[0];
            const nuevaCantidadEstado = (envio.cantidadEstado || 0) + 1;

            // Armar campos a actualizar
            let queryUpdateEnvio = `
                UPDATE envios
                SET cantidadEstado = ?
            `;
            const valoresUpdateEnvio = [nuevaCantidadEstado];

            if (nuevaCantidadEstado > 1) {
                queryUpdateEnvio += `, facturacion = 1`;
            }

            queryUpdateEnvio += `
                WHERE didEnvio = ? AND superado = 0 AND elim = 0
            `;
            valoresUpdateEnvio.push(didPaquete);

            // Ejecutar update del envío
            await executeQuery(connection, queryUpdateEnvio, valoresUpdateEnvio);

            // Marcar CDC como procesado
            await executeQuery(connection, `
                UPDATE cdc
                SET procesado = 1, fProcesado = NOW()
                WHERE id = ?
            `, [id]);

            console.log(`CDC ${id}: actualizado envío ${didPaquete} (cantidadEstado = ${nuevaCantidadEstado}${nuevaCantidadEstado > 1 ? ', facturacion = 1' : ''}).`);
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
};
