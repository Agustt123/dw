const { getConnectionLocal, executeQuery } = require("../db");

async function procesarFacturacionPorFecha(didOwner, fecha) {
    try {
        const connection = await getConnectionLocal(didOwner);

        console.log(`🔍 Procesando facturación para didOwner ${didOwner} en fecha ${fecha}`);

        // 1) Desde 'estado' contar cantidad de estados por didEnvio para la fecha dada
        const estadosPorEnvio = await executeQuery(connection, `
            SELECT didEnvio, COUNT(*) AS cantidadEstados
            FROM estado
            WHERE didOwner = ? AND DATE(autofecha) = ?
            GROUP BY didEnvio
        `, [didOwner, fecha]);

        // Procesar facturacion para envios con más de 1 estado
        for (const { didEnvio, cantidadEstados } of estadosPorEnvio) {
            if (cantidadEstados > 1) {
                await executeQuery(connection, `
                    UPDATE envios
                    SET facturacion = 1, cantidadEstado = ?
                    WHERE didOwner = ? AND didEnvio = ? AND superado = 0 AND elim = 0
                `, [didOwner, didEnvio, cantidadEstados]);
                console.log(`✅ didEnvio ${didEnvio}: facturacion=1 por estados (${cantidadEstados} cambios)`);
            }
        }

        // 2) Desde 'asignaciones', verificar si hay registros por didEnvio en la misma fecha
        const asignacionesPorEnvio = await executeQuery(connection, `
            SELECT DISTINCT didEnvio
            FROM asignaciones
            WHERE didOwner = ? AND DATE(autofecha) = ?
        `, [didOwner, fecha], true);

        // Actualizar facturacion para esos envios que tienen asignaciones en esa fecha
        for (const { didEnvio } of asignacionesPorEnvio) {
            await executeQuery(connection, `
                UPDATE envios
                SET facturacion = 1
                WHERE didOwner = ? AND didEnvio = ? AND superado = 0 AND elim = 0
            `, [didOwner, didEnvio]);
            console.log(`✅ didEnvio ${didEnvio}: facturacion=1 por asignaciones`);
        }

        console.log("✅ Procesamiento de facturación finalizado.");

    } catch (error) {
        console.error("❌ Error en procesarFacturacionPorFecha:", error);
    }
}

// Ejemplo de uso:
procesarFacturacionPorFecha(164, "2025-06-20");

module.exports = {
    procesarFacturacionPorFecha
};
