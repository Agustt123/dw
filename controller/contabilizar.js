const { getConnectionLocal, executeQuery } = require("../db");
const cron = require("node-cron");

// Utilidad para obtener la fecha de "ayer" en horario de Argentina
function obtenerFechaAyerArgentina() {
    const ahora = new Date().toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" });
    const fechaArgentina = new Date(ahora);
    fechaArgentina.setDate(fechaArgentina.getDate() - 1);

    const yyyy = fechaArgentina.getFullYear();
    const mm = String(fechaArgentina.getMonth() + 1).padStart(2, '0');
    const dd = String(fechaArgentina.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
}

async function procesarFacturacionPorFecha(didOwner, fecha) {
    try {
        const connection = await getConnectionLocal(didOwner);
        console.log(`🔍 Procesando facturación para didOwner ${didOwner} en fecha ${fecha}`);

        // 1) Estados por didEnvio
        const estadosPorEnvio = await executeQuery(connection, `
            SELECT didEnvio, COUNT(*) AS cantidadEstados
            FROM estado
            WHERE didOwner = ? AND DATE(autofecha) = ?
            GROUP BY didEnvio
        `, [didOwner, fecha], true);

        for (const { didEnvio, cantidadEstados } of estadosPorEnvio) {
            if (cantidadEstados > 1) {
                await executeQuery(connection, `
                    UPDATE envios
                    SET facturacion = 1, cantidadEstado = ?
                    WHERE didOwner = ? AND didEnvio = ? AND superado = 0 AND elim = 0
                `, [cantidadEstados, didOwner, didEnvio]);
                console.log(`✅ didEnvio ${didEnvio}: facturacion=1 por estados (${cantidadEstados} cambios)`);
            }
        }

        // 2) Asignaciones
        /*     const asignacionesPorEnvio = await executeQuery(connection, `
                 SELECT DISTINCT didEnvio
                 FROM asignaciones
                 WHERE didOwner = ? AND DATE(autofecha) = ?
             `, [didOwner, fecha], true);
     
             for (const { didEnvio } of asignacionesPorEnvio) {
                 await executeQuery(connection, `
                     UPDATE envios
                     SET facturacion = 1
                     WHERE didOwner = ? AND didEnvio = ? AND superado = 0 AND elim = 0
                 `, [didOwner, didEnvio]);
                 console.log(`✅ didEnvio ${didEnvio}: facturacion=1 por asignaciones`);
             }
                 */
        console.log("✅ Procesamiento de facturación finalizado.")


    } catch (error) {
        console.error("❌ Error en procesarFacturacionPorFecha:", error);
    }
}

// Mostrar hora actual al iniciar el script
const ahora = new Date()
console.log(`🚀 Script iniciado. Hora actual del sistema: ${ahora}`);

// 🔄 Ejecutar una vez al iniciar (para testing o debug manual)
(async () => {
    const fechaAyer = obtenerFechaAyerArgentina();
    await procesarFacturacionPorFecha(164, fechaAyer);
})();

// ⏰ Programar para las 2:00 AM de Argentina cada día
cron.schedule("0 2 * * *", async () => {
    const fechaAyer = obtenerFechaAyerArgentina();
    const horaEjecucion = new Date().toLocaleTimeString("es-AR", { timeZone: "America/Argentina/Buenos_Aires" });
    console.log(`🕑 [${horaEjecucion}] Ejecutando cron para fecha ${fechaAyer}`);
    await procesarFacturacionPorFecha(164, fechaAyer);
});
