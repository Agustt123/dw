const { getConnection, getConnectionLocal, executeQuery, redisClient } = require("../db");

async function ejecutarQueryParaTodasLasEmpresas(query, values = []) {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            try {
                const conn = await getConnection(didOwner);
                await executeQuery(conn, query, values);
                await conn.end();
                console.log(`✅ Query ejecutada para empresa ${didOwner}`);
            } catch (err) {
                console.error(`❌ Error ejecutando query para empresa ${didOwner}:`, err.message);
            }
        }
    } catch (err) {
        console.error("❌ Error general en ejecutarQueryParaTodasLasEmpresas:", err.message);
    }
}
async function corregirFechasHistorialTodasEmpresas() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]
        const query = `
ALTER TABLE envios_items
ADD COLUMN precio DOUBLE
;

`;




        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;
            if (didOwner == "275" || didOwner == "276" || didOwner == "345") continue;

            // if (didOwner <= 276) continue;

            const conn = await getConnection(didOwner);
            try {
                await executeQuery(conn, query, []);
                await conn.release();
                console.log(`✅ Fechas corregidas para empresa ${didOwner}`);
            } catch (err) {
                await conn.release();
                console.error(`❌ Error corrigiendo fechas para empresa ${didOwner}:`, err.message);
            }
        }
    } catch (err) {
        console.error("❌ Error general en corregirFechasHistorialTodasEmpresas:", err.message);
    }
}
async function actualizarIngresoAutomatico(didOwner) {
    const sellerIds = [
        274473795, 514006956, 78409777, 650441900, 288904545, 277435068,
        256872671, 735692880, 433564012, 404622472, 617634017, 45290997,
        50929990, 148545940, 37284960, 214472081, 84823381, 161946607,
        78793078, 80873335, 1823613961, 1788465836, 97487586, 1940032990,
        204129231, 58578515, 201367212, 58391878, 627497479, 2155601390,
        211148642
    ];

    try {
        const conn = await getConnection(didOwner);

        // Armar placeholders dinámicos
        const placeholders = sellerIds.map(() => '?').join(',');

        const query = `
            UPDATE clientes_cuentas
            SET ingreso_automatico = 0
            WHERE ML_id_vendedor IN (${placeholders} AND superado = 0 AND elim = 0)
        `;

        await executeQuery(conn, query, sellerIds);
        await conn.release();

        console.log(`✅ Actualizado ingreso_automatico = 0 para ${sellerIds.length} sellers en empresa ${didOwner}`);
    } catch (err) {
        console.error(`❌ Error actualizando empresa ${didOwner}:`, err.message);
    }
}



async function main() {
    await corregirFechasHistorialTodasEmpresas();
}

main();


module.exports = {
    ejecutarQueryParaTodasLasEmpresas,
    corregirFechasHistorialTodasEmpresas
};
