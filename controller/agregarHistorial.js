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

                if (rows.length > 0) {
                    console.log(`📌 Empresa ${didOwner} → Encontrado:`);
                    console.log(rows);
                    return rows; // ← Devuelvo el encontrado
                }
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
          INSERT INTO estados_envios (did, estado)
VALUES
(30, 'reprogramado por comprador'),
(31, 'reprogramado por meli');

        `;


        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            // Comparación correcta entre números
            if (didOwner === 275 || didOwner === 276 || didOwner === 345) continue;

            const conn = await getConnection(didOwner);
            try {
                await executeQuery(conn, query, []);
                await conn.release();
                console.log(`✅ Query ejecutada para empresa ${didOwner}`);
            } catch (err) {
                await conn.release();
                console.error(`❌ Error ejecutando query para empresa ${didOwner}:`, err.message);
            }
        }
    } catch (err) {
        console.error("❌ Error general en corregirFechasHistorialTodasEmpresas:", err.message);
    }
}

async function corregirEstadoEnviosHistorialEmpresa(didOwner, dids = []) {
    if (!didOwner || isNaN(didOwner)) {
        console.error("❌ didOwner inválido");
        return;
    }

    if (!Array.isArray(dids) || dids.length === 0) {
        console.error("❌ Debes enviar un array de DID");
        return;
    }

    const placeholders = dids.map(() => "?").join(",");

    const query = `
        INSERT INTO envios_historial (didEnvio, fecha, estado, quien, desde)
        SELECT
            e.did,
            e.autofecha,
            0,
            1,
            'proceso'
        FROM envios e
        WHERE e.did IN (${placeholders});
    `;

    const conn = await getConnection(didOwner);

    try {
        await executeQuery(conn, query, dids);
        console.log(`✅ Historial corregido (estado 7) para empresa ${didOwner}`);
    } catch (err) {
        console.error("❌ Error corrigiendo historial:", err.message);
    } finally {
        await conn.release();
    }
}





async function main() {
    //   await corregirFechasHistorialTodasEmpresas();
    await corregirEstadoEnviosHistorialEmpresa(97, [293550, 293555, 293556, 293560, 293601, 293607, 293620, 293621, 293635, 293636, 293693, 293770, 293778, 293803, 293819, 293837, 293855]); // Ejemplo para empresa con didOwner 2
}

main();


module.exports = {
    ejecutarQueryParaTodasLasEmpresas,
    corregirFechasHistorialTodasEmpresas
};
