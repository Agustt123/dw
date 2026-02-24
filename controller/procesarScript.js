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
UPDATE clientes SET didDeposito = '1' WHERE superado=0 and elim=0;


        `;


        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;



            if (didOwner === 275 || didOwner === 276 || didOwner === 345 || didOwner === 223) continue;

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
async function insertarDepositoCentralSiFalta_TodasEmpresas() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);

        const queryCountDepositos = `
      SELECT COUNT(*) AS c
      FROM depositos
      WHERE superado = 0 AND elim = 0;
    `;

        const queryInsertCentral = `
      INSERT INTO depositos
        (id, did, ddiCliente, cod, nombre, direccion, calle, numero, localidad, provincia, pais,
         latitud, longitud, email, propio, autofecha, quien, superado, elim)
      VALUES
        (NULL, '1', '0', 'cen', 'Central', '', '', '', '', '', '',
         '', '', '', '0', CURRENT_TIMESTAMP, '', '0', '0');
    `;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;

            // exclusions
            if (didOwner === 275 || didOwner === 276 || didOwner === 345) continue;


            const conn = await getConnection(didOwner);

            try {
                const rows = await executeQuery(conn, queryCountDepositos, []);
                const count = Number(rows?.[0]?.c ?? 0);

                if (count === 0) {
                    await executeQuery(conn, queryInsertCentral, []);
                    console.log(`✅ Empresa ${didOwner}: depositos vacía -> insertado 'Central'.`);
                } else {
                    console.log(`ℹ️ Empresa ${didOwner}: depositos tiene ${count} registros -> no se inserta.`);
                }

                await conn.release();
            } catch (err) {
                await conn.release();
                console.error(`❌ Error en empresa ${didOwner}:`, err.message);
            }
        }
    } catch (err) {
        console.error("❌ Error general:", err.message);
    }
}


async function listarEmpresasConCostoChofer() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]

        // Consulta para ver si existe al menos 1 registro en cada tabla
        const query = `
            SELECT
                EXISTS(SELECT 1 FROM lista_costochofer              LIMIT 1) AS has_lista_costochofer,
                EXISTS(SELECT 1 FROM lista_costochofer_servicios   LIMIT 1) AS has_lista_costochofer_servicios,
                EXISTS(SELECT 1 FROM lista_costochofer_zonas       LIMIT 1) AS has_lista_costochofer_zonas
        `;

        const empresasConDatos = [];

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            // Tus exclusiones
            if (didOwner == "275" || didOwner == "276" || didOwner == "345" || didOwner == "82" || didOwner == "204" || didOwner == "223" || didOwner == "244" || didOwner == "253") continue;
            // if (didOwner <= 276) continue;

            const conn = await getConnection(didOwner);

            try {
                const rows = await executeQuery(conn, query, []);
                await conn.release();

                const result = rows[0];
                const tieneAlgo =
                    result.has_lista_costochofer === 1 ||
                    result.has_lista_costochofer_servicios === 1 ||
                    result.has_lista_costochofer_zonas === 1;

                if (tieneAlgo) {
                    empresasConDatos.push({
                        didOwner,
                        lista_costochofer: !!result.has_lista_costochofer,
                        lista_costochofer_servicios: !!result.has_lista_costochofer_servicios,
                        lista_costochofer_zonas: !!result.has_lista_costochofer_zonas,
                    });
                    console.log(`✅ Empresa ${didOwner} tiene datos en alguna de las tablas de costo chofer`);
                } else {
                    console.log(`➡️ Empresa ${didOwner} SIN datos en tablas de costo chofer`);
                }

            } catch (err) {
                await conn.release();
                console.error(`❌ Error consultando empresa ${didOwner}:`, err.message);
            }
        }

        console.log("📋 Lista de empresas con datos de costo chofer:");
        console.log(empresasConDatos.map(e => e.didOwner));

        // Por si querés usar la info desde otro lado
        return empresasConDatos;

    } catch (err) {
        console.error("❌ Error general en listarEmpresasConCostoChofer:", err.message);
    }
}



async function main() {
    await insertarDepositoCentralSiFalta_TodasEmpresas();
}

main();


module.exports = {
    ejecutarQueryParaTodasLasEmpresas,
    corregirFechasHistorialTodasEmpresas
};
