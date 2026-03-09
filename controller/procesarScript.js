const { getConnection, getConnectionLocal, executeQuery, redisClient, getConnectionIndividual } = require("../db");

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
ALTER
    ALGORITHM = UNDEFINED
    DEFINER = \`lightdat_uinsta\`@\`localhost\`
    SQL SECURITY DEFINER
VIEW \`lightdata_clientes\` AS
SELECT
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`id\` AS \`id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`nombre\` AS \`nombre\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`codigo\` AS \`codigo\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`url\` AS \`url\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`email_interno\` AS \`email_interno\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`maneja_mapa_gmaps\` AS \`maneja_mapa_gmaps\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`maneja_mapa_heremaps\` AS \`maneja_mapa_heremaps\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_cliente_id\` AS \`ml_cliente_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`emails_externos\` AS \`emails_externos\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_secret_key\` AS \`ml_secret_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_url\` AS \`ml_url\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiendanube_id\` AS \`tiendanube_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiendanube_appkey\` AS \`tiendanube_appkey\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`sys_cantBloqueo\` AS \`sys_cantBloqueo\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`email_pass\` AS \`email_pass\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`captcha_privada\` AS \`captcha_privada\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`captcha_publica\` AS \`captcha_publica\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`heremaps_key\` AS \`heremaps_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`gmaps_key\` AS \`gmaps_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`woocommerce\` AS \`woocommerce\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiene_ml\` AS \`tiene_ml\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiene_tiendanube\` AS \`tiene_tiendanube\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`shopify\` AS \`shopify\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`heremaps_id\` AS \`heremaps_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`plan\` AS \`plan\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`pais\` AS \`pais\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`manejaCP\` AS \`manejaCP\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`fullfilment\` AS \`fullfilment\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`metodoEnvio_shopify\` AS \`metodoEnvio_shopify\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`metodoEnvio_tn\` AS \`metodoEnvio_tn\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`choferCosto\` AS \`choferCosto\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`me1\` AS \`me1\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`manejoMultidepositos\` AS \`manejoMultidepositos\`
FROM
    \`lightdat_sistema\`.\`lightdata_clientes\`
WHERE
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`id\` = 375;
`;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;



            if (didOwner === 275 || didOwner === 276 || didOwner === 345) continue;

            const conn = await getConnection(375);
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
async function contarEnviosTodasEmpresas() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);

        const inicioDia = "2026-02-25 00:00:00";
        const finDia = "2026-02-26 00:00:00";

        const countQuery = `
  SELECT COUNT(*) AS cantidad
  FROM envios
  WHERE fecha_inicio >= ?
    AND fecha_inicio < ?
    AND superado = 0
`;

        const exclusions = new Set([275, 276, 345]);

        let totalGlobal = 0;
        const resultados = [];

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;
            if (exclusions.has(didOwner)) continue;

            const conn = await getConnection(didOwner);

            try {
                const rows = await executeQuery(conn, countQuery, [inicioDia, finDia]);
                const cantidad = Number(rows?.[0]?.cantidad ?? 0);
                resultados.push({ didOwner, cantidad });
                totalGlobal += cantidad;

                console.log(`🏢 Empresa ${didOwner}: ${cantidad} envíos`);
            } catch (err) {
                console.error(`❌ Error contando envíos para empresa ${didOwner}:`, err.message);
            } finally {
                await conn.release();
            }
        }

        console.log("====================================");
        console.log(`✅ Total global envíos: ${totalGlobal}`);
        console.log("✅ Top (primeros 20 por cantidad):");

        resultados
            .sort((a, b) => b.cantidad - a.cantidad)
            .slice(0, 20)
            .forEach(r => console.log(`- ${r.didOwner}: ${r.cantidad}`));
    } catch (err) {
        console.error("❌ Error general en contarEnviosTodasEmpresas:", err.message);
    }
}

async function ejecutarQueryTodasEmpresasIndividual() {
    let empresasData;

    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        empresasData = JSON.parse(empresaDataStr);

        const didOwners = Object.keys(empresasData); // Ej: ["2", "3", "4"]

        const query = `
ALTER
    ALGORITHM = UNDEFINED
    DEFINER = \`lightdat_uinsta\`@\`localhost\`
    SQL SECURITY DEFINER
VIEW \`lightdata_clientes\` AS
SELECT
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`id\` AS \`id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`nombre\` AS \`nombre\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`codigo\` AS \`codigo\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`url\` AS \`url\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`email_interno\` AS \`email_interno\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`maneja_mapa_gmaps\` AS \`maneja_mapa_gmaps\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`maneja_mapa_heremaps\` AS \`maneja_mapa_heremaps\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_cliente_id\` AS \`ml_cliente_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`emails_externos\` AS \`emails_externos\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_secret_key\` AS \`ml_secret_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`ml_url\` AS \`ml_url\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiendanube_id\` AS \`tiendanube_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiendanube_appkey\` AS \`tiendanube_appkey\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`sys_cantBloqueo\` AS \`sys_cantBloqueo\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`email_pass\` AS \`email_pass\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`captcha_privada\` AS \`captcha_privada\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`captcha_publica\` AS \`captcha_publica\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`heremaps_key\` AS \`heremaps_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`gmaps_key\` AS \`gmaps_key\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`woocommerce\` AS \`woocommerce\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiene_ml\` AS \`tiene_ml\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`tiene_tiendanube\` AS \`tiene_tiendanube\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`shopify\` AS \`shopify\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`heremaps_id\` AS \`heremaps_id\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`plan\` AS \`plan\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`pais\` AS \`pais\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`manejaCP\` AS \`manejaCP\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`fullfilment\` AS \`fullfilment\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`metodoEnvio_shopify\` AS \`metodoEnvio_shopify\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`metodoEnvio_tn\` AS \`metodoEnvio_tn\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`choferCosto\` AS \`choferCosto\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`me1\` AS \`me1\`,
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`manejoMultidepositos\` AS \`manejoMultidepositos\`
FROM
    \`lightdat_sistema\`.\`lightdata_clientes\`
WHERE
    \`lightdat_sistema\`.\`lightdata_clientes\`.\`id\` = 375;
`;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);

            if (isNaN(didOwner)) continue;
            if (didOwner === 275 || didOwner === 276 || didOwner === 345) continue;

            let conn = null;

            try {
                conn = await getConnectionIndividual(375);

                await executeQuery(conn, query, []);

                console.log(`✅ Query ejecutada para empresa ${didOwner}`);
            } catch (err) {
                console.error(
                    `❌ Error ejecutando query para empresa ${didOwner}:`,
                    err?.message || err
                );
            } finally {
                try {
                    if (conn && typeof conn.end === "function") {
                        await conn.end();
                    } else if (conn && typeof conn.destroy === "function") {
                        conn.destroy();
                    }
                } catch (closeErr) {
                    console.error(
                        `⚠️ Error cerrando conexión para empresa ${didOwner}:`,
                        closeErr?.message || closeErr
                    );
                }
            }
        }
    } catch (err) {
        console.error(
            "❌ Error general en ejecutarQueryTodasEmpresasIndividual:",
            err?.message || err
        );
    }
}
async function main() {
    await ejecutarQueryTodasEmpresasIndividual();
}

main();


module.exports = {
    ejecutarQueryParaTodasLasEmpresas,
    corregirFechasHistorialTodasEmpresas
};
