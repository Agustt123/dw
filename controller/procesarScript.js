const { getConnection, getConnectionLocal, executeQuery, redisClient, getConnectionIndividual, getConnectionSistema } = require("../db");

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

        const query = `ALTER TABLE liquidaciones ADD dataLiquidaciones TEXT NOT NULL AFTER idlineas;
`;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;



            if (didOwner === 275 || didOwner === 276 || didOwner === 345) continue;

            const conn = await getConnection(didOwner);
            try {
                await executeQuery(conn, query, [didOwner], true);


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
async function corregirFechasHistorialEmpresaPrueba() {
    let conn;

    try {
        const didOwner = 375;
        conn = await getConnection(didOwner);

        const query = `
            SELECT
                id,
                nombre,
                codigo,
                url,
                email_interno,
                maneja_mapa_gmaps,
                maneja_mapa_heremaps,
                ml_cliente_id,
                emails_externos,
                ml_secret_key,
                ml_url,
                tiendanube_id,
                tiendanube_appkey,
                sys_cantBloqueo,
                email_pass,
                captcha_privada,
                captcha_publica,
                heremaps_key,
                gmaps_key,
                woocommerce,
                tiene_ml,
                tiene_tiendanube,
                shopify,
                heremaps_id,
                plan,
                pais,
                manejaCP,
                fullfilment,
                metodoEnvio_shopify,
                metodoEnvio_tn,
                choferCosto,
                me1,
                manejoMultidepositos
            FROM \`lightdat_sistema\`.\`lightdata_clientes\`
            WHERE id = ?
        `;

        const values = [375];

        const result = await executeQuery(conn, query, values, true);
        console.log("✅ Resultado empresa 375:", result);

    } catch (err) {
        console.error("❌ Error en empresa 375:", err.message);
    } finally {
        if (conn) await conn.release();
    }
}

async function sistemaQuery() {
    const conn = await getConnectionSistema();

    try {


        const [rows] = await conn.query("SELECT did,manejoMultidepositos FROM lightdata_clientes");
        conn.release();

        console.log(rows);





        console.log("📋 Lista de empresas con datos de costo chofer:");


        // Por si querés usar la info desde otro lado
        return true;

    } catch (err) {
        console.error("❌ Error general en listarEmpresasConCostoChofer:", err.message);
    } finally {
        if (conn) await conn.release();
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

        const inicioDia = "2026-01-01 00:00:00";
        const finDia = "2026-03-13 00:00:00";

        const countQuery = `
            SELECT COUNT(*) AS cantidad
            FROM envios
            WHERE fecha_inicio >= ?
        
              AND elim = 0
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

                resultados.push({
                    didOwner,
                    nombreEmpresa: empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`,
                    cantidad
                });

                totalGlobal += cantidad;
            } catch (err) {
                console.error(`❌ Error contando envíos para empresa ${didOwner}:`, err.message);
            } finally {
                await conn.release();
            }
        }

        resultados.sort((a, b) => b.cantidad - a.cantidad);

        console.log("====================================");
        console.log("📦 Cantidad de envíos por empresa:");
        console.log("====================================");

        resultados.forEach((r, index) => {
            console.log(
                `${index + 1}. ${r.nombreEmpresa} (ID: ${r.didOwner}) => ${r.cantidad} envíos`
            );
        });

        console.log("====================================");
        console.log(`✅ Total global de envíos: ${totalGlobal}`);
        console.log(`✅ Total de empresas procesadas: ${resultados.length}`);
    } catch (err) {
        console.error("❌ Error general en contarEnviosTodasEmpresas:", err.message);
    }
}
async function contarPesoTablasTodasEmpresas() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);

        const exclusions = new Set([275, 276, 345]);

        let totalGlobalBytes = 0;

        let totalEnviosBytes = 0;
        let totalHistorialBytes = 0;
        let totalAsignacionesBytes = 0;

        const resultados = [];

        const sizeQuery = `
            SELECT
                table_name,
                COALESCE(data_length, 0) + COALESCE(index_length, 0) AS total_bytes
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name IN ('envios', 'envios_historial', 'envios_asignaciones')
        `;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;
            if (exclusions.has(didOwner)) continue;
            console.log(empresaData[didOwner].dbname);

            const conn = await getConnection(didOwner);


            try {
                const rows = await executeQuery(conn, sizeQuery, [empresaData[didOwner].dbname]);

                let enviosBytes = 0;
                let historialBytes = 0;
                let asignacionesBytes = 0;

                for (const row of rows) {
                    const tableName = row.TABLE_NAME || row.table_name;
                    const bytes = Number(row.total_bytes || 0);

                    if (tableName === "envios") {
                        enviosBytes = bytes;
                    } else if (tableName === "envios_historial") {
                        historialBytes = bytes;
                    } else if (tableName === "envios_asignaciones") {
                        asignacionesBytes = bytes;
                    }
                }

                const totalEmpresaBytes = enviosBytes + historialBytes + asignacionesBytes;

                resultados.push({
                    didOwner,
                    nombreEmpresa: empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`,
                    enviosBytes,
                    historialBytes,
                    asignacionesBytes,
                    totalEmpresaBytes
                });

                totalEnviosBytes += enviosBytes;
                totalHistorialBytes += historialBytes;
                totalAsignacionesBytes += asignacionesBytes;
                totalGlobalBytes += totalEmpresaBytes;

            } catch (err) {
                console.error(`❌ Error calculando tamaño para empresa ${didOwner}:`, err.message);
            } finally {
                await conn.release();
            }
        }

        resultados.sort((a, b) => b.totalEmpresaBytes - a.totalEmpresaBytes);

        const toMB = (bytes) => (bytes / 1024 / 1024).toFixed(2);
        const toGB = (bytes) => (bytes / 1024 / 1024 / 1024).toFixed(2);

        console.log("====================================");
        console.log("📦 Peso de tablas por empresa:");
        console.log("====================================");

        resultados.forEach((r, index) => {
            console.log(
                `${index + 1}. ${r.nombreEmpresa} (ID: ${r.didOwner})` +
                ` | envios: ${toGB(r.enviosBytes)} GB` +
                ` | historial: ${toGB(r.historialBytes)} GB` +
                ` | asignaciones: ${toGB(r.asignacionesBytes)} GB` +
                ` | TOTAL: ${toGB(r.totalEmpresaBytes)} GB`
            );
        });

        console.log("====================================");
        console.log(`✅ Total envios: ${toGB(totalEnviosBytes)} GB (${toMB(totalEnviosBytes)} MB)`);
        console.log(`✅ Total envios_historial: ${toGB(totalHistorialBytes)} GB (${toMB(totalHistorialBytes)} MB)`);
        console.log(`✅ Total envios_asignaciones: ${toGB(totalAsignacionesBytes)} GB (${toMB(totalAsignacionesBytes)} MB)`);
        console.log(`✅ TOTAL GLOBAL: ${toGB(totalGlobalBytes)} GB (${toMB(totalGlobalBytes)} MB)`);
        console.log(`✅ Total de empresas procesadas: ${resultados.length}`);

    } catch (err) {
        console.error("❌ Error general en contarPesoTablasTodasEmpresas:", err.message);
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
