const { getConnection, getConnectionLocal, executeQuery, redisClient, getConnectionIndividual, getConnectionSistema } = require("../db");
const EMPRESAS_BLOQUEADAS = new Set([275, 276, 345]);

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
            console.error("No se encontro 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]
        const query = `
            ALTER TABLE envios_historial
            ADD COLUMN redis TINYINT NOT NULL DEFAULT -1
        `;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;
            if (EMPRESAS_BLOQUEADAS.has(didOwner)) continue;

            const conn = await getConnection(didOwner);

            try {
                await executeQuery(conn, query);
                console.log(`Empresa ${didOwner}: columna redis agregada en envios_historial`);
            } catch (err) {
                console.error(`Error ejecutando query para empresa ${didOwner}:`, err.message);
            } finally {
                await conn.release();
            }
        }
    } catch (err) {
        console.error("Error general en corregirFechasHistorialTodasEmpresas:", err.message);
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
        console.log("Iniciando conteo de envios...");
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);
        console.log(`Empresas encontradas: ${didOwners.length}`);

        const fechaInicioDesde = "2026-04-07 00:00:00";

        const countQuery = `
            SELECT COUNT(*) AS cantidad
            FROM envios
            WHERE fecha_inicio > ? and fecha_inicio < '2026-04-07 23:59:59'
      
        `;

        let totalGlobal = 0;
        const resultados = [];
        let procesadas = 0;
        let omitidas = 0;

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;
            if (EMPRESAS_BLOQUEADAS.has(didOwner)) {
                omitidas += 1;
                console.log(`Empresa ${didOwner} omitida por bloqueo`);
                continue;
            }

            console.log(`Procesando empresa ${didOwner}...`);
            const conn = await getConnection(didOwner);

            try {
                const rows = await executeQuery(conn, countQuery, [fechaInicioDesde], { timeoutMs: 120000 });
                const cantidad = Number(rows?.[0]?.cantidad ?? 0);

                resultados.push({
                    didOwner,
                    nombreEmpresa: empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`,
                    cantidad
                });

                totalGlobal += cantidad;
                procesadas += 1;
                console.log(`Empresa ${didOwner} => ${cantidad} envios`);
            } catch (err) {
                console.error(`❌ Error contando envíos para empresa ${didOwner}:`, err.message);
            } finally {
                await conn.release();
            }
        }

        resultados.sort((a, b) => b.cantidad - a.cantidad);

        console.log("====================================");
        console.log("📦 Cantidad de envíos por empresa:");
        console.log(`📅 Condición: fecha_inicio > '${fechaInicioDesde}'`);
        console.log("====================================");

        resultados.forEach((r, index) => {
            console.log(
                `${index + 1}. ${r.nombreEmpresa} (ID: ${r.didOwner}) => ${r.cantidad} envíos`
            );
        });

        console.log("====================================");
        console.log(`✅ Total global de envíos: ${totalGlobal}`);
        console.log(`✅ Total de empresas procesadas: ${resultados.length}`);

        console.log(`Total de empresas omitidas: ${omitidas}`);

        return {
            totalGlobal,
            totalEmpresas: procesadas,
            resultados
        };
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

function formatBytes(bytes) {
    const value = Number(bytes || 0);
    const mb = value / 1024 / 1024;
    const gb = value / 1024 / 1024 / 1024;

    return {
        bytes: value,
        mb: mb.toFixed(2),
        gb: gb.toFixed(2)
    };
}

function daysBetween(startDateStr, endDateStr) {
    const start = new Date(startDateStr);
    const end = new Date(endDateStr);
    const diffMs = end.getTime() - start.getTime();
    return Math.max(1, Math.ceil(diffMs / (1000 * 60 * 60 * 24)));
}

async function resumenCapacidadEnviosHasta2028() {
    try {
        console.log("Iniciando resumen de capacidad de envios...");

        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("No se encontro 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);
        const fechaInicioDesde = "2026-01-01 00:00:00";
        const hoy = "2026-03-31 00:00:00";
        const fechaProyeccion = "2028-12-31 00:00:00";

        const countRowsQuery = `
            SELECT
                COUNT(*) AS totalFiltrado,
                SUM(CASE WHEN elim = 0 THEN 1 ELSE 0 END) AS totalFiltradoNoElim
            FROM envios
            WHERE fecha_inicio > ?
        `;

        const totalRowsQuery = `
            SELECT COUNT(*) AS totalTabla
            FROM envios
        `;

        const tableSizeQuery = `
            SELECT
                table_name,
                COALESCE(data_length, 0) + COALESCE(index_length, 0) AS total_bytes
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name IN ('envios', 'envios_historial', 'envios_asignaciones')
        `;

        let totalEstimadoBytes = 0;
        let totalTablaBytes = 0;
        let totalFiltrado = 0;
        let totalFiltradoNoElim = 0;
        let procesadas = 0;
        let omitidas = 0;
        const resultados = [];
        const diasObservados = daysBetween(fechaInicioDesde, hoy);
        const diasHasta2028 = daysBetween(hoy, fechaProyeccion);

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;

            if (EMPRESAS_BLOQUEADAS.has(didOwner)) {
                omitidas += 1;
                continue;
            }

            let conn;

            try {
                console.log(`Estimando empresa ${didOwner}...`);
                conn = await getConnection(didOwner);

                const [rowsFiltrados, rowsTotales, sizeRows] = await Promise.all([
                    executeQuery(conn, countRowsQuery, [fechaInicioDesde], { timeoutMs: 120000 }),
                    executeQuery(conn, totalRowsQuery, [], { timeoutMs: 120000 }),
                    executeQuery(conn, tableSizeQuery, [empresaData[didOwnerStr]?.dbname], { timeoutMs: 120000 })
                ]);

                const totalTabla = Number(rowsTotales?.[0]?.totalTabla ?? 0);
                const filtrado = Number(rowsFiltrados?.[0]?.totalFiltrado ?? 0);
                const filtradoNoElim = Number(rowsFiltrados?.[0]?.totalFiltradoNoElim ?? 0);
                let totalBytesEnvios = 0;
                let totalBytesHistorial = 0;
                let totalBytesAsignaciones = 0;

                for (const row of sizeRows || []) {
                    const tableName = row.TABLE_NAME || row.table_name;
                    const bytes = Number(row.total_bytes ?? 0);

                    if (tableName === "envios") totalBytesEnvios = bytes;
                    if (tableName === "envios_historial") totalBytesHistorial = bytes;
                    if (tableName === "envios_asignaciones") totalBytesAsignaciones = bytes;
                }

                const totalBytesPaquete = totalBytesEnvios + totalBytesHistorial + totalBytesAsignaciones;
                const proporcion = totalTabla > 0 ? (filtradoNoElim / totalTabla) : 0;
                const estimadoBytesEnvios = totalBytesEnvios * proporcion;
                const factorPaquete = totalBytesEnvios > 0 ? (totalBytesPaquete / totalBytesEnvios) : 1;
                const estimadoBytesPaquete = estimadoBytesEnvios * factorPaquete;
                const crecimientoDiarioBytes = estimadoBytesPaquete / diasObservados;
                const proyeccionAdicionalBytes = crecimientoDiarioBytes * diasHasta2028;
                const proyeccionTotal2028Bytes = totalBytesPaquete + proyeccionAdicionalBytes;
                const nombreEmpresa = empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`;

                resultados.push({
                    didOwner,
                    nombreEmpresa,
                    totalTabla,
                    filtrado,
                    filtradoNoElim,
                    totalBytesEnvios,
                    totalBytesHistorial,
                    totalBytesAsignaciones,
                    totalBytesPaquete,
                    proporcion,
                    estimadoBytesEnvios,
                    estimadoBytesPaquete,
                    factorPaquete,
                    crecimientoDiarioBytes,
                    proyeccionAdicionalBytes,
                    proyeccionTotal2028Bytes
                });

                totalTablaBytes += totalBytesPaquete;
                totalEstimadoBytes += estimadoBytesPaquete;
                totalFiltrado += filtrado;
                totalFiltradoNoElim += filtradoNoElim;
                procesadas += 1;

                const sizeFmt = formatBytes(estimadoBytesPaquete);
                const totalFmt = formatBytes(totalBytesPaquete);
                const proyFmt = formatBytes(proyeccionTotal2028Bytes);
                console.log(
                    `Empresa ${didOwner} - ${nombreEmpresa}: enero=${sizeFmt.gb} GB, historico=${totalFmt.gb} GB, proy2028=${proyFmt.gb} GB`
                );
            } catch (err) {
                console.error(`Error estimando peso para empresa ${didOwner}:`, err.message);
            } finally {
                if (conn) await conn.release();
            }
        }

        resultados.sort((a, b) => b.estimadoBytesPaquete - a.estimadoBytesPaquete);

        console.log("====================================");
        console.log("Mini resumen de capacidad");
        console.log(`Ventana observada: ${fechaInicioDesde} -> ${hoy}`);
        console.log(`Proyeccion hasta: ${fechaProyeccion}`);
        console.log("====================================");

        resultados.forEach((r, index) => {
            const eneroFmt = formatBytes(r.estimadoBytesPaquete);
            const totalFmt = formatBytes(r.totalBytesPaquete);
            const proyFmt = formatBytes(r.proyeccionTotal2028Bytes);
            console.log(
                `${index + 1}. ${r.nombreEmpresa} (ID: ${r.didOwner}) => ` +
                `enero+ ${eneroFmt.gb} GB | ` +
                `historico ${totalFmt.gb} GB | ` +
                `proy.2028 ${proyFmt.gb} GB`
            );
        });

        const totalEstimadoFmt = formatBytes(totalEstimadoBytes);
        const totalTablaFmt = formatBytes(totalTablaBytes);
        const crecimientoDiarioTotalBytes = totalEstimadoBytes / diasObservados;
        const proyeccionAdicionalTotalBytes = crecimientoDiarioTotalBytes * diasHasta2028;
        const proyeccionTotal2028Bytes = totalTablaBytes + proyeccionAdicionalTotalBytes;
        const proyeccionTotalFmt = formatBytes(proyeccionTotal2028Bytes);
        const adicionalFmt = formatBytes(proyeccionAdicionalTotalBytes);
        const margenCrecimientoBytes = proyeccionTotal2028Bytes * 0.3;
        const pedidoConMargenBytes = proyeccionTotal2028Bytes + margenCrecimientoBytes;
        const margenFmt = formatBytes(margenCrecimientoBytes);
        const pedidoConMargenFmt = formatBytes(pedidoConMargenBytes);

        console.log("====================================");
        console.log(`Desde enero 2026: ${totalEstimadoFmt.gb} GB (${totalEstimadoFmt.mb} MB)`);
        console.log(`Desde los inicios: ${totalTablaFmt.gb} GB (${totalTablaFmt.mb} MB)`);
        console.log(`Crecimiento adicional estimado hasta fines de 2028: ${adicionalFmt.gb} GB (${adicionalFmt.mb} MB)`);
        console.log(`Espacio estimado total para pedir hasta 2028: ${proyeccionTotalFmt.gb} GB (${proyeccionTotalFmt.mb} MB)`);
        console.log(`Margen sugerido por crecimiento extra (30%): ${margenFmt.gb} GB (${margenFmt.mb} MB)`);
        console.log(`Espacio recomendado a pedir con margen: ${pedidoConMargenFmt.gb} GB (${pedidoConMargenFmt.mb} MB)`);
        console.log(`Filas enero 2026 no elim: ${totalFiltradoNoElim}`);
        console.log(`Filas historicas totales: ${resultados.reduce((acc, r) => acc + r.totalTabla, 0)}`);
        console.log(`Empresas procesadas: ${procesadas}`);
        console.log(`Empresas omitidas: ${omitidas}`);

        return {
            fechaInicioDesde,
            hoy,
            fechaProyeccion,
            diasObservados,
            diasHasta2028,
            totalFiltrado,
            totalFiltradoNoElim,
            totalEmpresas: procesadas,
            totalEmpresasOmitidas: omitidas,
            totalEstimadoBytes,
            totalTablaBytes,
            proyeccionAdicionalTotalBytes,
            proyeccionTotal2028Bytes,
            margenCrecimientoBytes,
            pedidoConMargenBytes,
            resultados
        };
    } catch (err) {
        console.error("Error general en resumenCapacidadEnviosHasta2028:", err.message);
        throw err;
    }
}

async function completarDidClientePorMlVendedorId() {
    try {
        console.log("Iniciando correccion de didCliente por ml_vendedor_id...");

        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("No se encontro 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);
        const fechaInicioDesde = "2026-03-31 00:00:00";

        const countPendientesQuery = `
            SELECT COUNT(*) AS cantidad
            FROM envios
            WHERE didCliente = 0
              AND fecha_inicio > ?
              AND ml_vendedor_id IS NOT NULL
              AND ml_vendedor_id <> ''
        `;

        const countSinMatchQuery = `
            SELECT COUNT(*) AS cantidad
            FROM envios e
            WHERE e.didCliente = 0
              AND e.fecha_inicio > ?
              AND e.ml_vendedor_id IS NOT NULL
              AND e.ml_vendedor_id <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM envios ref
                  WHERE ref.ml_vendedor_id = e.ml_vendedor_id
                    AND ref.didCliente <> 0
              )
        `;

        const updateQuery = `
            UPDATE envios e
            JOIN (
                SELECT ref.ml_vendedor_id, ref.didCliente
                FROM envios ref
                JOIN (
                    SELECT ml_vendedor_id, MAX(id) AS maxId
                    FROM envios
                    WHERE didCliente <> 0
                      AND ml_vendedor_id IS NOT NULL
                      AND ml_vendedor_id <> ''
                    GROUP BY ml_vendedor_id
                ) ult
                    ON ult.ml_vendedor_id = ref.ml_vendedor_id
                   AND ult.maxId = ref.id
            ) mapa
                ON mapa.ml_vendedor_id = e.ml_vendedor_id
            SET e.didCliente = mapa.didCliente
            WHERE e.didCliente = 0
              AND e.fecha_inicio > ?
              AND e.ml_vendedor_id IS NOT NULL
              AND e.ml_vendedor_id <> ''
        `;

        const sampleActualizadosQuery = `
            SELECT id, ml_vendedor_id, didCliente, fecha_inicio
            FROM envios
            WHERE didCliente <> 0
              AND fecha_inicio > ?
              AND ml_vendedor_id IS NOT NULL
              AND ml_vendedor_id <> ''
            ORDER BY id DESC
            LIMIT 10
        `;

        let totalEmpresas = 0;
        let totalOmitidas = 0;
        let totalPendientes = 0;
        let totalActualizados = 0;
        let totalSinMatch = 0;
        const resumen = [];

        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (Number.isNaN(didOwner)) continue;

            if (EMPRESAS_BLOQUEADAS.has(didOwner)) {
                totalOmitidas += 1;
                continue;
            }

            let conn;

            try {
                console.log(`Procesando empresa ${didOwner}...`);
                conn = await getConnection(didOwner);

                const pendientesAntesRows = await executeQuery(conn, countPendientesQuery, [fechaInicioDesde], { timeoutMs: 120000 });
                const pendientesAntes = Number(pendientesAntesRows?.[0]?.cantidad ?? 0);

                if (pendientesAntes === 0) {
                    resumen.push({
                        didOwner,
                        nombreEmpresa: empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`,
                        pendientesAntes: 0,
                        actualizados: 0,
                        sinMatch: 0
                    });
                    totalEmpresas += 1;
                    console.log(`Empresa ${didOwner}: sin registros para corregir`);
                    continue;
                }

                await executeQuery(conn, updateQuery, [fechaInicioDesde], { timeoutMs: 120000 });

                const pendientesDespuesRows = await executeQuery(conn, countPendientesQuery, [fechaInicioDesde], { timeoutMs: 120000 });
                const sinMatchRows = await executeQuery(conn, countSinMatchQuery, [fechaInicioDesde], { timeoutMs: 120000 });
                const muestraRows = await executeQuery(conn, sampleActualizadosQuery, [fechaInicioDesde], { timeoutMs: 120000 });

                const pendientesDespues = Number(pendientesDespuesRows?.[0]?.cantidad ?? 0);
                const sinMatch = Number(sinMatchRows?.[0]?.cantidad ?? 0);
                const actualizados = Math.max(0, pendientesAntes - pendientesDespues);
                const nombreEmpresa = empresaData[didOwnerStr]?.nombre || `Empresa ${didOwner}`;

                resumen.push({
                    didOwner,
                    nombreEmpresa,
                    pendientesAntes,
                    actualizados,
                    sinMatch,
                    muestra: muestraRows
                });

                totalEmpresas += 1;
                totalPendientes += pendientesAntes;
                totalActualizados += actualizados;
                totalSinMatch += sinMatch;

                console.log(
                    `Empresa ${didOwner} - ${nombreEmpresa}: pendientes=${pendientesAntes}, actualizados=${actualizados}, sinMatch=${sinMatch}`
                );
            } catch (err) {
                console.error(`Error corrigiendo empresa ${didOwner}:`, err.message);
            } finally {
                if (conn) await conn.release();
            }
        }

        resumen.sort((a, b) => b.actualizados - a.actualizados || b.pendientesAntes - a.pendientesAntes);

        console.log("====================================");
        console.log(`Resumen correccion didCliente por ml_vendedor_id desde ${fechaInicioDesde}`);
        console.log("====================================");

        resumen.forEach((item, index) => {
            console.log(
                `${index + 1}. ${item.nombreEmpresa} (ID: ${item.didOwner}) => ` +
                `pendientes=${item.pendientesAntes}, actualizados=${item.actualizados}, sinMatch=${item.sinMatch}`
            );
        });

        console.log("====================================");
        console.log(`Empresas procesadas: ${totalEmpresas}`);
        console.log(`Empresas omitidas: ${totalOmitidas}`);
        console.log(`Total pendientes detectados: ${totalPendientes}`);
        console.log(`Total actualizados: ${totalActualizados}`);
        console.log(`Total sin match: ${totalSinMatch}`);

        return {
            fechaInicioDesde,
            totalEmpresas,
            totalOmitidas,
            totalPendientes,
            totalActualizados,
            totalSinMatch,
            resumen
        };
    } catch (err) {
        console.error("Error general en completarDidClientePorMlVendedorId:", err.message);
        throw err;
    }
}

async function main() {
    console.log("Ejecutando procesarScript...");
    await corregirFechasHistorialTodasEmpresas();
    console.log("Fin de procesarScript");
}

main().catch((err) => {
    console.error("Error fatal en procesarScript:", err?.message || err);
});


module.exports = {
    ejecutarQueryParaTodasLasEmpresas,
    corregirFechasHistorialTodasEmpresas,
    contarEnviosTodasEmpresas,
    resumenCapacidadEnviosHasta2028,
    completarDidClientePorMlVendedorId
};
