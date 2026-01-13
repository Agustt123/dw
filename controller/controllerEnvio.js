const { getConnection, getConnectionLocal, executeQuery, redisClient } = require("../db");

async function sincronizarEnviosParaTodasLasEmpresas() {
    while (true) {
        const connDWTemp = await getConnectionLocal(0); // conexión temporal para DW
        try {
            const empresaDataStr = await redisClient.get("empresasData");

            if (!empresaDataStr) {
                console.error("❌ No se encontró 'empresasData' en Redis.");
                await esperar(30000); // Espera 30 segundos antes de volver a intentar
                continue;
            }

            const empresaData = JSON.parse(empresaDataStr);
            const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]

            // Insertar todos los didOwners si no existen
            for (const didOwnerStr of didOwners) {
                const didOwner = parseInt(didOwnerStr, 10);
                if (isNaN(didOwner)) continue;


                await executeQuery(
                    connDWTemp,
                    `INSERT IGNORE INTO envios_max_ids (didOwner, idMaxEnvios, idMaxAsignaciones, idMaxEstados)
                     VALUES (?, 0, 0, 0)`,
                    [didOwner]
                );
            }
            await connDWTemp.end();

            // Procesar cada empresa, una tanda de hasta 100 registros
            for (const didOwnerStr of didOwners) {
                const didOwner = parseInt(didOwnerStr, 10);
                if (isNaN(didOwner)) continue;

                try {
                    await sincronizarEnviosBatchParaEmpresa(didOwner);
                } catch (error) {
                    console.error(`❌ Error sincronizando datos para empresa ${didOwner}:`, error);
                }
            }

            // Pausa para no saturar el servidor (ejemplo: 10 segundos)
            await esperar(10000);

        } catch (error) {
            console.error("❌ Error general en la sincronización:", error);
            await esperar(30000); // Espera 30 segundos si falla algo grave
        }
        finally {
            // Asegurarse de cerrar la conexión temporal si aún está abierta
            if (connDWTemp && typeof connDWTemp.end === "function") {
                await connDWTemp.end();
            }
        }
    }
}
async function sincronizarEnviosParaTodasLasEmpresas2() {
    while (true) {
        try {
            /*   const empresaDataStr = await redisClient.get("empresasData");
   
               if (!empresaDataStr) {
                   console.error("❌ No se encontró 'empresasData' en Redis.");
                   await esperar(30000); // Espera 30 segundos antes de volver a intentar
                   continue;
               }
   
               const empresaData = JSON.parse(empresaDataStr);*/

            // Solo empresa 164 en entorno de prueba
            const didOwners = ["164"];

            // Código original comentado para cuando se necesiten todas:
            /*
            const didOwners = Object.keys(empresaData); // Ej: ["2", "3", "4"]

            // Insertar todos los didOwners si no existen
            const connDWTemp = await getConnectionLocal(0); // conexión temporal para DW
            for (const didOwnerStr of didOwners) {
                const didOwner = parseInt(didOwnerStr, 10);
                if (isNaN(didOwner)) continue;

                await executeQuery(
                    connDWTemp,
                    `INSERT IGNORE INTO envios_max_ids (didOwner, idMaxEnvios, idMaxAsignaciones, idMaxEstados)
                     VALUES (?, 0, 0, 0)`,
                    [didOwner]
                );
            }
            await connDWTemp.end();
            */

            // Procesar solo empresa 164
            for (const didOwnerStr of didOwners) {
                const didOwner = parseInt(didOwnerStr, 10);
                if (isNaN(didOwner)) continue;

                try {
                    await sincronizarEnviosBatchParaEmpresa(164);
                } catch (error) {
                    console.error(`❌ Error sincronizando datos para empresa ${didOwner}:`, error);
                }
            }

            // Pausa para no saturar el servidor
            await esperar(10000);

        } catch (error) {
            console.error("❌ Error general en la sincronización:", error);
            await esperar(30000);
        }
    }
}
async function sincronizarEnviosBatchParaEmpresa(didOwner) {
    console.log(`🔄 Sincronizando batch para empresa ${didOwner}`);

    let connEmpresa;
    let connDW;

    try {
        try {
            connEmpresa = await getConnection(didOwner);
        } catch (err) {
            console.error(`❌ Error al obtener conexión para empresa ${didOwner}:`, err);
            return; // Salir del batch, sin continuar
        }
        connDW = await getConnectionLocal(didOwner);

        const columnasEnviosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM envios")).map(c => c.Field);
        const columnasAsignacionesDW = (await executeQuery(connDW, "SHOW COLUMNS FROM asignaciones")).map(c => c.Field);
        const columnasEstadosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM estado")).map(c => c.Field);

        await procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW);
        console.log(`✅ Envios sincronizados para empresa ${didOwner}`);

        await procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW);
        console.log(`✅ Asignaciones sincronizadas para empresa ${didOwner}`);

        await procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW);
        await procesarEliminaciones(connEmpresa, connDW, didOwner);

        console.log(`✅ Batch sincronizado para empresa ${didOwner}`);
    } catch (error) {
        console.error(`❌ Error procesando empresa ${didOwner}:`, error);
    } finally {
        // Liberar si existe y tiene release
        if (connEmpresa && typeof connEmpresa.release === "function") {
            console.log(`[${didOwner}] Liberando conexión`);
            connEmpresa.release();
        }

        // Si querés cerrar la conexión local explícitamente (aunque no es un pool)
        if (connDW && typeof connDW.end === "function") {
            await connDW.end();
        }
    }
}


async function procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW) {
    const lastEnvios = await executeQuery(connDW, 'SELECT idMaxEnvios FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
    let lastIdEnvios = lastEnvios.length ? lastEnvios[0].idMaxEnvios : 0;

    const enviosRows = await executeQuery(connEmpresa, 'SELECT * FROM envios WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdEnvios]);

    let lastProcessedId = 0;

    // Columnas que NO pueden ser NULL y para las que NO tienes un valor en la fuente
    const columnasNoNull = [
        "estimated_delivery_time_date",
        "estimated_delivery_time_date_72",
        "estimated_delivery_time_date_480",
        // agrega acá más si es necesario
    ];

    for (const envio of enviosRows) {
        const envioDW = {
            ...envio,
            didEnvio: envio.did,
            didOwner
        };

        const envioFiltrado = {};
        for (const [k, v] of Object.entries(envioDW)) {
            if (columnasEnviosDW.includes(k) && k !== "id") {
                if (v === null && columnasNoNull.includes(k)) {
                    // no la agregamos, la ignoramos para que no aparezca en el INSERT
                    continue;
                }
                envioFiltrado[k] = v; // la incluimos solo si pasa el filtro
            }
        }


        if (Object.keys(envioFiltrado).length === 0) continue;

        const columnas = Object.keys(envioFiltrado);
        const valores = Object.values(envioFiltrado);
        const placeholders = columnas.map(() => "?").join(",");
        const updateSet = columnas
            .filter(c => c !== "didEnvio" && c !== "didOwner")
            .map(c => `${c} = VALUES(${c})`)
            .join(",");

        const sql = `
            INSERT INTO envios (${columnas.join(",")})
            VALUES (${placeholders})
            ON DUPLICATE KEY UPDATE ${updateSet}
        `;

        await executeQuery(connDW, sql, valores);

        lastProcessedId = envio.id;
    }

    if (lastProcessedId > 0) {
        await executeQuery(connDW,
            `UPDATE envios_max_ids SET idMaxEnvios = ? WHERE didOwner = ?`,
            [lastProcessedId, didOwner]);
    }
}



// Implementa cambios similares en procesarAsignaciones y procesarEstados




async function procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW) {
    const lastAsignaciones = await executeQuery(connDW, 'SELECT idMaxAsignaciones FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
    let lastIdAsignaciones = lastAsignaciones.length ? lastAsignaciones[0].idMaxAsignaciones : 0;

    const asignacionesRows = await executeQuery(connEmpresa, 'SELECT * FROM envios_asignaciones WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdAsignaciones]);

    let lastProcessedId = 0;

    for (const asignacion of asignacionesRows) {
        const asignacionDW = { ...asignacion, didAsignacion: asignacion.did, didOwner };

        const asignacionFiltrado = {};
        for (const [k, v] of Object.entries(asignacionDW)) {
            if (columnasAsignacionesDW.includes(k)) asignacionFiltrado[k] = v;
        }


        if (Object.keys(asignacionFiltrado).length === 0) continue;

        const columnas = Object.keys(asignacionFiltrado);
        const valores = Object.values(asignacionFiltrado);
        const placeholders = columnas.map(() => "?").join(",");
        const updateSet = columnas
            .filter(c => c !== "didAsignacion" && c !== "didOwner")
            .map(c => `${c} = VALUES(${c})`)
            .join(",");

        const sql = `
            INSERT INTO asignaciones (${columnas.join(",")})
            VALUES (${placeholders})
            ON DUPLICATE KEY UPDATE ${updateSet}
        `;
        await executeQuery(connDW, sql, valores, true);

        lastProcessedId = asignacion.id;
    }

    if (lastProcessedId > 0) {
        await executeQuery(connDW,
            'UPDATE envios_max_ids SET idMaxAsignaciones = ? WHERE didOwner = ?',
            [lastProcessedId, didOwner]);
    }
}

async function procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW) {
    const lastEstados = await executeQuery(connDW, 'SELECT idMaxEstados FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
    let lastIdEstados = lastEstados.length ? lastEstados[0].idMaxEstados : 0;

    const historialRows = await executeQuery(connEmpresa, 'SELECT * FROM envios_historial WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdEstados], true);

    let lastProcessedId = 0;

    for (const hist of historialRows) {
        const estadoDW = { ...hist, didEstado: hist.did, didOwner };

        const estadoFiltrado = {};
        for (const [k, v] of Object.entries(estadoDW)) {
            if (columnasEstadosDW.includes(k)) estadoFiltrado[k] = v;
        }

        if (Object.keys(estadoFiltrado).length === 0) continue;

        // Insertar el nuevo registro ignorando el id
        const columnas = Object.keys(estadoFiltrado);
        const valores = Object.values(estadoFiltrado);
        const placeholders = columnas.map(() => "?").join(",");
        const updateSet = columnas
            .filter(c => c !== "didEstado" && c !== "didOwner")
            .map(c => `${c} = VALUES(${c})`)
            .join(",");

        const sql = `
            INSERT INTO estado (${columnas.join(",")})
            VALUES (${placeholders})
            ON DUPLICATE KEY UPDATE ${updateSet}
        `;
        await executeQuery(connDW, sql, valores);
        console.log("estado insertado:", estadoFiltrado);


        lastProcessedId = hist.id;
    }

    // Actualizar idMaxEstados si hubo algún insert válido
    if (lastProcessedId > 0) {
        await executeQuery(connDW,
            'UPDATE envios_max_ids SET idMaxEstados = ? WHERE didOwner = ?',
            [lastProcessedId, didOwner]);
    }
}


async function procesarEliminaciones(connEmpresa, connDW, didOwner) {
    const limitParaEliminar = 100; // Define el límite para la consulta
    const lastIdSisIngActiElim = await executeQuery(connDW, 'SELECT idMaxSisIngActiElim FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
    let lastAidMaxSisIngActiElim = lastIdSisIngActiElim.length ? lastIdSisIngActiElim[0].idMaxSisIngActiElim : 0;

    const sistemaIngresosRows = await executeQuery(connEmpresa,
        `SELECT id, modulo, data FROM sistema_ingresos_activity 
         WHERE id > ? AND modulo = 'eliminra_envio' ORDER BY id ASC LIMIT ?`,
        [lastAidMaxSisIngActiElim, limitParaEliminar]);

    let maxIdEliminacion = 0; // Para almacenar el último ID de eliminación procesado

    for (const row of sistemaIngresosRows) {
        const { id, modulo, data } = row;

        if (modulo !== 'eliminra_envio') {
            console.log(`Módulo ignorado: ${modulo}`); // Registrar módulos no relevantes
            continue; // Saltar a la siguiente iteración si no es el módulo esperado
        }

        //&   console.log("Procesando eliminación para la fila:", row);

        const result = await executeQuery(connDW,
            `UPDATE envios SET elim = 1 WHERE didOwner = ? AND didEnvio = ?`,
            [didOwner, data]);

        // Solo actualizar envios_max_ids si se afectó alguna fila
        if (result.affectedRows > 0) {
            console.log("Se realizó una eliminación, ID:", id);
            maxIdEliminacion = Math.max(maxIdEliminacion, id); // Guardar el ID más alto procesado
        }
    }

    // Actualizar idMaxSisIngActiElim solo si se realizaron eliminaciones
    if (maxIdEliminacion > 0) {
        await executeQuery(connDW,
            `UPDATE envios_max_ids SET idMaxSisIngActiElim = ? WHERE didOwner = ?`,
            [maxIdEliminacion, didOwner]);
        console.log("ID máximo actualizado a:", maxIdEliminacion);
    } else {
        console.log("No se realizaron eliminaciones, no se actualiza el ID máximo.");
    }
}


function esperar(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = {
    sincronizarEnviosParaTodasLasEmpresas,
};
