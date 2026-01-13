const { getConnection, getConnectionLocal, executeQuery, redisClient } = require("../db");

async function sincronizarEnviosUnaVez() {
    let connDW = null;

    try {
        const empresaDataStr = await redisClient.get("empresasData");
        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);

        if (!didOwners.length) {
            console.log("⚠️ No hay empresas para sincronizar envíos.");
            return;
        }

        // ✅ UNA sola conexión DW por corrida
        connDW = await getConnectionLocal();

        // ✅ Cachear columnas UNA sola vez por corrida (ahorra miles de queries)
        const columnasEnviosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM envios")).map(c => c.Field);
        const columnasAsignacionesDW = (await executeQuery(connDW, "SHOW COLUMNS FROM asignaciones")).map(c => c.Field);
        const columnasEstadosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM estado")).map(c => c.Field);

        // Insert IGNORE envios_max_ids
        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            await executeQuery(
                connDW,
                `INSERT IGNORE INTO envios_max_ids
         (didOwner, idMaxEnvios, idMaxAsignaciones, idMaxEstados)
         VALUES (?, 0, 0, 0)`,
                [didOwner]
            );
        }

        // Procesar empresas (reusando connDW)
        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            try {
                await sincronizarEnviosBatchParaEmpresa(
                    didOwner,
                    connDW,
                    columnasEnviosDW,
                    columnasAsignacionesDW,
                    columnasEstadosDW
                );
            } catch (e) {
                console.error(`❌ Error sincronizando empresa ${didOwner}:`, e);
            }
        }
    } catch (e) {
        console.error("❌ Error general en sincronizarEnviosUnaVez:", e);
    } finally {
        if (connDW?.release) connDW.release();
    }
}

async function sincronizarEnviosBatchParaEmpresa(
    didOwner,
    connDW,
    columnasEnviosDW,
    columnasAsignacionesDW,
    columnasEstadosDW
) {
    console.log(`🔄 Sincronizando batch para empresa ${didOwner}`);

    let connEmpresa = null;

    try {
        connEmpresa = await getConnection(didOwner);

        await procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW);
        await procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW);
        await procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW);
        await procesarEliminaciones(connEmpresa, connDW, didOwner);

        console.log(`✅ Batch sincronizado para empresa ${didOwner}`);
    } catch (error) {
        console.error(`❌ Error procesando empresa ${didOwner}:`, error);
    } finally {
        if (connEmpresa?.release) {
            console.log(`[${didOwner}] Liberando conexión empresa`);
            connEmpresa.release();
        }
    }
}

// -------------------- Procesadores (tu código tal cual) --------------------

async function procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW) {
    const lastEnvios = await executeQuery(connDW, "SELECT idMaxEnvios FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    let lastIdEnvios = lastEnvios.length ? lastEnvios[0].idMaxEnvios : 0;

    const enviosRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios WHERE id > ? ORDER BY id ASC LIMIT 100",
        [lastIdEnvios]
    );

    let lastProcessedId = 0;

    const columnasNoNull = [
        "estimated_delivery_time_date",
        "estimated_delivery_time_date_72",
        "estimated_delivery_time_date_480",
    ];

    for (const envio of enviosRows) {
        const envioDW = { ...envio, didEnvio: envio.did, didOwner };

        const envioFiltrado = {};
        for (const [k, v] of Object.entries(envioDW)) {
            if (columnasEnviosDW.includes(k) && k !== "id") {
                if (v === null && columnasNoNull.includes(k)) continue;
                envioFiltrado[k] = v;
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
        await executeQuery(
            connDW,
            "UPDATE envios_max_ids SET idMaxEnvios = ? WHERE didOwner = ?",
            [lastProcessedId, didOwner]
        );
    }
}

async function procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW) {
    const lastAsignaciones = await executeQuery(connDW, "SELECT idMaxAsignaciones FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    let lastIdAsignaciones = lastAsignaciones.length ? lastAsignaciones[0].idMaxAsignaciones : 0;

    const asignacionesRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios_asignaciones WHERE id > ? ORDER BY id ASC LIMIT 100",
        [lastIdAsignaciones]
    );

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

        await executeQuery(connDW, sql, valores);
        lastProcessedId = asignacion.id;
    }

    if (lastProcessedId > 0) {
        await executeQuery(
            connDW,
            "UPDATE envios_max_ids SET idMaxAsignaciones = ? WHERE didOwner = ?",
            [lastProcessedId, didOwner]
        );
    }
}

async function procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW) {
    const lastEstados = await executeQuery(connDW, "SELECT idMaxEstados FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    let lastIdEstados = lastEstados.length ? lastEstados[0].idMaxEstados : 0;

    const historialRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios_historial WHERE id > ? ORDER BY id ASC LIMIT 100",
        [lastIdEstados]
    );

    let lastProcessedId = 0;

    for (const hist of historialRows) {
        const estadoDW = { ...hist, didEstado: hist.did, didOwner };

        const estadoFiltrado = {};
        for (const [k, v] of Object.entries(estadoDW)) {
            if (columnasEstadosDW.includes(k)) estadoFiltrado[k] = v;
        }

        if (Object.keys(estadoFiltrado).length === 0) continue;

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
        lastProcessedId = hist.id;
    }

    if (lastProcessedId > 0) {
        await executeQuery(
            connDW,
            "UPDATE envios_max_ids SET idMaxEstados = ? WHERE didOwner = ?",
            [lastProcessedId, didOwner]
        );
    }
}

async function procesarEliminaciones(connEmpresa, connDW, didOwner) {
    const limitParaEliminar = 100;
    const last = await executeQuery(connDW, "SELECT idMaxSisIngActiElim FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    let lastId = last.length ? last[0].idMaxSisIngActiElim : 0;

    const sistemaIngresosRows = await executeQuery(
        connEmpresa,
        `SELECT id, modulo, data FROM sistema_ingresos_activity
     WHERE id > ? AND modulo = 'eliminra_envio' ORDER BY id ASC LIMIT ?`,
        [lastId, limitParaEliminar]
    );

    let maxIdEliminacion = 0;

    for (const row of sistemaIngresosRows) {
        const { id, modulo, data } = row;
        if (modulo !== "eliminra_envio") continue;

        const result = await executeQuery(
            connDW,
            "UPDATE envios SET elim = 1 WHERE didOwner = ? AND didEnvio = ?",
            [didOwner, data]
        );

        if (result.affectedRows > 0) maxIdEliminacion = Math.max(maxIdEliminacion, id);
    }

    if (maxIdEliminacion > 0) {
        await executeQuery(
            connDW,
            "UPDATE envios_max_ids SET idMaxSisIngActiElim = ? WHERE didOwner = ?",
            [maxIdEliminacion, didOwner]
        );
    }
}

module.exports = {
    sincronizarEnviosUnaVez,
};
