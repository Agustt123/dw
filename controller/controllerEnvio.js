const { getConnection, executeQuery, redisClient, getConnectionLocalEnvios } = require("../db");

async function sincronizarEnviosUnaVez() {
    let connDW = null;

    // ✅ métricas de la corrida
    const metrics = {
        startedAt: Date.now(),
        elapsedMs: 0,
        empresas: 0,
        envios: 0,
        asignaciones: 0,
        estados: 0,
        eliminaciones: 0,
        porEmpresa: {}, // { [didOwner]: { envios, asignaciones, estados, eliminaciones } }
    };

    try {
        const empresaDataStr = await redisClient.get("empresasData");
        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return metrics; // ✅ siempre devolver algo
        }

        const empresaData = JSON.parse(empresaDataStr);
        const didOwners = Object.keys(empresaData);

        if (!didOwners.length) {
            console.log("⚠️ No hay empresas para sincronizar envíos.");
            return metrics;
        }

        // ✅ UNA sola conexión DW por corrida
        connDW = await getConnectionLocalEnvios();

        // ✅ Cachear columnas UNA sola vez por corrida
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

            metrics.empresas += 1;
            metrics.porEmpresa[didOwner] ??= { envios: 0, asignaciones: 0, estados: 0, eliminaciones: 0 };

            try {
                await sincronizarEnviosBatchParaEmpresa(
                    didOwner,
                    connDW,
                    columnasEnviosDW,
                    columnasAsignacionesDW,
                    columnasEstadosDW,
                    metrics
                );
            } catch (e) {
                console.error(`❌ Error sincronizando empresa ${didOwner}:`, e?.message || e);
            }
        }
    } catch (e) {
        console.error("❌ Error general en sincronizarEnviosUnaVez:", e?.message || e);
    } finally {
        try {
            if (connDW?.release) connDW.release();
        } catch (_) { /* ignore */ }

        metrics.elapsedMs = Date.now() - metrics.startedAt;
    }

    return metrics;
}

async function sincronizarEnviosBatchParaEmpresa(
    didOwner,
    connDW,
    columnasEnviosDW,
    columnasAsignacionesDW,
    columnasEstadosDW,
    metrics
) {
    console.log(`🔄 Sincronizando batch para empresa ${didOwner}`);

    let connEmpresa = null;
    let connEmpresaBad = false;

    try {
        connEmpresa = await getConnection(didOwner);

        await procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW, metrics);
        await procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW, metrics);
        await procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW, metrics);
        await procesarEliminaciones(connEmpresa, connDW, didOwner, metrics);

        console.log(`✅ Batch sincronizado para empresa ${didOwner}`);
    } catch (error) {
        const msg = String(error?.message || error).toLowerCase();
        const code = error?.code;

        if (
            error?.__shouldDestroyConnection ||
            code === "PROTOCOL_CONNECTION_LOST" ||
            code === "ECONNRESET" ||
            code === "ETIMEDOUT" ||
            msg.includes("timeout")
        ) {
            connEmpresaBad = true;
        }

        console.error(`❌ Error procesando empresa ${didOwner}:`, error?.message || error);
    } finally {
        if (!connEmpresa) return;

        if (connEmpresaBad && typeof connEmpresa.destroy === "function") {
            console.log(`[${didOwner}] 🔥 Destruyendo conexión empresa (timeout/red)`);
            connEmpresa.destroy();
        } else if (typeof connEmpresa.release === "function") {
            console.log(`[${didOwner}] Liberando conexión empresa`);
            connEmpresa.release();
        } else if (typeof connEmpresa.end === "function") {
            await connEmpresa.end();
        }
    }
}

// -------------------- Procesadores --------------------

async function procesarEnvios(connEmpresa, connDW, didOwner, columnasEnviosDW, metrics) {
    const lastEnvios = await executeQuery(connDW, "SELECT idMaxEnvios FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    const lastIdEnvios = lastEnvios.length ? lastEnvios[0].idMaxEnvios : 0;

    const enviosRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios WHERE id >  ?  AND autofecha > '2025-01-28 00:00:00' ORDER BY id ASC LIMIT 100",
        [lastIdEnvios]
    );

    // ✅ métricas
    metrics.porEmpresa[didOwner] ??= { envios: 0, asignaciones: 0, estados: 0, eliminaciones: 0 };
    metrics.porEmpresa[didOwner].envios += enviosRows.length;
    metrics.envios += enviosRows.length;

    if (enviosRows.length === 100) {
        console.log(`[${didOwner}] ⚠️ envios: LIMIT 100 alcanzado (posible backlog)`);
    }

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

async function procesarAsignaciones(connEmpresa, connDW, didOwner, columnasAsignacionesDW, metrics) {
    const lastAsignaciones = await executeQuery(connDW, "SELECT idMaxAsignaciones FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    const lastIdAsignaciones = lastAsignaciones.length ? lastAsignaciones[0].idMaxAsignaciones : 0;

    const asignacionesRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios_asignaciones WHERE autofecha > '2025-01-28 00:00:00'  AND id > ? ORDER BY id ASC LIMIT 100",
        [lastIdAsignaciones]
    );

    // ✅ métricas
    metrics.porEmpresa[didOwner] ??= { envios: 0, asignaciones: 0, estados: 0, eliminaciones: 0 };
    metrics.porEmpresa[didOwner].asignaciones += asignacionesRows.length;
    metrics.asignaciones += asignacionesRows.length;

    if (asignacionesRows.length === 100) {
        console.log(`[${didOwner}] ⚠️ asignaciones: LIMIT 100 alcanzado (posible backlog)`);
    }

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

async function procesarEstados(connEmpresa, connDW, didOwner, columnasEstadosDW, metrics) {
    const lastEstados = await executeQuery(connDW, "SELECT idMaxEstados FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    const lastIdEstados = lastEstados.length ? lastEstados[0].idMaxEstados : 0;

    const historialRows = await executeQuery(
        connEmpresa,
        "SELECT * FROM envios_historial WHERE id > ? AND autofecha > '2025-01-28 00:00:00' ORDER BY id ASC LIMIT 100   ",
        [lastIdEstados]
    );

    // ✅ métricas
    metrics.porEmpresa[didOwner] ??= { envios: 0, asignaciones: 0, estados: 0, eliminaciones: 0 };
    metrics.porEmpresa[didOwner].estados += historialRows.length;
    metrics.estados += historialRows.length;

    if (historialRows.length === 100) {
        console.log(`[${didOwner}] ⚠️ estados: LIMIT 100 alcanzado (posible backlog)`);
    }

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

async function procesarEliminaciones(connEmpresa, connDW, didOwner, metrics) {
    const limitParaEliminar = 100;
    const last = await executeQuery(connDW, "SELECT idMaxSisIngActiElim FROM envios_max_ids WHERE didOwner = ?", [didOwner]);
    const lastId = last.length ? last[0].idMaxSisIngActiElim : 0;

    const sistemaIngresosRows = await executeQuery(
        connEmpresa,
        `SELECT id, modulo, data FROM sistema_ingresos_activity
     WHERE autofecha > '2025-01-28 00:00:00' AND id > ? AND modulo = 'eliminra_envio' ORDER BY id ASC LIMIT ?`,
        [lastId, limitParaEliminar]
    );

    // ✅ métricas
    metrics.porEmpresa[didOwner] ??= { envios: 0, asignaciones: 0, estados: 0, eliminaciones: 0 };
    metrics.porEmpresa[didOwner].eliminaciones += sistemaIngresosRows.length;
    metrics.eliminaciones += sistemaIngresosRows.length;

    if (sistemaIngresosRows.length === 100) {
        console.log(`[${didOwner}] ⚠️ eliminaciones: LIMIT alcanzado (posible backlog)`);
    }

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
