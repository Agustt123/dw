const { getConnection, getConnectionLocal, executeQuery, redisClient } = require("../db");

async function sincronizarEnviosParaTodasLasEmpresas() {
    try {
        const empresaDataStr = await redisClient.get("empresasData");

        if (!empresaDataStr) {
            console.error("❌ No se encontró 'empresasData' en Redis.");
            return;
        }

        const empresaData = JSON.parse(empresaDataStr);
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

        // Luego procesamos cada empresa
        for (const didOwnerStr of didOwners) {
            const didOwner = parseInt(didOwnerStr, 10);
            if (isNaN(didOwner)) continue;

            try {
                await sincronizarEnviosParaEmpresa(didOwner);
            } catch (error) {
                console.error(`❌ Error sincronizando datos para empresa ${didOwner}:`, error);
            }
        }

    } catch (error) {
        console.error("❌ Error general en la sincronización:", error);
    }
}

async function sincronizarEnviosParaEmpresa(didOwner) {
    console.log(`🔄 Iniciando sincronización para empresa ${didOwner}`);

    const connEmpresa = await getConnection(didOwner);
    const connDW = await getConnectionLocal(didOwner);

    try {
        const columnasEnviosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM envios")).map(c => c.Field);
        const columnasAsignacionesDW = (await executeQuery(connDW, "SHOW COLUMNS FROM asignaciones")).map(c => c.Field);
        const columnasEstadosDW = (await executeQuery(connDW, "SHOW COLUMNS FROM estado")).map(c => c.Field);

        // ---- ENVÍOS ----
        const lastEnvios = await executeQuery(connDW, 'SELECT idMaxEnvios FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
        let lastIdEnvios = lastEnvios.length ? lastEnvios[0].idMaxEnvios : 0;

        const enviosRows = await executeQuery(connEmpresa, 'SELECT * FROM envios WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdEnvios]);

        for (const envio of enviosRows) {
            const envioDW = {
                ...envio,
                didEnvio: envio.did,
                didOwner
            };
            delete envioDW.did;

            const envioFiltrado = {};
            for (const [k, v] of Object.entries(envioDW)) {
                if (columnasEnviosDW.includes(k)) envioFiltrado[k] = v;
            }

            if (Object.keys(envioFiltrado).length === 0) continue;

            const columnas = Object.keys(envioFiltrado);
            const valores = Object.values(envioFiltrado);
            const placeholders = columnas.map(() => "?").join(",");
            const updateSet = columnas.filter(c => c !== "didEnvio" && c !== "didOwner").map(c => `${c} = VALUES(${c})`).join(",");

            const sql = `
                INSERT INTO envios (${columnas.join(",")})
                VALUES (${placeholders})
                ON DUPLICATE KEY UPDATE ${updateSet}
            `;
            await executeQuery(connDW, sql, valores);

            await executeQuery(connDW,
                `UPDATE envios_max_ids SET idMaxEnvios = ? WHERE didOwner = ?`,
                [envio.id, didOwner]);
        }

        console.log(`✅ ${enviosRows.length} envíos sincronizados para empresa ${didOwner}`);

        // ---- ASIGNACIONES ----
        const lastAsignaciones = await executeQuery(connDW, 'SELECT idMaxAsignaciones FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
        let lastIdAsignaciones = lastAsignaciones.length ? lastAsignaciones[0].idMaxAsignaciones : 0;

        const asignacionesRows = await executeQuery(connEmpresa, 'SELECT * FROM envios_asignaciones WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdAsignaciones]);

        for (const asignacion of asignacionesRows) {
            const asignacionDW = {
                ...asignacion,
                didAsignacion: asignacion.did,
                didOwner
            };
            delete asignacionDW.did;

            const asignacionFiltrada = {};
            for (const [k, v] of Object.entries(asignacionDW)) {
                if (columnasAsignacionesDW.includes(k)) asignacionFiltrada[k] = v;
            }

            if (Object.keys(asignacionFiltrada).length === 0) continue;

            const columnas = Object.keys(asignacionFiltrada);
            const valores = Object.values(asignacionFiltrada);
            const placeholders = columnas.map(() => "?").join(",");
            const updateSet = columnas.filter(c => c !== "didAsignacion" && c !== "didOwner").map(c => `${c} = VALUES(${c})`).join(",");

            const sql = `
                INSERT INTO asignaciones (${columnas.join(",")})
                VALUES (${placeholders})
                ON DUPLICATE KEY UPDATE ${updateSet}
            `;
            await executeQuery(connDW, sql, valores);

            await executeQuery(connDW,
                `UPDATE envios_max_ids SET idMaxAsignaciones = ? WHERE didOwner = ?`,
                [asignacion.id, didOwner]);
        }

        console.log(`✅ ${asignacionesRows.length} asignaciones sincronizadas para empresa ${didOwner}`);

        // ---- ESTADOS ----
        const lastEstados = await executeQuery(connDW, 'SELECT idMaxEstados FROM envios_max_ids WHERE didOwner = ?', [didOwner]);
        let lastIdEstados = lastEstados.length ? lastEstados[0].idMaxEstados : 0;

        const historialRows = await executeQuery(connEmpresa, 'SELECT * FROM envios_historial WHERE id > ? ORDER BY id ASC LIMIT 100', [lastIdEstados]);

        for (const hist of historialRows) {
            const estadoDW = {
                ...hist,
                didEstado: hist.did,
                didOwner
            };
            delete estadoDW.did;

            const estadoFiltrado = {};
            for (const [k, v] of Object.entries(estadoDW)) {
                if (columnasEstadosDW.includes(k)) estadoFiltrado[k] = v;
            }

            if (Object.keys(estadoFiltrado).length === 0) continue;

            const columnas = Object.keys(estadoFiltrado);
            const valores = Object.values(estadoFiltrado);
            const placeholders = columnas.map(() => "?").join(",");
            const updateSet = columnas.filter(c => c !== "didEstado" && c !== "didOwner").map(c => `${c} = VALUES(${c})`).join(",");

            const sql = `
                INSERT INTO estado (${columnas.join(",")})
                VALUES (${placeholders})
                ON DUPLICATE KEY UPDATE ${updateSet}
            `;
            await executeQuery(connDW, sql, valores);

            await executeQuery(connDW,
                `UPDATE envios_max_ids SET idMaxEstados = ? WHERE didOwner = ?`,
                [hist.id, didOwner]);
        }

        console.log(`✅ ${historialRows.length} estados sincronizados para empresa ${didOwner}`);

    } catch (error) {
        console.error(`❌ Error procesando empresa ${didOwner}:`, error);
    } finally {
        await connEmpresa.end();
        await connDW.end();
    }
}

module.exports = {
    sincronizarEnviosParaTodasLasEmpresas,
};
