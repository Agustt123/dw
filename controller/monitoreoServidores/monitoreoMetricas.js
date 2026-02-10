const axios = require("axios");
const { executeQuery } = require("../../db");

function toNumberOrNull(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function round1(n) {
    return Math.round(n * 10) / 10;
}

async function getNextDid(db) {
    // did = max(did) + 1
    const rows = await executeQuery(
        db,
        "SELECT IFNULL(MAX(did), 0) + 1 AS did FROM sat_monitoreo_recursos",
        []
    );
    return rows?.[0]?.did;
}

function pickSimple(respData) {
    // Si el endpoint devuelve { simple, raw } -> usamos simple
    // Si devuelve simple directo -> usamos respData
    if (!respData) return null;
    return respData.simple || respData;
}

async function monitoreoRecursos(
    db,
    services = [
        { key: "asignaciones", url: "http://asignaciones.lightdata.app/_sat/metrics" },
        { key: "backgps", url: "http://backgps2.lightdata.com.ar/_sat/metrics" },
        { key: "colecta", url: "http://colecta.lightdata.app/_sat/metrics" },
        { key: "aplanta", url: "http://aplanta.lightdata.app/_sat/metrics" },
        { key: "altaEnvios", url: "http://altaenvios.lightdata.com.ar/_sat/metrics" },
        { key: "fulfillment", url: "http://ffull.lightdata.app/_sat/metrics" },
        { key: "ffmobile", url: "http://ffmovil.lightdata.app/_sat/metrics" },
        { key: "callback", url: "http://whml.lightdata.app/_sat/metrics" },
        { key: "lightdatito", url: "http://node1.liit.com.ar/_sat/metrics" },
        { key: "websocket_mail", url: "https://notificaremails.lightdata.com.ar/_sat/metrics" },
        { key: "etiquetas", url: "http://printserver.lightdata.app/_sat/metrics" },
        { key: "estados", url: "http://serverestado.lightdata.app/_sat/metrics" },
        { key: "apimovil", url: "http://apimovil2.lightdata.app/_sat/metrics" },
    ],
    options = {}
) {
    const timeoutMs = options.timeoutMs ?? 2000;

    // 1) did único para TODA la corrida
    const did = await getNextDid(db);
    if (!did) throw new Error("No se pudo obtener did.");

    // Query de insert (misma para todos)
    const insertSql = `
    INSERT INTO sat_monitoreo_recursos
    (did, servidor, endpoint, ok, codigoHttp, latenciaMs, error,
     usoRam, usoCpu, usoDisco, temperaturaCpu,
     carga1m, ramProcesoMb, cpuProceso)
    VALUES
    (?,   ?,        ?,        ?,  ?,         ?,         ?,
     ?,     ?,      ?,        ?,
     ?,      ?,           ?)
  `;

    const results = [];

    // 2) Recorremos los servers y guardamos 1 fila por server
    for (const s of services) {
        const endpoint = s.url;

        let ok = 0;
        let codigoHttp = null;
        let latenciaMs = null;
        let error = null;

        // Métricas (simple)
        let usoRam = null;
        let usoCpu = null;
        let usoDisco = null;
        let temperaturaCpu = null;

        let carga1m = null;
        let ramProcesoMb = null;
        let cpuProceso = null;

        try {
            const t0 = process.hrtime.bigint();
            const resp = await axios.get(endpoint, {
                timeout: timeoutMs,
                validateStatus: () => true,
            });
            const t1 = process.hrtime.bigint();

            latenciaMs = Math.max(1, Math.round(Number(t1 - t0) / 1_000_000));
            codigoHttp = resp.status;

            ok = resp.status >= 200 && resp.status < 300 ? 1 : 0;

            const simple = pickSimple(resp.data);

            // mapeo a columnas (si no viene, queda null)
            if (simple) {
                usoRam = toNumberOrNull(simple.usoRamPct);
                usoCpu = toNumberOrNull(simple.usoCpuPct);
                usoDisco = toNumberOrNull(simple.usoDiscoPct);
                temperaturaCpu = toNumberOrNull(simple.tempC);

                carga1m = toNumberOrNull(simple.carga1m);
                ramProcesoMb = toNumberOrNull(simple.ramProcesoMB);
                cpuProceso = toNumberOrNull(simple.usoCpuProcesoPct);

                // redondeo
                usoRam = usoRam === null ? null : round1(usoRam);
                usoCpu = usoCpu === null ? null : round1(usoCpu);
                usoDisco = usoDisco === null ? null : round1(usoDisco);
                temperaturaCpu = temperaturaCpu === null ? null : round1(temperaturaCpu);

                carga1m = carga1m === null ? null : round1(carga1m);
                ramProcesoMb = ramProcesoMb === null ? null : round1(ramProcesoMb);
                cpuProceso = cpuProceso === null ? null : round1(cpuProceso);
            } else {
                ok = 0;
                error = "Respuesta vacía";
            }
        } catch (err) {
            ok = 0;
            error = (err && err.message) ? err.message.slice(0, 255) : "ERROR";
            console.error(`[SAT-RECURSOS] ${s.key} ERROR`, error);
        }

        const values = [
            did,
            s.key,
            endpoint,
            ok,
            codigoHttp,
            latenciaMs,
            error,
            usoRam,
            usoCpu,
            usoDisco,
            temperaturaCpu,
            carga1m,
            ramProcesoMb,
            cpuProceso,
        ];

        await executeQuery(db, insertSql, values);

        results.push({
            did,
            servidor: s.key,
            ok,
            codigoHttp,
            latenciaMs,
            usoRam,
            usoCpu,
            usoDisco,
            temperaturaCpu,
        });
    }

    return { did, results };
}

module.exports = { monitoreoRecursos };
