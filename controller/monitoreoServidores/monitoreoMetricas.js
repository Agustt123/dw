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
    if (!respData) return null;
    return respData.simple || respData;
}

function maxOrNull(values) {
    const nums = values.filter((v) => Number.isFinite(v));
    return nums.length ? Math.max(...nums) : null;
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

    // 1) did único para TODA la corrida (por microservicio)
    const did = await getNextDid(db);
    if (!did) throw new Error("No se pudo obtener did.");

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
            error = err?.message ? err.message.slice(0, 255) : "ERROR";
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
        });
    }

    // 3) Agregamos un DID MÁS para el "microservicio conjunto" con máximos
    const didConjunto = await getNextDid(db);
    if (!didConjunto) throw new Error("No se pudo obtener didConjunto.");

    const latenciaMax = maxOrNull(results.map((r) => r.latenciaMs));
    const usoRamMax = maxOrNull(results.map((r) => r.usoRam));
    const usoCpuMax = maxOrNull(results.map((r) => r.usoCpu));
    const usoDiscoMax = maxOrNull(results.map((r) => r.usoDisco));
    const tempMax = maxOrNull(results.map((r) => r.temperaturaCpu));

    const carga1mMax = maxOrNull(results.map((r) => r.carga1m));
    const ramProcesoMax = maxOrNull(results.map((r) => r.ramProcesoMb));
    const cpuProcesoMax = maxOrNull(results.map((r) => r.cpuProceso));

    // (opcional) ok del conjunto: 1 si al menos uno estuvo ok, sino 0
    const okConjunto = results.some((r) => r.ok === 1) ? 1 : 0;

    const valuesConjunto = [
        didConjunto,
        "conjunto",
        "ALL",
        okConjunto,
        null, // codigoHttp
        latenciaMax,
        null, // error
        usoRamMax === null ? null : round1(usoRamMax),
        usoCpuMax === null ? null : round1(usoCpuMax),
        usoDiscoMax === null ? null : round1(usoDiscoMax),
        tempMax === null ? null : round1(tempMax),
        carga1mMax === null ? null : round1(carga1mMax),
        ramProcesoMax === null ? null : round1(ramProcesoMax),
        cpuProcesoMax === null ? null : round1(cpuProcesoMax),
    ];

    await executeQuery(db, insertSql, valuesConjunto);

    const conjuntoRow = {
        did: didConjunto,
        servidor: "conjunto",
        endpoint: "ALL",
        ok: okConjunto,
        codigoHttp: null,
        latenciaMs: latenciaMax,
        error: null,
        usoRam: valuesConjunto[7],
        usoCpu: valuesConjunto[8],
        usoDisco: valuesConjunto[9],
        temperaturaCpu: valuesConjunto[10],
        carga1m: valuesConjunto[11],
        ramProcesoMb: valuesConjunto[12],
        cpuProceso: valuesConjunto[13],
    };

    return {
        did,
        didConjunto,
        results,
        conjunto: conjuntoRow,
    };
}

module.exports = { monitoreoRecursos };
