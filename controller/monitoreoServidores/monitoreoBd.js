const axios = require("axios");
const { executeQuery } = require("../../db");

function toNumberOrNull(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function round2(n) {
    return Math.round(n * 100) / 100;
}

async function getNextDid(db) {
    const rows = await executeQuery(
        db,
        "SELECT IFNULL(MAX(did), 0) + 1 AS did FROM sat_monitoreo_db",
        []
    );
    return rows?.[0]?.did;
}

function pickProcesosData(respData) {
    if (!respData) return null;
    return respData.data || respData;
}

function sumOrNull(values) {
    const nums = values.filter((v) => Number.isFinite(v));
    if (!nums.length) return null;
    return nums.reduce((acc, n) => acc + n, 0);
}

function avgOrNull(values) {
    const nums = values.filter((v) => Number.isFinite(v));
    if (!nums.length) return null;
    return nums.reduce((acc, n) => acc + n, 0) / nums.length;
}

function maxOrNull(values) {
    const nums = values.filter((v) => Number.isFinite(v));
    return nums.length ? Math.max(...nums) : null;
}

async function monitoreoBd(
    db,
    services = [
        { key: "asignaciones", url: "http://asignaciones.lightdata.app/_sat/procesos" },
        { key: "backgps", url: "http://backgps2.lightdata.com.ar/_sat/procesos" },
        { key: "colecta", url: "http://colecta.lightdata.app/_sat/procesos" },
        { key: "aplanta", url: "http://aplanta.lightdata.app/_sat/procesos" },
        { key: "produccion", url: "http://altaenvios.lightdata.com.ar/_sat/procesos" },
        { key: "fulfillment", url: "http://ffull.lightdata.app/_sat/procesos" },
        // { key: "ffmobile", url: "http://ffmovil.lightdata.app/_sat/procesos" },
        { key: "callback", url: "http://whml.lightdata.app/_sat/procesos" },
        { key: "lightdatito", url: "http://node1.liit.com.ar/_sat/procesos" },
        //  { key: "websocket_mail", url: "https://notificaremails.lightdata.com.ar/_sat/procesos" },
        //{ key: "etiquetas", url: "http://printserver.lightdata.app/_sat/procesos" },
        { key: "estados", url: "http://serverestado.lightdata.app/_sat/procesos" },
        { key: "apimovil", url: "http://apimovil2.lightdata.app/_sat/procesos" },
    ],
    options = {}
) {
    const timeoutMs = options.timeoutMs ?? 2000;

    const did = await getNextDid(db);
    if (!did) throw new Error("No se pudo obtener did.");

    const insertSql = `
        INSERT INTO sat_monitoreo_db
        (did, servidor, endpoint, ok, codigoHttp, latenciaMs, error,
         procesos, total_segundos, promedio_segundos, max_segundos)
        VALUES
        (?,   ?,        ?,        ?,  ?,         ?,         ?,
         ?,        ?,              ?,                 ?)
    `;

    const results = [];

    for (const s of services) {
        const endpoint = s.url;

        let ok = 0;
        let codigoHttp = null;
        let latenciaMs = null;
        let error = null;

        let procesos = null;
        let totalSegundos = null;
        let promedioSegundos = null;
        let maxSegundos = null;

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

            const data = pickProcesosData(resp.data);

            if (data) {
                procesos = toNumberOrNull(data.procesos);
                totalSegundos = toNumberOrNull(data.total_segundos);
                promedioSegundos = toNumberOrNull(data.promedio_segundos);
                maxSegundos = toNumberOrNull(data.max_segundos);

                promedioSegundos = promedioSegundos === null ? null : round2(promedioSegundos);
            } else {
                ok = 0;
                error = "Respuesta vacia";
            }
        } catch (err) {
            ok = 0;
            error = err?.message ? err.message.slice(0, 255) : "ERROR";
            console.error(`[SAT-DB] ${s.key} ERROR`, error);
        }

        const values = [
            did,
            s.key,
            endpoint,
            ok,
            codigoHttp,
            latenciaMs,
            error,
            procesos,
            totalSegundos,
            promedioSegundos,
            maxSegundos,
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
            procesos,
            total_segundos: totalSegundos,
            promedio_segundos: promedioSegundos,
            max_segundos: maxSegundos,
        });
    }

    const didConjunto = await getNextDid(db);
    if (!didConjunto) throw new Error("No se pudo obtener didConjunto.");

    const procesosTotal = sumOrNull(results.map((r) => r.procesos));
    const totalSegundosTotal = sumOrNull(results.map((r) => r.total_segundos));
    const promedioSegundosAvg = avgOrNull(results.map((r) => r.promedio_segundos));
    const maxSegundosMax = maxOrNull(results.map((r) => r.max_segundos));
    const latenciaMax = maxOrNull(results.map((r) => r.latenciaMs));
    const okConjunto = results.some((r) => r.ok === 1) ? 1 : 0;

    const valuesConjunto = [
        didConjunto,
        "conjunto",
        "ALL",
        okConjunto,
        null,
        latenciaMax,
        null,
        procesosTotal,
        totalSegundosTotal,
        promedioSegundosAvg === null ? null : round2(promedioSegundosAvg),
        maxSegundosMax,
    ];

    await executeQuery(db, insertSql, valuesConjunto);

    return {
        did,
        didConjunto,
        results,
        conjunto: {
            did: didConjunto,
            servidor: "conjunto",
            endpoint: "ALL",
            ok: okConjunto,
            codigoHttp: null,
            latenciaMs: latenciaMax,
            error: null,
            procesos: valuesConjunto[7],
            total_segundos: valuesConjunto[8],
            promedio_segundos: valuesConjunto[9],
            max_segundos: valuesConjunto[10],
        },
    };
}

async function obtenerUltimoMonitoreoBd(db) {
    const rows = await executeQuery(
        db,
        `
        SELECT
            id,
            did,
            autofecha,
            servidor,
            endpoint,
            ok,
            codigoHttp,
            latenciaMs,
            error,
            procesos,
            total_segundos,
            promedio_segundos,
            max_segundos
        FROM sat_monitoreo_db
        WHERE did = (
            SELECT MAX(did)
            FROM sat_monitoreo_db
            WHERE servidor <> 'conjunto'
        )
        ORDER BY id ASC
        `,
        []
    );

    const conjunto = await executeQuery(
        db,
        `
        SELECT
            id,
            did,
            autofecha,
            servidor,
            endpoint,
            ok,
            codigoHttp,
            latenciaMs,
            error,
            procesos,
            total_segundos,
            promedio_segundos,
            max_segundos
        FROM sat_monitoreo_db
        WHERE servidor = 'conjunto'
        ORDER BY id DESC
        LIMIT 1
        `,
        []
    );

    return {
        todos: rows,
        conjunto: conjunto?.[0] || null,
    };
}

module.exports = { monitoreoBd, obtenerUltimoMonitoreoBd };
