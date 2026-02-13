const cron = require("node-cron");
const { getConnectionLocalCdc } = require("../../db");

const { monitoreo } = require("./monitoreo");



function startMonitoreoJob() {
    // cada 10 minutos: minuto 0,10,20,30,40,50
    cron.schedule("*/5 * * * *", async () => {
        const db = await getConnectionLocalCdc();
        try {
            const r = await monitoreo(db);
            console.log("[MONITOREO JOB] ok:", r);
        } catch (err) {
            console.error("[MONITOREO JOB] error:", err.message);
        } finally {
            if (db?.release) try { db.release(); } catch { }
        }
    });

    console.log("[MONITOREO JOB] scheduler iniciado (cada 10 min)");
}

async function obtenerMetricasUltimaCorrida() {
    const db = await getConnectionLocalCdc();

    // 1) último did
    const didRows = await executeQuery(
        db,
        "SELECT IFNULL(MAX(did), 0) AS did FROM sat_monitoreo_recursos",
        []
    );

    const did = Number(didRows?.[0]?.did ?? 0);
    if (!did) {
        return { did: null, rows: [] };
    }

    // 2) todas las filas de ese did
    const rows = await executeQuery(
        db,
        `
    SELECT
      did,
      servidor,
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
      cpuProceso
    FROM sat_monitoreo_recursos
    WHERE did = ?
    ORDER BY servidor ASC
    `,
        [did]
    );

    return { did, rows };
}

module.exports = { startMonitoreoJob, obtenerMetricasUltimaCorrida };
