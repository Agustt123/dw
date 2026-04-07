const cron = require("node-cron");
const { getConnectionLocalCdc, executeQuery } = require("../../db");
const MONITOREO_TIMEOUT_MS = 5000;

const { monitoreo } = require("./monitoreo");



function startMonitoreoJob() {
    let running = false;

    // cada 10 minutos: minuto 0,10,20,30,40,50
    cron.schedule("*/5 * * * *", async () => {
        if (running) {
            console.log("[MONITOREO JOB] sigue corriendo, salteo ciclo");
            return;
        }

        running = true;
        const db = await getConnectionLocalCdc();
        try {
            const r = await monitoreo(db);
            //  console.log("[MONITOREO JOB] ok:", r);
        } catch (err) {
            console.error("[MONITOREO JOB] error:", err.message);
        } finally {
            if (db?.release) try { db.release(); } catch { }
            running = false;
        }
    });

    //console.log("[MONITOREO JOB] scheduler iniciado (cada 10 min)");
}

async function obtenerMetricasUltimaCorrida() {
    let db;
    try {
        db = await getConnectionLocalCdc();

        // 1) último did
        const didRows = await executeQuery(
            db,
            "SELECT IFNULL(MAX(did), 0) AS did FROM sat_monitoreo_recursos",
            [],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
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
      WHERE 
       servidor = ?
      ORDER BY id DESC LIMIT 1
      `,
            ["conjunto"],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );

        return { rows };
    } catch (error) {
        console.error("❌ Error en obtenerMetricasUltimaCorrida:", error);
        throw {
            status: 500,
            response: { estado: false, error: -1, message: error?.message || String(error) },
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = { startMonitoreoJob, obtenerMetricasUltimaCorrida };
