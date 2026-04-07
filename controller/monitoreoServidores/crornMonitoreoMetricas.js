const cron = require("node-cron");
const { getConnectionLocalCdc } = require("../../db");
const { monitoreoRecursos } = require("./monitoreoMetricas");





function startMonitoreoMetricas() {
    let running = false;

    // cada 10 minutos: minuto 0,10,20,30,40,50
    cron.schedule("*/1 * * * *", async () => {
        if (running) {
            console.log("[MONITOREO METRICAS JOB] sigue corriendo, salteo ciclo");
            return;
        }

        running = true;
        const db = await getConnectionLocalCdc();
        try {
            const r = await monitoreoRecursos(db);
            //   console.log("[MONITOREO JOB] ok:", r);
        } catch (err) {
            console.error("[MONITOREO JOB] error:", err.message);
        } finally {
            if (db?.release) try { db.release(); } catch { }
            running = false;
        }
    });

    console.log("[MONITOREO JOB] scheduler iniciado (cada 10 min)");
}

module.exports = { startMonitoreoMetricas };
