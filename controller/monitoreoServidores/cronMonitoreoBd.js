const cron = require("node-cron");
const { getConnectionLocalCdc } = require("../../db");
const { monitoreoBd } = require("./monitoreoBd");

function startMonitoreoBd() {
    cron.schedule("*/1 * * * *", async () => {
        const db = await getConnectionLocalCdc();
        try {
            await monitoreoBd(db);
        } catch (err) {
            console.error("[MONITOREO BD JOB] error:", err.message);
        } finally {
            if (db?.release) try { db.release(); } catch { }
        }
    });

    console.log("[MONITOREO BD JOB] scheduler iniciado (cada 1 min)");
}

module.exports = { startMonitoreoBd };
