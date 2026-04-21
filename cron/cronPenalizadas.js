const cron = require("node-cron");
const { processPenalizadas } = require("./penalizadas/penalizadaService");

const CRON_EXPR = process.env.CRON_PENALIZADAS_EXPR || "*/10 * * * *";

function startPenalizadasJob() {
    let running = false;

    cron.schedule(CRON_EXPR, async () => {
        if (running) {
            console.log("[PENALIZADAS] sigue corriendo, salteo ciclo");
            return;
        }

        running = true;

        try {
            const result = await processPenalizadas();
            console.log("[PENALIZADAS] ok", result);
        } catch (error) {
            console.error("[PENALIZADAS] error:", error?.message || error);
        } finally {
            running = false;
        }
    });

    console.log(`[PENALIZADAS] scheduler iniciado (${CRON_EXPR})`);
}

if (require.main === module) {
    const runOnce = process.argv.includes("--once");

    if (runOnce) {
        processPenalizadas()
            .then((result) => {
                console.log("[PENALIZADAS] corrida manual ok", result);
                process.exit(0);
            })
            .catch((error) => {
                console.error("[PENALIZADAS] corrida manual error:", error?.message || error);
                process.exit(1);
            });
    } else {
        startPenalizadasJob();
    }
}

module.exports = {
    startPenalizadasJob,
    processPenalizadas,
};
