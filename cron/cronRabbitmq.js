const cron = require("node-cron");
const { collectRabbitSnapshot } = require("../controller/monitoreoServidores/rabbitmqMonitor");

const CRON_EXPR = process.env.CRON_RABBITMQ_EXPR || "*/5 * * * *";

function startRabbitmqJob() {
    let running = false;

    cron.schedule(CRON_EXPR, async () => {
        if (running) {
            console.log("[RABBITMQ] sigue corriendo, salteo ciclo");
            return;
        }

        running = true;

        try {
            const result = await collectRabbitSnapshot();
            console.log("[RABBITMQ] ok", {
                did: result?.did,
                queuesCount: result?.queuesCount,
                sev: result?.summary?.sev,
                resumen: result?.summary?.resumen,
            });
        } catch (error) {
            console.error("[RABBITMQ] error:", error?.message || error);
        } finally {
            running = false;
        }
    });

    console.log(`[RABBITMQ] scheduler iniciado (${CRON_EXPR})`);
}

if (require.main === module) {
    const runOnce = process.argv.includes("--once");

    if (runOnce) {
        collectRabbitSnapshot()
            .then((result) => {
                console.log("[RABBITMQ] corrida manual ok", {
                    did: result?.did,
                    queuesCount: result?.queuesCount,
                    sev: result?.summary?.sev,
                    resumen: result?.summary?.resumen,
                });
                process.exit(0);
            })
            .catch((error) => {
                console.error("[RABBITMQ] corrida manual error:", error?.message || error);
                process.exit(1);
            });
    } else {
        startRabbitmqJob();
    }
}

module.exports = {
    startRabbitmqJob,
    collectRabbitSnapshot,
};
