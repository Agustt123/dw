const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");

async function main() {
    try {
        console.log("🚀 [BACKFILL] Iniciando pendientesHoy standalone...");
        const result = await pendientesHoy();
        console.log("✅ [BACKFILL] Finalizado:", result);
        process.exit(0);
    } catch (err) {
        console.error("❌ [BACKFILL] Error:", err?.message || err);
        process.exit(1);
    }
}

process.on("unhandledRejection", (reason) => {
    console.error("❌ [BACKFILL] unhandledRejection:", reason);
});

process.on("uncaughtException", (err) => {
    console.error("❌ [BACKFILL] uncaughtException:", err);
});

main();