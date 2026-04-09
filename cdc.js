// cdc.js
// Proceso independiente para procesar CDC (Change Data Capture)
// Corre cada minuto independientemente de envios

const { redisClient } = require("./db.js");
const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/procesarCDC/checkcdc2.js");

const LOOP_PAUSE_MS = Number(process.env.CDC_LOOP_PAUSE_MS || 1 * 1000); // 1 segundo default
const CDC_TIMEOUT_MS = Number(process.env.CDC_TIMEOUT_MS || 500 * 1000); // 500 segundos default

const EMPRESAS_BLOQUEADAS = new Set([275, 276, 345]);

let running = false;
let stopRequested = false;

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, timeoutMs, label) {
    let timer = null;

    const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(() => {
            reject(new Error(`⏱️ ${label} timeout despues de ${timeoutMs} ms`));
        }, timeoutMs);
    });

    return Promise.race([promise, timeoutPromise]).finally(() => {
        if (timer) clearTimeout(timer);
    });
}

async function obtenerEmpresas() {
    try {
        const empresasStr = await redisClient.get("empresasData");
        if (!empresasStr) {
            console.warn("⚠️ [CDC] No hay 'empresasData' en Redis");
            return [];
        }

        const empresasData = typeof empresasStr === "string" ? JSON.parse(empresasStr) : empresasStr;
        const didOwners = Object.keys(empresasData)
            .map(k => parseInt(k, 10))
            .filter(n => !isNaN(n));

        return didOwners;
    } catch (err) {
        console.error("❌ [CDC] Error obteniendo empresas:", err?.message || err);
        return [];
    }
}

async function procesarCdcParaEmpresa() {
    try {
        const startAt = Date.now();

        // Ejecutar ambos en paralelo si es posible
        await Promise.all([
            withTimeout(
                EnviarcdAsignacion(),
                CDC_TIMEOUT_MS,
                `[CDC] Asignacion`
            ),
            withTimeout(
                EnviarcdcEstado(),
                CDC_TIMEOUT_MS,
                `[CDC] Estado`
            )
        ]);

        const elapsed = Date.now() - startAt;
        console.log(`✅ [CDC] Procesamiento global completado (${elapsed}ms)`);
    } catch (err) {
        console.error(`❌ [CDC] Error en procesamiento global:`, err?.message || err);
    }
}

async function runCdcTick() {
    if (running) {
        console.log("⏭️ [CDC] CDC sigue corriendo, salteo tick");
        return;
    }

    running = true;
    const startedAt = Date.now();

    try {
        console.log("🔁 [CDC] Iniciando procesamiento de CDC...");

        await procesarCdcParaEmpresa();

        const elapsed = Date.now() - startedAt;

        console.log(`✅ [CDC] Tick completado: tiempo=${(elapsed / 1000).toFixed(1)}s`);
    } catch (err) {
        const elapsed = Date.now() - startedAt;
        console.error("❌ [CDC] Error en tick:", {
            elapsed,
            message: err?.message || String(err),
        });
    } finally {
        running = false;
    }
}

async function main() {
    console.log("🚀 [CDC] Iniciando procesamiento de CDC...", {
        LOOP_PAUSE_MS: `${LOOP_PAUSE_MS / 1000}s`,
        CDC_TIMEOUT_MS: `${CDC_TIMEOUT_MS / 1000}s`,
    });

    let tick = 0;
    while (!stopRequested) {
        tick += 1;
        console.log(`🧭 [CDC] tick=${tick}`);
        await runCdcTick();

        if (stopRequested) break;

        console.log(`😴 [CDC] Esperando ${LOOP_PAUSE_MS / 1000}s para proximo tick...`);
        await sleep(LOOP_PAUSE_MS);
    }

    console.log("🛑 [CDC] Loop detenido");
    process.exit(0);
}

function requestStop(signal) {
    console.log(`🛑 [CDC] Recibi ${signal}, deteniendo despues del tick actual...`);
    stopRequested = true;
}

process.on("SIGINT", () => requestStop("SIGINT"));
process.on("SIGTERM", () => requestStop("SIGTERM"));

process.on("unhandledRejection", (reason) => {
    console.error("❌ [CDC] unhandledRejection:", reason);
});

process.on("uncaughtException", (err) => {
    console.error("❌ [CDC] uncaughtException:", err);
});

main().catch((err) => {
    console.error("❌ [CDC] Error fatal:", err?.message || err);
    process.exit(1);
});
