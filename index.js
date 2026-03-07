const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const isBackfillOnly = process.argv.includes("--backfill-only");

const PORT = 13000;

// Timeouts
const ENVIO_TIMEOUT = 30 * 60 * 1000;
const CDC_TIMEOUT = 15 * 60 * 1000;
const PENDIENTES_TIMEOUT = 60 * 60 * 1000;
const CDC_CONCURRENCY = parseInt(process.env.CDC_CONCURRENCY || "3", 10);

// =========================
// API
// =========================
function startApi() {
    const informeColecta = require("./route/informe-colecta.js");
    const cantidad = require("./route/cantidad.js");
    const monitorear = require("./route/monitoreo.js");

    const app = express();

    app.use(bodyParser.json({ limit: "50mb" }));
    app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
    app.use(cors({
        origin: "*",
        methods: ["GET", "POST", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization"]
    }));

    app.use("/informe-colecta", informeColecta);
    app.use("/cantidad", cantidad);
    app.use("/monitoreo", monitorear);

    app.get("/ping", (req, res) => res.status(200).json({ estado: true, mensaje: "OK" }));
    app.get("/healthz", (req, res) => res.status(200).json({
        ok: true,
        mode: isBackfillOnly ? "backfill-only" : "api+jobs",
        ts: Date.now()
    }));

    app.listen(PORT, () => {
        console.log(`✅ [API] Servidor escuchando en http://localhost:${PORT}`);
    });
}

// =========================
// Helpers
// =========================
function withTimeout(promise, ms, label) {
    let t;
    const timeout = new Promise((_, rej) => {
        t = setTimeout(() => rej(new Error(`⏱️ Timeout: ${label} (${ms}ms)`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

async function runWithConcurrency(items, limit, worker) {
    const queue = [...items];

    const workers = Array.from({ length: limit }, async (_, idx) => {
        while (queue.length > 0) {
            const item = queue.shift();
            if (item === undefined) return;

            try {
                await worker(item, idx + 1);
            } catch (e) {
                console.error(`❌ [POOL ${idx + 1}] Error procesando item ${item}:`, e?.message || e);
            }
        }
    });

    await Promise.all(workers);
}

// =========================
// Jobs
// =========================
async function buildJobsContext() {
    const db = require("./db.js");
    const { sincronizarEnviosUnaVez } = require("./controller/controllerEnvio.js");
    const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/procesarCDC/checkcdc2.js");
    const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");
    const { startMonitoreoJob } = require("./controller/monitoreoServidores/cronMonitoreo.js");
    const { startMonitoreoMetricas } = require("./controller/monitoreoServidores/crornMonitoreoMetricas.js");

    return {
        ...db,
        sincronizarEnviosUnaVez,
        EnviarcdAsignacion,
        EnviarcdcEstado,
        pendientesHoy,
        startMonitoreoJob,
        startMonitoreoMetricas
    };
}

async function main() {
    const {
        redisClient,
        getFromRedis,
        closeDWPool,
        sincronizarEnviosUnaVez,
        EnviarcdAsignacion,
        EnviarcdcEstado,
        pendientesHoy,
        startMonitoreoJob,
        startMonitoreoMetricas
    } = await buildJobsContext();

    let empresasDB = null;
    let backfillRunning = false;
    let runningEnvios = false;
    let runningCdc = false;
    let cdcPending = false;
    let runningPend = false;

    async function actualizarEmpresas() {
        try {
            empresasDB = (await getFromRedis("empresasData")) || null;
            console.log("✅ [JOBS] Empresas actualizadas desde Redis");
        } catch (e) {
            console.error("❌ [JOBS] Error al actualizar empresas desde Redis:", e?.message || e);
            empresasDB = null;
        }
    }

    function obtenerDidOwners() {
        if (!empresasDB) return [];

        if (typeof empresasDB === "string") {
            try {
                empresasDB = JSON.parse(empresasDB);
            } catch {
                return [];
            }
        }

        if (typeof empresasDB === "object" && !Array.isArray(empresasDB)) {
            return Object.keys(empresasDB)
                .map(x => parseInt(x, 10))
                .filter(n => !isNaN(n));
        }

        return [];
    }

    async function procesarEmpresaCdc(didOwner, workerId) {
        try {
            console.log(`🔁 [CDC][W${workerId}] Empresa ${didOwner}: asignacion...`);
            await withTimeout(
                Promise.resolve().then(() => EnviarcdAsignacion(didOwner)),
                CDC_TIMEOUT,
                `CDC asignacion ${didOwner}`
            );

            console.log(`🔁 [CDC][W${workerId}] Empresa ${didOwner}: estado...`);
            await withTimeout(
                Promise.resolve().then(() => EnviarcdcEstado(didOwner)),
                CDC_TIMEOUT,
                `CDC estado ${didOwner}`
            );

            console.log(`✅ [CDC][W${workerId}] Empresa ${didOwner}: OK`);
        } catch (e) {
            console.error(`❌ [CDC][W${workerId}] Empresa ${didOwner}:`, e?.message || e);
        }
    }

    async function correrCdcUnaVez() {
        await actualizarEmpresas();
        const didOwners = obtenerDidOwners();

        if (!didOwners.length) {
            console.log("⚠️ [CDC] No se encontraron empresas.");
            return;
        }

        console.log(`🚀 [CDC] Procesando ${didOwners.length} empresas con concurrency=${CDC_CONCURRENCY}`);
        const startedAt = Date.now();

        await runWithConcurrency(didOwners, CDC_CONCURRENCY, procesarEmpresaCdc);

        const elapsedMs = Date.now() - startedAt;
        console.log(`✅ [CDC] Finalizado en ${(elapsedMs / 1000).toFixed(1)}s`);
    }

    async function runEnviosUnaVez() {
        console.log("🚀 [ENVIOS] Iniciando sincronizacion...");

        const startedAt = Date.now();

        try {
            const stats = await withTimeout(
                Promise.resolve().then(() => sincronizarEnviosUnaVez()),
                ENVIO_TIMEOUT,
                "sincronizarEnviosUnaVez"
            );

            const elapsedMs = Date.now() - startedAt;

            if (!stats) {
                console.log(`✅ [ENVIOS] Finalizado en ${(elapsedMs / 1000).toFixed(1)}s`);
                return;
            }

            const mins = Math.max((stats.elapsedMs || elapsedMs || 1) / 60000, 1 / 60000);
            const enviosMin = Number((stats.envios / mins).toFixed(1));

            console.log(
                `✅ [ENVIOS] Completado — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, empresas=${stats.empresas}, tiempo=${((stats.elapsedMs || elapsedMs) / 1000).toFixed(1)}s, ≈ ${enviosMin} envios/min`
            );
        } catch (e) {
            console.error("❌ [ENVIOS] Error:", e?.message || e);
        }
    }

    async function runPendientesUnaVez() {
        console.log("🚀 [PENDIENTES] Iniciando pendientesHoy...");

        const startedAt = Date.now();

        try {
            const result = await withTimeout(
                Promise.resolve().then(() => pendientesHoy()),
                PENDIENTES_TIMEOUT,
                "pendientesHoy"
            );

            const elapsedMs = Date.now() - startedAt;
            console.log(`✅ [PENDIENTES] Finalizado en ${(elapsedMs / 1000).toFixed(1)}s`, result || "");
        } catch (e) {
            console.error("❌ [PENDIENTES] Error:", e?.message || e);
        }
    }

    async function runBackfillUnaVez() {
        if (backfillRunning) {
            console.log("⏭️ [BACKFILL] Ya está corriendo, salteo.");
            return;
        }

        backfillRunning = true;

        try {
            console.log("========================================");
            console.log("🚀 [BACKFILL] Iniciando llenado histórico");
            console.log(`🧩 [BACKFILL] ENVIO_TIMEOUT=${ENVIO_TIMEOUT}ms`);
            console.log(`🧩 [BACKFILL] CDC_TIMEOUT=${CDC_TIMEOUT}ms`);
            console.log(`🧩 [BACKFILL] PENDIENTES_TIMEOUT=${PENDIENTES_TIMEOUT}ms`);
            console.log(`🧩 [BACKFILL] CDC_CONCURRENCY=${CDC_CONCURRENCY}`);
            console.log("🧩 [BACKFILL] Orden: ENVIOS -> CDC -> PENDIENTES");
            console.log("========================================");

            const startedAt = Date.now();

            await runEnviosUnaVez();
            await correrCdcUnaVez();
            await runPendientesUnaVez();

            const elapsedMs = Date.now() - startedAt;
            console.log(`✅ [BACKFILL] Completo en ${(elapsedMs / 1000).toFixed(1)}s`);
        } catch (e) {
            console.error("❌ [BACKFILL] Error:", e?.message || e);
        } finally {
            backfillRunning = false;
        }
    }

    async function runCdcSafely() {
        if (runningCdc || runningEnvios || backfillRunning) {
            cdcPending = true;
            return;
        }

        runningCdc = true;

        try {
            do {
                cdcPending = false;
                console.log("🔁 [JOBS] CDC: iniciando...");
                await correrCdcUnaVez();
                console.log("✅ [JOBS] CDC: completado");

                if (runningEnvios || backfillRunning) {
                    cdcPending = true;
                    break;
                }
            } while (cdcPending);
        } catch (e) {
            console.error("❌ [JOBS] Error en CDC:", e?.message || e);
        } finally {
            runningCdc = false;
        }
    }

    async function runPendientesFixed() {
        if (runningPend || backfillRunning) {
            console.log("⏭️ [JOBS] pendientesHoy sigue corriendo o backfill activo, salteo tick");
            return;
        }

        runningPend = true;

        try {
            await withTimeout(
                Promise.resolve().then(() => pendientesHoy()),
                25000,
                "pendientesHoy"
            );
        } catch (e) {
            console.error("❌ [JOBS] Error en pendientesHoy:", e?.message || e);
        } finally {
            runningPend = false;
        }
    }

    async function runEnviosTick() {
        if (runningEnvios || backfillRunning) {
            console.log("⏭️ [JOBS] Envios sigue corriendo o backfill activo, no arranco otro");
            return;
        }

        runningEnvios = true;
        console.log("🔁 [JOBS] Envios: iniciando sincronizacion...");

        try {
            const stats = await withTimeout(
                Promise.resolve().then(() => sincronizarEnviosUnaVez()),
                200 * 1000,
                "sincronizarEnviosUnaVez"
            );

            if (stats) {
                const mins = Math.max((stats.elapsedMs || 1) / 60000, 1 / 60000);
                const enviosMin = (stats.envios / mins).toFixed(1);

                console.log(
                    `✅ [JOBS] Envios: completada — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, empresas=${stats.empresas}, tiempo=${((stats.elapsedMs || 0) / 1000).toFixed(1)}s, ≈ ${enviosMin} envios/min`
                );
            }
        } catch (e) {
            console.error("⏱️ [JOBS] Envios timeout/error:", e?.message || e);
        } finally {
            runningEnvios = false;
        }

        if (cdcPending) {
            runCdcSafely().catch(() => { });
        }
    }

    function iniciarSchedulers() {
        setInterval(() => {
            runEnviosTick().catch(() => { });
            runCdcSafely().catch(() => { });
        }, 60 * 1000);

        setInterval(() => {
            runPendientesFixed().catch(() => { });
        }, 30 * 1000);
    }

    async function shutdown() {
        console.log("🛑 Cerrando proceso...");
        try { await redisClient.disconnect(); } catch { }
        try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
        process.exit(0);
    }

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    process.on("unhandledRejection", (reason) => {
        console.error("❌ unhandledRejection:", reason);
    });

    process.on("uncaughtException", (err) => {
        console.error("❌ uncaughtException:", err);
    });

    if (isBackfillOnly) {
        await runBackfillUnaVez();
        await shutdown();
        return;
    }

    startApi();

    console.log("✅ [APP] Iniciando modo API + jobs + backfill inicial");
    await actualizarEmpresas();

    // Arranca el backfill una sola vez, sin bloquear el proceso de API
    setTimeout(() => {
        runBackfillUnaVez().catch((e) => {
            console.error("❌ [BACKFILL] Falló al iniciar:", e?.message || e);
        });
    }, 2000);

    // Jobs normales
    iniciarSchedulers();
    startMonitoreoJob();
    startMonitoreoMetricas();
}

main().catch((err) => {
    console.error("❌ Error fatal al iniciar:", err?.message || err);
    process.exit(1);
});