// index.js
// Modo backfill / toma historica
// - API corre en proceso principal
// - JOBS corren en child process
// - Si se pasa --backfill, corre una sola toma historica y termina
// - Sin schedulers, sin pendientes, sin monitoreo
// - CDC con concurrencia controlada

const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { fork } = require("child_process");

const isJobsChild = process.argv.includes("--jobs-child");
const isBackfill = process.argv.includes("--backfill");

const PORT = 13000;

// Timeouts especiales para historico
const ENVIO_TIMEOUT = isBackfill ? 30 * 60 * 1000 : 200 * 1000; // 30 min en historico
const CDC_TIMEOUT = isBackfill ? 15 * 60 * 1000 : 500 * 1000;   // 15 min por tarea CDC
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
        mode: isBackfill ? "backfill" : "normal",
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
// JOBS
// =========================
async function startJobs() {
    const { redisClient, getFromRedis, closeDWPool } = require("./db.js");
    const { sincronizarEnviosUnaVez } = require("./controller/controllerEnvio.js");
    const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/procesarCDC/checkcdc2.js");
    const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");
    const { startMonitoreoJob } = require("./controller/monitoreoServidores/cronMonitoreo.js");
    const { startMonitoreoMetricas } = require("./controller/monitoreoServidores/crornMonitoreoMetricas.js");

    let empresasDB = null;

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

    async function correrCdcBackfill() {
        await actualizarEmpresas();
        const didOwners = obtenerDidOwners();

        if (!didOwners.length) {
            console.log("⚠️ [BACKFILL] No se encontraron empresas para CDC.");
            return;
        }

        console.log(
            `🚀 [BACKFILL] CDC historico para ${didOwners.length} empresas con concurrency=${CDC_CONCURRENCY}`
        );

        const startedAt = Date.now();

        await runWithConcurrency(didOwners, CDC_CONCURRENCY, procesarEmpresaCdc);

        const elapsedMs = Date.now() - startedAt;
        console.log(`✅ [BACKFILL] CDC finalizado en ${(elapsedMs / 1000).toFixed(1)}s`);
    }

    async function runEnviosBackfill() {
        console.log("🚀 [BACKFILL] Envios: iniciando sincronizacion historica...");

        const startedAt = Date.now();

        try {
            const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

            const stats = await withTimeout(
                p,
                ENVIO_TIMEOUT,
                "sincronizarEnviosUnaVez"
            );

            const elapsedMs = Date.now() - startedAt;

            if (!stats) {
                console.log(`✅ [BACKFILL] Envios finalizado en ${(elapsedMs / 1000).toFixed(1)}s`);
                return;
            }

            const mins = Math.max((stats.elapsedMs || elapsedMs || 1) / 60000, 1 / 60000);
            const enviosMin = Number((stats.envios / mins).toFixed(1));

            console.log(
                `✅ [BACKFILL] Envios completado — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, empresas=${stats.empresas}, tiempo=${((stats.elapsedMs || elapsedMs) / 1000).toFixed(1)}s, ≈ ${enviosMin} envios/min`
            );
        } catch (e) {
            console.error("❌ [BACKFILL] Error en envios historicos:", e?.message || e);
        }
    }

    async function runBackfill() {
        console.log("========================================");
        console.log("🚀 [BACKFILL] Iniciando toma historica");
        console.log(`🧩 [BACKFILL] ENVIO_TIMEOUT=${ENVIO_TIMEOUT}ms`);
        console.log(`🧩 [BACKFILL] CDC_TIMEOUT=${CDC_TIMEOUT}ms`);
        console.log(`🧩 [BACKFILL] CDC_CONCURRENCY=${CDC_CONCURRENCY}`);
        console.log("========================================");

        const globalStart = Date.now();

        await actualizarEmpresas();

        // 1. Primero envios
        await runEnviosBackfill();

        // 2. Despues CDC
        await correrCdcBackfill();

        const elapsedMs = Date.now() - globalStart;
        console.log(`✅ [BACKFILL] Toma historica completa en ${(elapsedMs / 1000).toFixed(1)}s`);
    }

    async function startNormalMode() {
        let runningEnvios = false;
        let runningCdc = false;
        let cdcPending = false;
        let runningPend = false;

        async function correrCdcUnaVez() {
            await actualizarEmpresas();
            const didOwners = obtenerDidOwners();

            if (!didOwners.length) {
                console.log("⚠️ [JOBS] No se encontraron empresas para CDC.");
                return;
            }

            console.log(`🔁 [JOBS] CDC para ${didOwners.length} empresas...`);

            for (const didOwner of didOwners) {
                try {
                    await withTimeout(
                        Promise.resolve().then(() => EnviarcdAsignacion(didOwner)),
                        CDC_TIMEOUT,
                        `CDC asignacion ${didOwner}`
                    );

                    await withTimeout(
                        Promise.resolve().then(() => EnviarcdcEstado(didOwner)),
                        CDC_TIMEOUT,
                        `CDC estado ${didOwner}`
                    );
                } catch (e) {
                    console.error(`❌ [JOBS] Error CDC empresa ${didOwner}:`, e?.message || e);
                }
            }
        }

        async function runCdcSafely() {
            if (runningCdc) {
                cdcPending = true;
                return;
            }

            if (runningEnvios) {
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

                    if (runningEnvios) {
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
            if (runningPend) {
                console.log("⏭️ [JOBS] pendientesHoy sigue corriendo, salteo tick");
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
            if (runningEnvios) {
                console.log("⏭️ [JOBS] Envios sigue corriendo, no arranco otro");
                return;
            }

            runningEnvios = true;
            console.log("🔁 [JOBS] Envios: iniciando sincronizacion...");

            try {
                const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

                const stats = await withTimeout(
                    p,
                    200 * 1000,
                    "sincronizarEnviosUnaVez"
                );

                if (stats) {
                    const mins = (stats.elapsedMs || 1) / 60000;
                    const enviosMin = (stats.envios / mins).toFixed(1);

                    console.log(
                        `✅ [JOBS] Envios: completada — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, empresas=${stats.empresas}, tiempo=${(stats.elapsedMs / 1000).toFixed(1)}s, ≈ ${enviosMin} envios/min`
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

        console.log("✅ [JOBS] Iniciando jobs en modo normal...");
        await actualizarEmpresas();

        iniciarSchedulers();
        startMonitoreoJob();
        startMonitoreoMetricas();
    }

    console.log(`✅ [JOBS] Iniciando jobs en modo ${isBackfill ? "BACKFILL" : "NORMAL"}...`);

    if (isBackfill) {
        await runBackfill();

        console.log("🛑 [BACKFILL] Finalizado. Cerrando proceso de jobs...");
        try { await redisClient.disconnect(); } catch { }
        try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
        process.exit(0);
        return;
    }

    await startNormalMode();

    process.on("SIGINT", async () => {
        console.log("🛑 [JOBS] Cerrando...");
        try { await redisClient.disconnect(); } catch { }
        try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
        process.exit(0);
    });

    process.on("unhandledRejection", (reason) => {
        console.error("❌ [JOBS] unhandledRejection:", reason);
    });

    process.on("uncaughtException", (err) => {
        console.error("❌ [JOBS] uncaughtException:", err);
    });
}

// =========================
// Bootstrap
// =========================
(async () => {
    try {
        if (isJobsChild) {
            await startJobs();
            return;
        }

        startApi();

        const childArgs = ["--jobs-child"];
        if (isBackfill) childArgs.push("--backfill");

        let child = fork(__filename, childArgs, {
            stdio: "inherit",
            env: process.env,
        });

        child.on("exit", (code, signal) => {
            console.error(`❌ [JOBS] Proceso hijo termino (code=${code}, signal=${signal})`);

            // En historico NO reiniciar automaticamente
            if (isBackfill) {
                console.log("✅ [BACKFILL] Child finalizado, no se reinicia.");
                return;
            }

            console.error("🔁 [JOBS] Lo reinicio en 2s...");
            setTimeout(() => {
                child = fork(__filename, ["--jobs-child"], {
                    stdio: "inherit",
                    env: process.env,
                });
            }, 2000);
        });

        process.on("SIGINT", () => {
            console.log("🛑 [API] Cerrando...");
            try { child.kill("SIGINT"); } catch { }
            process.exit(0);
        });

        process.on("unhandledRejection", (reason) => {
            console.error("❌ [API] unhandledRejection:", reason);
        });

        process.on("uncaughtException", (err) => {
            console.error("❌ [API] uncaughtException:", err);
        });
    } catch (err) {
        console.error("❌ Error al iniciar:", err?.message || err);
        process.exit(1);
    }
})();//