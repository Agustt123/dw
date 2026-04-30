// index.js
// - API corre en proceso principal
// - JOBS corren en un child process del mismo archivo
// - Logs de ambos aparecen en pm2 logs
// - No necesitás MODE ni 2 archivos

const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { fork } = require("child_process");
const { startMonitoreoBd } = require("./controller/monitoreoServidores/cronMonitoreoBd.js");
const { collectSatMetrics } = require("./satMetrics.js");

const isJobsChild = process.argv.includes("--jobs-child");

const PORT = 13000;

// =========================
// API (siempre en el proceso principal)
// =========================
function startApi() {
    const informeColecta = require("./route/informe-colecta.js");
    const cantidad = require("./route/cantidad.js");
    const monitorear = require("./route/monitoreo.js");
    const entregados = require("./route/entregados.js");

    const app = express();

    app.use(bodyParser.json({ limit: "50mb" }));
    app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
    app.use(cors({
        origin: "*",
        methods: ["GET", "POST", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization"]
    }));

    app.use("/informe-colecta", informeColecta);
    app.get("/ping", (req, res) => res.status(200).json({ estado: true, mensaje: "OK" }));
    app.get("/healthz", (req, res) => res.status(200).json({ ok: true, ts: Date.now() }));
    app.get("/_sat/metrics", async (req, res) => {
        try {
            const metrics = await collectSatMetrics({ serviceName: "dw" });
            return res.status(200).json(metrics);
        } catch (error) {
            console.error("❌ [API] Error en /_sat/metrics:", error?.message || error);
            return res.status(500).json({
                status: "error",
                service: "dw",
                error: error?.message || "No se pudieron obtener las métricas",
            });
        }
    });

    app.use("/cantidad", cantidad);
    app.use("/monitoreo", monitorear);
    app.use("/entregados", entregados);

    app.listen(PORT, () => console.log(`✅ [API] Servidor escuchando en http://localhost:${PORT}`));
}

// =========================
// Helpers
// =========================
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function withTimeout(promise, ms, label) {
    let t;
    const timeout = new Promise((_, rej) => {
        t = setTimeout(() => rej(new Error(`⏱️ Timeout: ${label} (${ms}ms)`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

// =========================
// JOBS (en child process)
// =========================
async function startJobs() {
    const { redisClient, getFromRedis, closeDWPool } = require("./db.js");
    const { sincronizarEnviosUnaVez } = require("./controller/controllerEnvio.js");
    const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");
    const { startMonitoreoJob } = require("./controller/monitoreoServidores/cronMonitoreo.js");
    const { startMonitoreoMetricas } = require("./controller/monitoreoServidores/crornMonitoreoMetricas.js");
    const { startRabbitmqJob } = require("./cron/cronRabbitmq.js");

    let parentWatchdog = null;

    function stopParentWatchdog() {
        if (!parentWatchdog) return;
        clearInterval(parentWatchdog);
        parentWatchdog = null;
    }

    function exitIfOrphaned(reason) {
        console.error(`[JOBS] Finalizando child: ${reason}`);
        stopParentWatchdog();
        process.exit(1);
    }

    function startParentWatchdog() {
        parentWatchdog = setInterval(() => {
            if (process.ppid === 1) {
                exitIfOrphaned("PPID=1, quedo huerfano");
                return;
            }
        }, 5000);

        if (typeof parentWatchdog.unref === "function") {
            parentWatchdog.unref();
        }
    }

    process.on("disconnect", () => {
        exitIfOrphaned("canal IPC desconectado");
    });

    let empresasDB = null;

    async function actualizarEmpresas() {
        try {
            empresasDB = (await getFromRedis("empresasData")) || null;
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

    // =========================
    // Locks
    // =========================
    let runningEnvios = false;

    let runningPend = false;

    async function runPendientesFixed() {

        if (runningPend) {
            console.log("⏭️ [JOBS] pendientesHoy sigue corriendo, salteo ciclo");
            return;
        }

        runningPend = true;

        try {
            await withTimeout(pendientesHoy(), 250000, "pendientesHoy");
            console.log("✅ [JOBS] pendientesHoy completado");
        } catch (e) {
            console.error("❌ [JOBS] Error en pendientesHoy:", e?.message || e);
        } finally {
            runningPend = false;
        }
    }

    async function loopPendientesHoy() {
        console.log("✅ [JOBS] Iniciando loop infinito de pendientesHoy...");

        while (true) {
            try {
                //await runPendientesFixed();
            } catch (e) {
                console.error("❌ [JOBS] Error en loopPendientesHoy:", e?.message || e);
            }

            await sleep(30 * 1000);
        }
    }

    async function runEnviosTick() {
        if (runningEnvios) {
            console.log("⏭️ [JOBS] Envios sigue corriendo, no arranco otro");
            return;
        }

        runningEnvios = true;
        console.log("🔁 [JOBS] Envios: iniciando sincronización...");

        const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

        await withTimeout(p, 200 * 1000, "sincronizarEnviosUnaVez")
            .then((stats) => {
                if (!stats) return;

                const mins = (stats.elapsedMs || 1) / 60000;
                const enviosMin = (stats.envios / mins).toFixed(1);

                console.log(
                    `✅ [JOBS] Envios: completada — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, ` +
                    `empresas=${stats.empresas}, tiempo=${(stats.elapsedMs / 1000).toFixed(1)}s, ≈ ${enviosMin} envíos/min`
                );
            })
            .catch((e) => {
                console.error("⏱️ [JOBS] Envios timeout:", e?.message || e);
            });

        runningEnvios = false;
    }

    function iniciarSchedulers() {
        // ENVÍOS (CDC corre en proceso separado cdc.js)
        runEnviosTick().catch(() => { });
        setInterval(() => {
            runEnviosTick().catch(() => { });
        }, 60 * 1000);
    }

    console.log("✅ [JOBS] Iniciando jobs...");
    startParentWatchdog();
    await actualizarEmpresas();

    iniciarSchedulers();

    // pendientesHoy corre aparte en loop infinito
    /* loopPendientesHoy().catch((e) => {
         console.error("❌ [JOBS] loopPendientesHoy murió:", e?.message || e);
     });
 */
    startMonitoreoJob();
    startMonitoreoBd();
    startMonitoreoMetricas();

    process.on("SIGINT", async () => {
        console.log("🛑 [JOBS] Cerrando...");
        try { await redisClient.disconnect(); } catch { }
        try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
        process.exit();
    });

    process.on("unhandledRejection", (reason) => {
        console.error("❌ [JOBS] unhandledRejection:", reason);
    });

    process.on("uncaughtException", (err) => {
        console.error("❌ [JOBS] uncaughtException:", err);
    });
}

// =========================
// Bootstrap único
// =========================
(async () => {
    try {
        if (isJobsChild) {
            // child process: solo jobs
            await startJobs();
            return;
        }

        // proceso principal: API + spawnea jobs
        startApi();

        function spawnJobsChild() {
            const child = fork(__filename, ["--jobs-child"], {
                stdio: ["inherit", "inherit", "inherit", "ipc"],
                env: process.env,
            });

            child.on("exit", (code, signal) => {
                console.error(`❌ [JOBS] Proceso hijo terminó (code=${code}, signal=${signal}). Lo reinicio...`);
                setTimeout(() => {
                    spawnJobsChild();
                }, 2000);
            });

            return child;
        }

        const child = spawnJobsChild();

        process.on("SIGINT", () => {
            console.log("🛑 [API] Cerrando...");
            try { child.kill("SIGINT"); } catch { }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => {
            console.error("❌ [API] unhandledRejection:", reason);
        });

        process.on("uncaughtException", (err) => {
            console.error("❌ [API] uncaughtException:", err);
        });
    } catch (err) {
        console.error("❌ Error al iniciar:", err?.message || err);
    }
})();

