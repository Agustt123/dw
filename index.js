// index.js
// - API corre en proceso principal
// - JOBS corren en un child process del mismo archivo
// - Logs de ambos aparecen en pm2 logs
// - No necesitás MODE ni 2 archivos

const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { fork } = require("child_process");

const isJobsChild = process.argv.includes("--jobs-child");

const PORT = 13000;

// =========================
// API (siempre en el proceso principal)
// =========================
function startApi() {
    const informeColecta = require("./route/informe-colecta.js");
    const cantidad = require("./route/cantidad.js");
    const monitorear = require("./route/monitoreo.js");

    const app = express();

    app.use(bodyParser.json({ limit: "50mb" }));
    app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
    app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"], allowedHeaders: ["Content-Type", "Authorization"] }));

    app.use("/informe-colecta", informeColecta);
    app.get("/ping", (req, res) => res.status(200).json({ estado: true, mensaje: "OK" }));
    app.get("/healthz", (req, res) => res.status(200).json({ ok: true, ts: Date.now() }));

    app.use("/cantidad", cantidad);
    app.use("/monitoreo", monitorear);

    app.listen(PORT, () => console.log(`✅ [API] Servidor escuchando en http://localhost:${PORT}`));
}

// =========================
// JOBS (en child process)
// =========================
function withTimeout(promise, ms, label) {
    let t;
    const timeout = new Promise((_, rej) => {
        t = setTimeout(() => rej(new Error(`⏱️ Timeout: ${label} (${ms}ms)`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

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
        } catch (e) {
            console.error("❌ [JOBS] Error al actualizar empresas desde Redis:", e?.message || e);
            empresasDB = null;
        }
    }

    function obtenerDidOwners() {
        if (!empresasDB) return [];
        if (typeof empresasDB === "string") {
            try { empresasDB = JSON.parse(empresasDB); } catch { return []; }
        }
        if (typeof empresasDB === "object" && !Array.isArray(empresasDB)) {
            return Object.keys(empresasDB).map(x => parseInt(x, 10)).filter(n => !isNaN(n));
        }
        return [];
    }

    // =========================
    // CDC
    // =========================
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
                await withTimeout(EnviarcdAsignacion(didOwner), 200000, `CDC asignacion ${didOwner}`);
                await withTimeout(EnviarcdcEstado(didOwner), 200000, `CDC estado ${didOwner}`);
            } catch (e) {
                console.error(`❌ [JOBS] Error CDC empresa ${didOwner}:`, e?.message || e);
            }
        }
    }

    // =========================
    // Locks
    // =========================
    let runningEnvios = false;

    let runningCdc = false;
    let cdcPending = false;

    let runningPend = false;

    async function runCdcSafely() {
        if (runningCdc) { cdcPending = true; return; }
        if (runningEnvios) { cdcPending = true; return; }

        runningCdc = true;
        try {
            do {
                cdcPending = false;
                console.log("🔁 [JOBS] CDC: iniciando...");
                await correrCdcUnaVez();
                console.log("✅ [JOBS] CDC: completado");

                if (runningEnvios) { cdcPending = true; break; }
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
            // Importante: timeout < intervalo para ritmo sano
            await withTimeout(pendientesHoy(), 25000, "pendientesHoy");
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
        console.log("🔁 [JOBS] Envios: iniciando sincronización...");

        const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

        // FIX CLAVE: NO hacemos await del promise original si se colgó
        await withTimeout(p, 55 * 1000, "sincronizarEnviosUnaVez")
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

        // Liberamos lock siempre
        runningEnvios = false;

        // Si CDC quedó pendiente mientras Envios corría, arrancalo ahora
        if (cdcPending) runCdcSafely().catch(() => { });
    }

    function iniciarSchedulers() {
        // ENVÍOS + CDC (cada 120s)
        setInterval(() => {
            runEnviosTick().catch(() => { });
            runCdcSafely().catch(() => { });
        }, 120 * 1000);

        // Pendientes cada 30s (antes estaba 15s)
        setInterval(() => {
            runPendientesFixed().catch(() => { });
        }, 30 * 1000);
    }

    console.log("✅ [JOBS] Iniciando jobs...");
    await actualizarEmpresas();

    iniciarSchedulers();
    startMonitoreoJob();
    startMonitoreoMetricas();

    process.on("SIGINT", async () => {
        console.log("🛑 [JOBS] Cerrando...");
        try { await redisClient.disconnect(); } catch { }
        try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
        process.exit();
    });

    process.on("unhandledRejection", (reason) => console.error("❌ [JOBS] unhandledRejection:", reason));
    process.on("uncaughtException", (err) => console.error("❌ [JOBS] uncaughtException:", err));
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

        // Spawn de jobs desde el mismo archivo
        const child = fork(__filename, ["--jobs-child"], {
            stdio: "inherit", // IMPORTANTÍSIMO: logs del child salen en pm2 logs
            env: process.env,
        });

        child.on("exit", (code, signal) => {
            console.error(`❌ [JOBS] Proceso hijo terminó (code=${code}, signal=${signal}). Lo reinicio...`);
            // reinicio simple
            setTimeout(() => {
                fork(__filename, ["--jobs-child"], { stdio: "inherit", env: process.env });
            }, 2000);
        });

        process.on("SIGINT", () => {
            console.log("🛑 [API] Cerrando...");
            try { child.kill("SIGINT"); } catch { }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => console.error("❌ [API] unhandledRejection:", reason));
        process.on("uncaughtException", (err) => console.error("❌ [API] uncaughtException:", err));
    } catch (err) {
        console.error("❌ Error al iniciar:", err?.message || err);
    }
})();
