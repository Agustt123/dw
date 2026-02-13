// index.js (TODO en un solo archivo, pero corriendo API y JOBS separados por modo)
// Uso:
//   MODE=api  node index.js    -> levanta solo Express
//   MODE=jobs node index.js    -> levanta solo schedulers
// Recomendado en PM2:
//   pm2 start index.js --name dw-api  --env MODE=api
//   pm2 start index.js --name dw-jobs --env MODE=jobs

const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

// Infra / DB
const { redisClient, getFromRedis, closeDWPool } = require("./db.js");

// Rutas API
const informeColecta = require("./route/informe-colecta.js");
const cantidad = require("./route/cantidad.js");
const monitorear = require("./route/monitoreo.js");

// Jobs / Controllers
const { sincronizarEnviosUnaVez } = require("./controller/controllerEnvio.js");
const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/procesarCDC/checkcdc2.js");
const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");
const { startMonitoreoJob } = require("./controller/monitoreoServidores/cronMonitoreo.js");
const { startMonitoreoMetricas } = require("./controller/monitoreoServidores/crornMonitoreoMetricas.js");

const MODE = (process.env.MODE || "api").toLowerCase(); // "api" | "jobs"
const PORT = Number(process.env.PORT || 13000);

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

function safeJsonParse(x) {
    if (typeof x !== "string") return x;
    try { return JSON.parse(x); } catch { return null; }
}

// =========================
// API
// =========================
function startApi() {
    const app = express();

    app.use(bodyParser.json({ limit: "50mb" }));
    app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
    app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"], allowedHeaders: ["Content-Type", "Authorization"] }));

    app.use("/informe-colecta", informeColecta);
    app.get("/ping", (req, res) => res.status(200).json({ estado: true, mensaje: "OK" }));
    app.get("/healthz", (req, res) => res.status(200).json({ ok: true, ts: Date.now() }));

    app.use("/cantidad", cantidad);
    app.use("/monitoreo", monitorear);

    app.listen(PORT, () => console.log(`✅ [API] escuchando en http://localhost:${PORT}`));
}

// =========================
// JOBS (separado del API por MODE)
// =========================
let empresasDB = null;

async function actualizarEmpresas() {
    try {
        empresasDB = (await getFromRedis("empresasData")) || null;
    } catch (e) {
        console.error("❌ Error al actualizar empresas desde Redis:", e?.message || e);
        empresasDB = null;
    }
}

function obtenerDidOwners() {
    if (!empresasDB) return [];
    empresasDB = safeJsonParse(empresasDB) || empresasDB;

    if (typeof empresasDB === "object" && empresasDB && !Array.isArray(empresasDB)) {
        return Object.keys(empresasDB)
            .map((x) => parseInt(x, 10))
            .filter((n) => Number.isFinite(n));
    }
    return [];
}

async function correrCdcUnaVez() {
    await actualizarEmpresas();
    const didOwners = obtenerDidOwners();
    if (!didOwners.length) {
        console.log("⚠️ No se encontraron empresas para CDC.");
        return;
    }

    console.log(`🔁 CDC para ${didOwners.length} empresas...`);

    for (const didOwner of didOwners) {
        try {
            await withTimeout(EnviarcdAsignacion(didOwner), 200000, `CDC asignacion ${didOwner}`);
            await withTimeout(EnviarcdcEstado(didOwner), 200000, `CDC estado ${didOwner}`);
        } catch (e) {
            console.error(`❌ Error CDC empresa ${didOwner}:`, e?.message || e);
        }
    }
}

// Locks
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
            console.log("🔁 CDC: iniciando...");
            await correrCdcUnaVez();
            console.log("✅ CDC: completado");

            if (runningEnvios) { cdcPending = true; break; }
        } while (cdcPending);
    } catch (e) {
        console.error("❌ Error en CDC:", e?.message || e);
    } finally {
        runningCdc = false;
    }
}

async function runPendientesFixed() {
    if (runningPend) {
        console.log("⏭️ pendientesHoy sigue corriendo, salteo tick");
        return;
    }

    runningPend = true;
    try {
        // timeout menor que el intervalo (30s)
        await withTimeout(pendientesHoy(), 25000, "pendientesHoy");
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e?.message || e);
    } finally {
        runningPend = false;
    }
}

async function runEnviosTick() {
    if (runningEnvios) {
        console.log("⏭️ Envios sigue corriendo, no arranco otro");
        return;
    }

    runningEnvios = true;
    console.log("🔁 Envios: iniciando sincronización...");

    const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

    await withTimeout(p, 55 * 1000, "sincronizarEnviosUnaVez")
        .then((stats) => {
            if (!stats) return;
            const mins = (stats.elapsedMs || 1) / 60000;
            const enviosMin = (stats.envios / mins).toFixed(1);
            console.log(
                `✅ Envios: completada — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, ` +
                `empresas=${stats.empresas}, tiempo=${(stats.elapsedMs / 1000).toFixed(1)}s, ≈ ${enviosMin} envíos/min`
            );
        })
        .catch((e) => {
            console.error("⏱️ Envios timeout:", e?.message || e);
        });

    // IMPORTANTE: liberamos lock SIEMPRE, aunque p quede colgado
    runningEnvios = false;

    if (cdcPending) runCdcSafely().catch(() => { });
}

function iniciarSchedulers() {
    // Envios + CDC cada 120s
    setInterval(() => {
        runEnviosTick().catch(() => { });
        runCdcSafely().catch(() => { });
    }, 120 * 1000);

    // Pendientes cada 30s (tu comentario original)
    setInterval(() => {
        runPendientesFixed().catch(() => { });
    }, 30 * 1000);
}

async function startJobs() {
    await actualizarEmpresas();

    iniciarSchedulers();
    startMonitoreoJob();
    startMonitoreoMetricas();

    console.log("✅ [JOBS] corriendo (envios/cdc/pendientes/monitoreos)");
}

// =========================
// Bootstrap (API o JOBS)
// =========================
(async () => {
    try {
        if (MODE === "jobs") {
            await startJobs();
        } else {
            startApi();
        }

        process.on("SIGINT", async () => {
            console.log("Cerrando proceso...");
            try { await redisClient.disconnect(); } catch { }
            try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => console.error("❌ unhandledRejection:", reason));
        process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));
    } catch (err) {
        console.error("❌ Error al iniciar:", err?.message || err);
    }
})();
