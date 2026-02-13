// server.jobs.js
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
        console.error("❌ Error al actualizar empresas desde Redis:", e);
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

function withTimeout(promise, ms, label) {
    let t;
    const timeout = new Promise((_, rej) => {
        t = setTimeout(() => rej(new Error(`⏱️ Timeout: ${label} (${ms}ms)`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

// =========================
// CDC (solo CDC)
// =========================
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
            console.error(`❌ Error CDC empresa ${didOwner}:`, e.message || e);
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

// =========================
// CDC safe runner (no se pisa)
// =========================
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
        console.error("❌ Error en CDC:", e.message || e);
    } finally {
        runningCdc = false;
    }
}

// =========================
// Pendientes (no se pisa)
// =========================
async function runPendientesFixed() {
    if (runningPend) {
        console.log("⏭️ pendientesHoy sigue corriendo, salteo tick");
        return;
    }

    runningPend = true;
    try {
        await withTimeout(pendientesHoy(), 25000, "pendientesHoy"); // < intervalo
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e.message || e);
    } finally {
        runningPend = false;
    }
}

// =========================
// Envios runner (no se pisa y NO se cuelga el lock)
// =========================
async function runEnviosTick() {
    if (runningEnvios) {
        console.log("⏭️ Envios sigue corriendo, no arranco otro");
        return;
    }

    runningEnvios = true;
    console.log("🔁 Envios: iniciando sincronización...");

    const p = Promise.resolve().then(() => sincronizarEnviosUnaVez());

    // OJO: si sincronizarEnviosUnaVez queda colgado, igual liberamos lock por timeout
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
            console.error("⏱️ Envios timeout:", e.message || e);
        });

    // liberamos lock SIEMPRE (no await del p si quedó colgado)
    runningEnvios = false;

    // si CDC quedó pendiente, lo largamos
    if (cdcPending) runCdcSafely().catch(() => { });
}

// =========================
// Schedulers
// =========================
function iniciarSchedulers() {
    // ENVÍOS + CDC (cada 120s)
    setInterval(() => {
        runEnviosTick().catch(() => { });
        runCdcSafely().catch(() => { });
    }, 120 * 1000);

    // Pendientes cada 30s (tu comentario decía 30s)
    setInterval(() => {
        runPendientesFixed().catch(() => { });
    }, 30 * 1000);
}

// =========================
// Bootstrap JOBS
// =========================
(async () => {
    try {
        await actualizarEmpresas();

        iniciarSchedulers();
        startMonitoreoJob();
        startMonitoreoMetricas();

        console.log("✅ JOBS corriendo (envios/cdc/pendientes/monitoreos)");

        process.on("SIGINT", async () => {
            console.log("Cerrando JOBS...");
            try { await redisClient.disconnect(); } catch { }
            try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => console.error("❌ unhandledRejection:", reason));
        process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));
    } catch (err) {
        console.error("❌ Error al iniciar JOBS:", err);
    }
})();
