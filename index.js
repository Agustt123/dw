// index.js
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const { redisClient, getFromRedis, closeDWPool } = require("./db.js");
const { sincronizarEnviosUnaVez } = require("./controller/controllerEnvio.js");
const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/procesarCDC/checkcdc2.js");
const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");
const informeColecta = require("./route/informe-colecta.js");

const app = express();

app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"], allowedHeaders: ["Content-Type", "Authorization"] }));

app.use("/informe-colecta", informeColecta);

app.get("/ping", (req, res) => res.status(200).json({ estado: true, mensaje: "OK" }));

const PORT = 13000;

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

async function correrCdcYPendientesUnaVez() {
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

    try {
        await withTimeout(pendientesHoy(), 300000, "pendientesHoy");
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e.message || e);
    }
}

let runningPromise = null;

// ✅ NUEVO: lock + pending para CDC/pendientes
let runningCdc = false;
let cdcPending = false;

async function runCdcSafely() {
    // Si ya está corriendo, marcamos pendiente y salimos
    if (runningCdc) {
        cdcPending = true;
        return;
    }

    // Si envíos está corriendo, marcamos pendiente y salimos
    if (runningPromise) {
        cdcPending = true;
        return;
    }

    runningCdc = true;
    try {
        do {
            cdcPending = false;

            console.log("🔁 CDC/pendientes: iniciando...");
            await correrCdcYPendientesUnaVez();
            console.log("✅ CDC/pendientes: completado");

            // Si durante la ejecución alguien lo marcó pendiente, lo repetimos
            // (pero ojo: si Envios arrancó mientras tanto, cortamos y queda pendiente)
            if (runningPromise) {
                cdcPending = true;
                break;
            }
        } while (cdcPending);
    } catch (e) {
        console.error("❌ Error en CDC/pendientes:", e.message || e);
    } finally {
        runningCdc = false;
    }
}

function iniciarSchedulerUnico() {
    setInterval(async () => {
        // =========================
        // ENVÍOS (lock existente)
        // =========================
        if (!runningPromise) {
            console.log("🔁 Envios: iniciando sincronización...");
            runningPromise = sincronizarEnviosUnaVez();

            withTimeout(runningPromise, 55 * 1000, "sincronizarEnviosUnaVez")
                .then((stats) => {
                    const mins = (stats.elapsedMs || 1) / 60000;
                    const enviosMin = (stats.envios / mins).toFixed(1);
                    console.log(
                        `✅ Envios: completada — envios=${stats.envios}, asig=${stats.asignaciones}, estados=${stats.estados}, elim=${stats.eliminaciones}, ` +
                        `empresas=${stats.empresas}, tiempo=${(stats.elapsedMs / 1000).toFixed(1)}s, ≈ ${enviosMin} envíos/min`
                    );
                })
                .catch((e) => {
                    console.error("⏱️ Envios se pasó de 55s (sigue corriendo):", e.message || e);
                })
                .finally(async () => {
                    try { await runningPromise; } catch { }

                    runningPromise = null;

                    // ✅ si CDC quedó pendiente mientras Envios corría, lo arrancamos ahora
                    if (cdcPending) {
                        runCdcSafely().catch(() => { });
                    }
                });
        } else {
            console.log("⏭️ Envios sigue corriendo, no arranco otro");
        }

        // =========================
        // CDC/PENDIENTES (siempre intentamos; si no se puede, queda pending)
        // =========================
        runCdcSafely().catch(() => { });
    }, 120 * 1000);
}

(async () => {
    try {
        await actualizarEmpresas();

        iniciarSchedulerUnico();

        app.listen(PORT, () => console.log(`Servidor escuchando en http://localhost:${PORT}`));

        process.on("SIGINT", async () => {
            console.log("Cerrando servidor...");
            try { await redisClient.disconnect(); } catch { }
            try { if (typeof closeDWPool === "function") await closeDWPool(); } catch { }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => console.error("❌ unhandledRejection:", reason));
        process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));
    } catch (err) {
        console.error("❌ Error al iniciar el servidor:", err);
    }
})();
