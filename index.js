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
            // ✅ timeout por empresa para que no se cuelgue
            await withTimeout(EnviarcdAsignacion(didOwner), 20000, `CDC asignacion ${didOwner}`);
            await withTimeout(EnviarcdcEstado(didOwner), 20000, `CDC estado ${didOwner}`);
            console.log(`✅ CDC OK empresa ${didOwner}`);
        } catch (e) {
            console.error(`❌ Error CDC empresa ${didOwner}:`, e.message || e);
        }
    }

    try {
        await withTimeout(pendientesHoy(), 30000, "pendientesHoy");
        console.log("✅ pendientesHoy OK");
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e.message || e);
    }
}

let running = false;

function iniciarSchedulerUnico() {
    setInterval(async () => {
        if (running) {
            console.log("⏭️ Scheduler: ciclo saltado (todavía en ejecución)");
            return;
        }
        running = true;

        try {
            console.log("🔁 Envios: iniciando sincronización...");
            await withTimeout(sincronizarEnviosUnaVez(), 55 * 1000, "sincronizarEnviosUnaVez"); // max 55 segundos
            console.log("✅ Envios: sincronización completada");

            console.log("🔁 CDC/pendientes: iniciando...");
            await correrCdcYPendientesUnaVez();
            console.log("✅ CDC/pendientes: completado");
        } catch (e) {
            console.error("❌ Error en ciclo scheduler:", e.message || e);
        } finally {
            running = false;
        }
    }, 55 * 1000); // 55 segundos
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
