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
app.use(
    cors({
        origin: "*",
        methods: ["GET", "POST", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization"],
    })
);

app.use("/informe-colecta", informeColecta);

app.get("/ping", (req, res) => {
    res.status(200).json({ estado: true, mensaje: "OK" });
});

const PORT = 13000;

// ----------------- Empresas desde Redis -----------------
let empresasDB = null;

async function actualizarEmpresas() {
    try {
        const empresasDataJson = await getFromRedis("empresasData");
        empresasDB = empresasDataJson || null;
    } catch (error) {
        console.error("❌ Error al actualizar empresas desde Redis:", error);
        empresasDB = null;
    }
}

function obtenerDidOwners() {
    if (!empresasDB) return [];

    if (typeof empresasDB === "string") {
        try {
            empresasDB = JSON.parse(empresasDB);
        } catch (e) {
            console.error("❌ empresasDB es string pero no JSON válido:", e);
            return [];
        }
    }

    if (typeof empresasDB === "object" && !Array.isArray(empresasDB)) {
        return Object.keys(empresasDB)
            .map((x) => parseInt(x, 10))
            .filter((n) => !isNaN(n));
    }

    console.warn("⚠️ empresasDB no tiene formato objeto esperado:", typeof empresasDB);
    return [];
}

// ----------------- CDC + pendientes (una pasada) -----------------
async function correrCdcYPendientesUnaVez() {
    await actualizarEmpresas();
    const didOwners = obtenerDidOwners();

    if (!didOwners.length) {
        console.log("⚠️ No se encontraron empresas para CDC (empresasData vacío o inexistente).");
        return;
    }

    console.log(`🔁 Corriendo CDC para ${didOwners.length} empresas...`);

    for (const didOwner of didOwners) {
        try {
            await EnviarcdAsignacion(didOwner);
            await EnviarcdcEstado(didOwner);
            console.log(`✅ CDC OK empresa ${didOwner}`);
        } catch (e) {
            console.error(`❌ Error CDC empresa ${didOwner}:`, e);
        }
    }

    try {
        await pendientesHoy(); // si tu pendientesHoy no es async, sacale el await
        console.log("✅ pendientesHoy OK");
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e);
    }
}

// ----------------- Schedulers (todo cada 1 min, sin corrida inicial) -----------------
function iniciarSchedulers() {
    // Envios: cada 1 min
    let runningEnvios = false;
    setInterval(async () => {
        if (runningEnvios) {
            console.log("⏭️ Envios: ciclo saltado (todavía en ejecución)");
            return;
        }
        runningEnvios = true;
        try {
            console.log("🔁 Envios: iniciando sincronización...");
            await sincronizarEnviosUnaVez();
            console.log("✅ Envios: sincronización completada");
        } catch (e) {
            console.error("❌ Envios: error en ciclo:", e);
        } finally {
            runningEnvios = false;
        }
    }, 1 * 60 * 1000);

    // CDC + pendientes: cada 1 min
    let runningCdc = false;
    setInterval(async () => {
        if (runningCdc) {
            console.log("⏭️ CDC/pendientes: ciclo saltado (todavía en ejecución)");
            return;
        }
        runningCdc = true;
        try {
            await correrCdcYPendientesUnaVez();
            console.log("✅ CDC/pendientes: ciclo completado");
        } catch (e) {
            console.error("❌ CDC/pendientes: error en ciclo:", e);
        } finally {
            runningCdc = false;
        }
    }, 1 * 60 * 1000);
}

// ----------------- Boot -----------------
(async () => {
    try {
        await actualizarEmpresas();

        iniciarSchedulers();

        app.listen(PORT, () => {
            console.log(`Servidor escuchando en http://localhost:${PORT}`);
        });

        process.on("SIGINT", async () => {
            console.log("Cerrando servidor...");
            try {
                await redisClient.disconnect();
            } catch (e) {
                console.error("Error desconectando Redis:", e);
            }
            try {
                if (typeof closeDWPool === "function") await closeDWPool();
            } catch (e) {
                console.error("Error cerrando DW pool:", e);
            }
            process.exit();
        });

        process.on("unhandledRejection", (reason) => {
            console.error("❌ unhandledRejection:", reason);
        });
        process.on("uncaughtException", (err) => {
            console.error("❌ uncaughtException:", err);
        });
    } catch (err) {
        console.error("❌ Error al iniciar el servidor:", err);
    }
})();
