const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { redisClient, getFromRedis } = require("./db.js");
const { sincronizarEnviosParaTodasLasEmpresas } = require("./controller/controllerEnvio.js");
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

// Asume empresasDB = { "2": {...}, "3": {...} }
function obtenerDidOwners() {
    if (!empresasDB) return [];

    // Si viene string JSON por algún motivo
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

    // Si fuera array, acá deberías mapear según estructura
    console.warn("⚠️ empresasDB no tiene formato objeto esperado:", typeof empresasDB);
    return [];
}

app.get("/ping", (req, res) => {
    res.status(200).json({
        estado: true,
        mesanje: "Hola chris",
    });
});

const PORT = 13000;

async function correrCdcParaTodasLasEmpresas() {
    await actualizarEmpresas();

    const didOwners = obtenerDidOwners();
    if (!didOwners.length) {
        console.log("⚠️ No se encontraron empresas para correr CDC (empresasData vacío o inexistente).");
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

    // Si pendientesHoy es global, llamala una vez:
    try {
        pendientesHoy();
        console.log("✅ pendientesHoy OK");
    } catch (e) {
        console.error("❌ Error en pendientesHoy:", e);
    }

    // Si pendientesHoy fuera por empresa, sería así:
    // for (const didOwner of didOwners) await pendientesHoy(didOwner);
}

(async () => {
    try {
        await actualizarEmpresas();

        // ✅ Envios para todas (loop infinito) - NO usar await
        sincronizarEnviosParaTodasLasEmpresas();

        // ✅ Primera corrida inmediata de CDC/pendientes para todas
        await correrCdcParaTodasLasEmpresas();

        // ✅ Intervalo (evita solaparse)
        let running = false;
        setInterval(async () => {
            if (running) {
                console.log("⏭️ Ciclo CDC/pendientes saltado: ya hay uno en curso");
                return;
            }
            running = true;
            try {
                await correrCdcParaTodasLasEmpresas();
                console.log("✅ Ciclo CDC/pendientes completado");
            } catch (e) {
                console.error("❌ Error en ciclo CDC/pendientes:", e);
            } finally {
                running = false;
            }
        }, 1 * 60 * 1000);

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
            process.exit();
        });
    } catch (err) {
        console.error("❌ Error al iniciar el servidor:", err);
    }
})();
