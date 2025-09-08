const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { redisClient, getFromRedis } = require("./db.js");
const { sincronizarEnviosParaTodasLasEmpresas, sincronizarEnviosParaTodasLasEmpresas2 } = require("./controller/controllerEnvio.js");
const { EnviarcdAsignacion, EnviarcdcEstado } = require("./controller/checkcdc.js");
const { pendientesHoy } = require("./controller/pendientes.js");



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


let empresasDB = null;

async function actualizarEmpresas() {
    try {
        const empresasDataJson = await getFromRedis("empresasData");
        empresasDB = empresasDataJson || [];
    } catch (error) {
        console.error("Error al actualizar empresas desde Redis:", error);
    }
}




app.get("/", (req, res) => {
    res.status(200).json({
        estado: true,
        mesanje: "Hola chris",
    });
});

const PORT = 13000;

(async () => {
    try {
        await actualizarEmpresas();
        await sincronizarEnviosParaTodasLasEmpresas2();

        await EnviarcdAsignacion(164);
        await EnviarcdcEstado(164);
        await pendientesHoy();
        // Actualizar empresas cada 10 minutos
        setInterval(async () => {
            await pendientesHoy();
            await EnviarcdAsignacion(164);
            await EnviarcdcEstado(164);
        }, 5 * 60 * 1000);

        app.listen(PORT, () => {
            console.log(`Servidor escuchando en http://localhost:${PORT}`);
        });
        //   console.log(empresasDB, "empresasDB");


        process.on("SIGINT", async () => {
            console.log("Cerrando servidor...");
            await redisClient.disconnect();
            process.exit();
        });
    } catch (err) {
        console.error("Error al iniciar el servidor:", err);
    }
})();
