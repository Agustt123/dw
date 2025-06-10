const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { redisClient, getFromRedis } = require("./db.js");
const { sincronizarEnviosParaTodasLasEmpresas } = require("./controller/controllerEnvio.js");



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
        await sincronizarEnviosParaTodasLasEmpresas();

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
