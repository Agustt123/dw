const express = require("express");
const { getConnectionLocalPendientes } = require("../db");
const {
    entregadosPorChofer,
    resetClientesCache,
} = require("../controller/entregados/entregadosPorChofer");

const entregados = express.Router();

entregados.post("/", async (req, res) => {
    let db;

    try {
        db = await getConnectionLocalPendientes();
        const resultado = await entregadosPorChofer(req.body || {}, db);
        return res.status(200).json(resultado);
    } catch (error) {
        const message = error?.message || "Error en el servidor";
        const status = /invalido|invalida/.test(message) ? 400 : 500;

        console.error("Error /entregados:", error);
        return res.status(status).json({
            estado: false,
            mensaje: status === 400 ? message : "Error en el servidor",
        });
    } finally {
        if (db?.release) {
            try { db.release(); } catch (_) { }
        }
    }
});

entregados.post("/cache/reset", (req, res) => {
    const result = resetClientesCache(req.body?.didEmpresa);
    return res.status(200).json({ estado: true, ...result });
});

module.exports = entregados;
