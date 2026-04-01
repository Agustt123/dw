
// routes/informeColecta.js
const express = require('express');
const { cantidadGlobalMesYDiaCached } = require('../controller/cantidadPaquetes/cantidad');
const { getConnectionLocalCdc } = require('../db');

const cantidad = express.Router();


// ---------- handlers ----------
cantidad.post('/', async (req, res) => {

    const { dia } = req.body || {};

    try {
        const fecha = dia || "2026-01-13";
        const resultado = await cantidadGlobalMesYDiaCached(
            () => getConnectionLocalCdc(),
            fecha
        );
        return res.status(200).json(resultado);
    } catch (error) {
        console.error("Error /byChofer:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});



module.exports = cantidad;
