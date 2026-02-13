
// routes/informeColecta.js
const express = require('express');
const { cantidadGlobal, cantidadGlobalMesYDia } = require('../controller/cantidadPaquetes/cantidad');
const { getConnectionLocalCdc } = require('../db');

const cantidad = express.Router();


// ---------- handlers ----------
cantidad.post('/', async (req, res) => {

    const { dia } = req.body || {};
    console.log("ANTES getConnection");
    const db = await getConnectionLocalCdc();
    console.log("DESPUES getConnection");


    try {
        const resultado = await cantidadGlobalMesYDia(db, dia || "2026-01-13");
        return res.status(200).json(resultado);
    } catch (error) {
        console.error("Error /byChofer:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});



module.exports = cantidad;
