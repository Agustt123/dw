
// routes/informeColecta.js
const express = require('express');

const { getConnectionLocalCdc, getConnection } = require('../db');
const { monitoreo } = require('../controller/monitoreoServidores/monitoreo');

const monitorear = express.Router();


// ---------- handlers ----------
monitorear.post('/', async (req, res) => {


    const db = await getConnectionLocalCdc();

    try {
        const resultado = await monitoreo(db);
        return res.status(200).json(resultado);
    } catch (error) {
        console.error("Error :", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});



module.exports = monitorear;
