
// routes/informeColecta.js
const express = require('express');

const { getConnectionLocalCdc, getConnection } = require('../db');
const { monitoreo } = require('../controller/monitoreoServidores/monitoreo');

const { getMonitoreo } = require('../controller/monitoreoServidores/getMonitoreo');

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


monitorear.get('/', async (req, res) => {
    const db = await getConnectionLocalCdc();
    try {
        const resultado = await getMonitoreo(db);
        return res.status(200).json({ estado: true, data: resultado });
    } catch (error) {
        console.error("Error /testLocal:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});



module.exports = monitorear;
