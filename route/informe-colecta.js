// routes/informeColecta.js
const express = require('express');
const crypto = require('crypto');
const { colectasEstado0PorChofer } = require('../controller/informeColecta/chofer-total');
const { getConnectionLocal } = require('../db');
const { detalleColectasPorChoferDiaCliente } = require('../controller/informeColecta/detalle-chofer');
const { detalleColectasPorCliente } = require('../controller/informeColecta/detalle-cliente');
const { detalleColectasPorChoferesDia, detallesColectasFantasma } = require('../controller/informeColecta/detalle-cliente-fantasma');

const informeColecta = express.Router();

// Hash esperado (constante)
const EXPECTED_TOKEN = crypto.createHash('sha256').update('lightdata').digest('hex');
console.log(EXPECTED_TOKEN);


// Middleware de verificación
function requireToken(req, res, next) {
    const provided = req.body?.token;
    if (provided !== EXPECTED_TOKEN) {
        return res.status(401).json({ estado: false, mensaje: 'Token inválido' });
    }
    return next();
}

// Aplico el middleware a todas las rutas de este router
informeColecta.use(requireToken);

// ---------- handlers ----------
informeColecta.post('/byChofer', async (req, res) => {
    const db = await getConnectionLocal();
    const { didEmpresa, desde, hasta } = req.body || {};

    if (didEmpresa == null || !/^\d{4}-\d{2}-\d{2}$/.test(desde || '') || !/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) {
        return res.status(400).json({ estado: false, mensaje: "Parámetros inválidos" });
    }

    try {
        const resultado = await colectasEstado0PorChofer(didEmpresa, desde, hasta, db);
        return res.status(200).json(resultado);
    } catch (error) {
        console.error("Error /byChofer:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});

informeColecta.post('/detalleByChofer', async (req, res) => {
    const db = await getConnectionLocal();
    const { didEmpresa, didChofer, desde, hasta } = req.body || {};

    if (didEmpresa == null || didChofer == null ||
        !/^\d{4}-\d{2}-\d{2}$/.test(desde || '') || !/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) {
        return res.status(400).json({ estado: false, mensaje: "Parámetros inválidos" });
    }

    try {
        const data = await detalleColectasPorChoferDiaCliente(didEmpresa, didChofer, desde, hasta, db);
        return res.status(200).json(data);
    } catch (error) {
        console.error("Error /detalleByChofer:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});
informeColecta.post('/detalleByCLiente', async (req, res) => {
    const db = await getConnectionLocal();
    const { didEmpresa, didCliente, desde, hasta } = req.body || {};

    if (didEmpresa == null || didCliente == null ||
        !/^\d{4}-\d{2}-\d{2}$/.test(desde || '') || !/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) {
        return res.status(400).json({ estado: false, mensaje: "Parámetros inválidos" });
    }

    try {
        const data = await detalleColectasPorCliente(didEmpresa, didCliente, desde, hasta, db);
        return res.status(200).json(data);
    } catch (error) {
        console.error("Error /detalleByChofer:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});
informeColecta.post('/detalleByChoferFantasma', async (req, res) => {
    const db = await getConnectionLocal();
    const { didEmpresa, didsChofer, desde, hasta } = req.body || {};

    if (didEmpresa == null || didsChofer == null ||
        !/^\d{4}-\d{2}-\d{2}$/.test(desde || '') || !/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) {
        return res.status(400).json({ estado: false, mensaje: "Parámetros inválidos" });
    }

    try {
        const data = await detallesColectasFantasma(didEmpresa, didsChofer, desde, hasta, db);
        return res.status(200).json(data);
    } catch (error) {
        console.error("Error /detalleByChoferFantasma:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});

module.exports = informeColecta;
