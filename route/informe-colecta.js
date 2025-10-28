// routes/informeColecta.js
const express = require('express');
const { colectasEstado0PorChofer } = require('../controller/informeColecta/chofer-total');

const { getConnectionLocal } = require('../db');
const { detalleColectasPorChoferDiaCliente } = require('../controller/informeColecta/detalle-chofer');

const informeColecta = express.Router();

/**
 * Body esperado:
 * {
 *   "didEmpresa": number|string,
 *   "desde": "YYYY-MM-DD",
 *   "hasta": "YYYY-MM-DD"
 * }
 */
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

/**
 * Body esperado:
 * {
 *   "didEmpresa": number|string,
 *   "didChofer": number|string,
 *   "desde": "YYYY-MM-DD",
 *   "hasta": "YYYY-MM-DD"
 * }
 */
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

module.exports = informeColecta;
