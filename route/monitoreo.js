
// routes/informeColecta.js
const express = require('express');

const { getConnectionLocalCdc, getConnection } = require('../db');
const { monitoreo } = require('../controller/monitoreoServidores/monitoreo');
const { obtenerUltimoMonitoreoBd } = require('../controller/monitoreoServidores/monitoreoBd');

const { getMonitoreo } = require('../controller/monitoreoServidores/getMonitoreo');
const { obtenerMetricasUltimaCorrida } = require('../controller/monitoreoServidores/cronMonitoreo');
const { obtenerMetricasProcesslist } = require('../fuctions/showProcesList');
const { insertarPeorPct } = require('../controller/monitoreoServidores/insertarPeorpct');
const { insertarNotificacionesUltima } = require('../controller/monitoreoServidores/insertarNotificacionesUltima');
const { obtenerUltimoPeorPct } = require('../controller/monitoreoServidores/obtenerUltimoPeorPct');
const {
    obtenerUltimaNotificacion,
    obtenerUltimaNotificacionV2,
} = require('../controller/monitoreoServidores/obtenerUltimaNotificacion');
const { insertarAlerta } = require('../controller/monitoreoServidores/insertarAlerta');
const { obtenerUltimasAlertas } = require('../controller/monitoreoServidores/obtenerUltimasAlertas');

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


monitorear.get('/metricas', async (req, res) => {

    try {
        const resultado = await obtenerMetricasUltimaCorrida();
        return res.status(200).json({ estado: true, data: resultado });
    } catch (error) {
        console.error("Error /testLocal:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {

    }
});

monitorear.get('/procesos', async (req, res) => {

    const db = await getConnectionLocalCdc();
    try {
        const resultado = await obtenerMetricasProcesslist(db);
        return res.status(200).json({ estado: true, data: resultado });
    } catch (error) {
        console.error("Error /testLocal:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }

    }
});

monitorear.get('/procesos-conjunto', async (req, res) => {

    const db = await getConnectionLocalCdc();
    try {
        const resultado = await obtenerUltimoMonitoreoBd(db);
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error /procesos-conjunto:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {
        if (db?.release) try { db.release(); } catch { }
    }
});

monitorear.post('/peor-pct', async (req, res) => {


    try {
        const resultado = await insertarPeorPct(req.body);
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error /peor-pct:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {

    }
});

monitorear.get('/peor-pct', async (req, res) => {
    try {
        const resultado = await obtenerUltimoPeorPct();
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error GET /peor-pct:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});

monitorear.post('/notificaciones-ultima', async (req, res) => {
    try {
        const resultado = await insertarNotificacionesUltima(req.body);
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error /notificaciones-ultima:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    } finally {

    }
});

monitorear.get('/notificaciones-ultima', async (req, res) => {
    try {
        const resultado = await obtenerUltimaNotificacion();
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error GET /notificaciones-ultima:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});

monitorear.get('/notificaciones-ultima/v2', async (req, res) => {
    try {
        const resultado = await obtenerUltimaNotificacionV2();
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error GET /notificaciones-ultima/v2:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});

monitorear.post('/alerta', async (req, res) => {
    try {
        const resultado = await insertarAlerta(req.body);
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error /alerta:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});

monitorear.get('/alerta', async (req, res) => {
    try {
        const resultado = await obtenerUltimasAlertas(req.query.limit);
        return res.status(200).json({
            estado: true,
            data: resultado
        });
    } catch (error) {
        console.error("Error GET /alerta:", error);
        return res.status(500).json({ estado: false, mensaje: "Error en el servidor" });
    }
});

module.exports = monitorear;
