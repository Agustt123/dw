const axios = require("axios");
const { executeQuery } = require("../../db");

async function monitoreo(db, services = [
    { key: "asignaciones", url: "http://asignaciones.lightdata.app/ping" },
    { key: "backgps", url: "http://backgps2.lightdata.com.ar/ping" },
    { key: "colecta", url: "http://colecta.lightdata.app/ping" },
    { key: "aplanta", url: "http://aplanta.lightdata.app/ping" },
    { key: "altaEnvios", url: "http://altaenvios.lightdata.com.ar/ping" },
    { key: "fulfillment", url: "http://ffull.lightdata.app/ping" },
    { key: "ffmobile", url: "http://ffmovil.lightdata.app/ping" },
    { key: "callback", url: "http://whml.lightdata.app/ping" },
    { key: "lightdatito", url: "http://node1.liit.com.ar/ping" },
    { key: "websocket_mail", url: "https://notificaremails.lightdata.com.ar/ping" },
    { key: "etiquetas", url: "http://printserver.lightdata.app/ping" },
    { key: "estados", url: "http://serverestado.lightdata.app/ping" },
    { key: "apimovil", url: "http://apimovil2.lightdata.app/ping" },
]) {
    const row = {};

    for (const s of services) {
        try {
            const t0 = process.hrtime.bigint();
            await axios.get(s.url, {
                timeout: 2000,
                validateStatus: () => true
            });
            const t1 = process.hrtime.bigint();

            let ms = Number(t1 - t0) / 1_000_000;
            if (ms < 1) ms = 1;

            row[s.key] = Math.round(ms);
        } catch (err) {
            row[s.key] = null;
            console.error(`[MONITOREO] ${s.key} ERROR`, err.message);
        }
    }

    const columns = Object.keys(row);
    const placeholders = columns.map(() => "?").join(",");
    const values = columns.map(c => row[c]);

    const sql = `
    INSERT INTO monitoreo_servicios (${columns.join(",")})
    VALUES (${placeholders})
  `;

    await executeQuery(db, sql, values);
    return row;
}

module.exports = { monitoreo };
