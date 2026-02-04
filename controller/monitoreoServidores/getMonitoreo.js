
const { executeQuery } = require("../../db");

async function getMonitoreo(db) {
    try {
        const query = `SELECT * from monitoreo_servicios ORDER BY id DESC LIMIT 5`;
        const results = await executeQuery(db, query);
        return results;
    } catch (error) {
        throw new Error("Error al obtener el monitoreo de servicios: " + error.message);
    }




}


await(db, sql, values);


module.exports = { getMonitoreo };
