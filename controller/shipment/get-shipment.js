const { executeQuery } = require("../../db");

async function getShipmentByIdOwner(dIdOwner, shipment_id, conn) {
    if (!dIdOwner) throw new Error("dIdOwner requerido");

    const sql = `
    SELECT * envios 
    WHERE dIdOwner = ?
    and ml_shipment_id  = ?
    AND superado = 0 AND elim = 0
    `;

    const rows = await executeQuery(conn, sql, [dIdOwner, shipment_id], true);
    return rows;
}



module.exports = { getShipmentByIdOwner };
