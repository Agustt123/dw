const { executeQuery } = require("../../db");

async function cantidadGlobal(conn, fecha) {
  const sql = `
   SELECT COUNT(DISTINCT CONCAT(didOwner, ':', didPaquete)) AS cantidad
FROM home_app_idx
WHERE dia=?
  AND en_historial=1
  AND didOwner<>0
 ;
  `;

  const rows = await executeQuery(conn, sql, [fecha], true);
  const cantidad = Number(rows?.[0]?.cantidad ?? 0);

  return { ok: true, cantidad, fecha };
}

module.exports = { cantidadGlobal };
