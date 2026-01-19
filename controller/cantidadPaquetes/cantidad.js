const { executeQuery } = require("../../db");

async function cantidadGlobal(conn, fecha) {
    const sql = `
    SELECT 
      COALESCE(SUM(
        CASE 
          WHEN didsPaquete IS NULL OR TRIM(didsPaquete) = '' THEN 0
          ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
        END
      ), 0) AS cantidad_paquetes,
      COUNT(*) AS filas
    FROM home_app
    WHERE fecha = ?
      AND didOwner = 0
      AND didChofer = 0
      AND didCliente = 0
  `;

    const rows = await executeQuery(conn, sql, [fecha], true);

    return {
        fecha,
        cantidad_paquetes: Number(rows?.[0]?.cantidad_paquetes ?? 0),
        filas: Number(rows?.[0]?.filas ?? 0),
    };
}

module.exports = { cantidadGlobal };
