const { executeQuery } = require("../../db");

/**
 * Colectas (estado '0') por chofer y por cliente para un owner y rango de fechas.
 * Devuelve:
 * {
 *   data: [
 *     { chofer: number, colectas: number },
 *     ...
 *   ],
 *   dataCliente: [
 *     { cliente: number, colecta: number },
 *     ...
 *   ]
 * }
 */
async function colectasEstado0PorChofer(dIdOwner, desde, hasta, conn) {
  if (!dIdOwner) throw new Error("dIdOwner requerido");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(desde || '')) throw new Error("desde debe ser YYYY-MM-DD");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) throw new Error("hasta debe ser YYYY-MM-DD");

  try {
    // 🔹 1. Colectas por chofer (cuenta cliente + día)
    const sqlColectas = `
      SELECT
        didChofer,
        COUNT(DISTINCT CONCAT(didCliente, '-', DATE_FORMAT(dia, '%Y-%m-%d'))) AS colectas
      FROM home_app
      WHERE dIdOwner   = ?
        AND didChofer <> 0
        AND didCliente <> 0
        AND estado     = '0'
        AND dia BETWEEN ? AND ?
        AND TRIM(didsPaquete) <> ''
        AND didsPaquete REGEXP '[0-9]'
      GROUP BY didChofer
      ORDER BY colectas DESC
    `;

    const rows = await executeQuery(conn, sqlColectas, [dIdOwner, desde, hasta], true);

    const data = rows.map(r => ({
      chofer: Number(r.didChofer),
      colectas: Number(r.colectas || 0)
    }));

    // 🔹 2. Colectas por cliente (cuenta chofer + día, ignora chofer 0)
    const sqlClientes = `
      SELECT
        didCliente,
        COUNT(DISTINCT CONCAT(didChofer, '-', DATE_FORMAT(dia, '%Y-%m-%d'))) AS colectas
      FROM home_app
      WHERE dIdOwner   = ?
        AND estado     = '0'
        AND didChofer <> 0            -- 👈 excluye chofer 0
        AND didCliente <> 0
        AND dia BETWEEN ? AND ?
        AND TRIM(didsPaquete) <> ''
        
      GROUP BY didCliente
      ORDER BY colectas DESC
    `;

    const clientesRows = await executeQuery(conn, sqlClientes, [dIdOwner, desde, hasta], true);

    const dataCliente = clientesRows.map(c => ({
      cliente: Number(c.didCliente),
      colecta: Number(c.colectas || 0)
    }));

    // 🔹 3. Devolver ambos conjuntos
    return { data, dataCliente };

  } finally {
    // sin manejo de conexión acá porque recibimos `conn`
  }
}

module.exports = { colectasEstado0PorChofer };
