const { executeQuery, getConnectionLocal } = require("../../db");

/**
 * Colectas (estado '0') por chofer para un owner y rango de fechas.
 * Devuelve: [{ chofer: number, colectas: number }, ...]
 * Donde "colectas" = # de clientes distintos atendidos por ese chofer.
 */
async function colectasEstado0PorChofer(dIdOwner, desde, hasta, conn) {
    try {
        const sql = `
      SELECT
        didChofer,
        COUNT(DISTINCT didCliente) AS colectas
      FROM home_app
      WHERE dIdOwner   = ?
        AND didChofer <> 0
        AND didCliente <> 0
        AND estado     = '0'
        AND dia BETWEEN ? AND ?
        AND didsPaquete IS NOT NULL
        AND didsPaquete <> ''
      GROUP BY didChofer
      ORDER BY colectas DESC
    `;

        const rows = await executeQuery(conn, sql, [dIdOwner, desde, hasta], true);

        return rows.map(r => ({
            chofer: Number(r.didChofer),
            colectas: Number(r.colectas || 0),
        }));
    } finally {
        // sin manejo de conexión acá porque recibimos `conn`
    }
}

module.exports = { colectasEstado0PorChofer };
