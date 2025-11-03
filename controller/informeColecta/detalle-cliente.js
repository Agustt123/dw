const { executeQuery } = require("../../db");

/**
 * Detalle de colectas (estado '0') para UN cliente, por día.
 * Salida:
 * {
 *   didsPaquetes: "<todos los dids de todos los días>",
 *   "DD-MM": {
 *     cantidad: <número de choferes con colectas>,
 *     dids: "<dids de ese día separados por coma>"
 *   },
 *   ...
 * }
 */
async function detalleColectasPorCliente(dIdOwner, didCliente, desde, hasta, conn) {
    if (dIdOwner == null) throw new Error("dIdOwner requerido");
    if (didCliente == null) throw new Error("didCliente requerido");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde || '')) throw new Error("desde debe ser YYYY-MM-DD");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) throw new Error("hasta debe ser YYYY-MM-DD");

    const sql = `
      SELECT
        didCliente,
        DATE_FORMAT(dia, '%Y-%m-%d') AS dia,
        didChofer,
        didsPaquete
      FROM home_app
      WHERE dIdOwner   = ?
        AND didCliente = ?
        AND didChofer  <> 0
        AND estado     = '0'
        AND dia BETWEEN ? AND ?
        AND TRIM(didsPaquete) <> ''
        AND didsPaquete REGEXP '[0-9]'
      ORDER BY dia, didChofer
    `;

    const rows = await executeQuery(conn, sql, [dIdOwner, didCliente, desde, hasta], true);

    // ---- formateador de fecha ----
    const fmtDia = (val) => {
        if (val == null) return '??-??';
        const s = String(val).trim();
        const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
        if (m1) return `${m1[3]}-${m1[2]}`;
        const m2 = /^(\d{2})[\/-](\d{2})[\/-](\d{4})$/.exec(s);
        if (m2) return `${m2[1]}-${m2[2]}`;
        return s;
    };

    // ---- agrupador por día ----
    const porDia = new Map(); // Map<string, Set<number>> (paquetes por día)
    const didsGlobal = new Set(); // todos los paquetes del cliente

    for (const r of rows) {
        const ddmm = fmtDia(r.dia);

        const ids = String(r.didsPaquete || '')
            .split(',')
            .map(s => s.trim())
            .filter(Boolean)
            .map(n => Number(n))
            .filter(Number.isFinite);

        if (ids.length === 0) continue;

        if (!porDia.has(ddmm)) porDia.set(ddmm, new Set());
        const setDia = porDia.get(ddmm);
        ids.forEach(id => {
            setDia.add(id);
            didsGlobal.add(id);
        });
    }

    // ---- armar salida final ----
    const fechas = {};

    // agregar string global con todos los paquetes
    fechas.didsPaquetes = [...didsGlobal].join(',');

    for (const [diaKey, idsSet] of porDia.entries()) {
        fechas[diaKey] = {
            cantidad: idsSet.size, // número de paquetes distintos ese día (no choferes)
            dids: [...idsSet].join(',')
        };
    }

    return fechas;
}

module.exports = { detalleColectasPorCliente };
