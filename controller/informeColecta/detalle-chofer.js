const { executeQuery } = require("../../db");

/**
 * Detalle de colectas (estado '0') para UN chofer, por día y cliente.
 * Salida:
 * {
 *   "DD-MM": {
 *     cantidad: <número de clientes con colectas>,
 *     dids: [<todos los didsPaquete del día>],
 *     clientes: {
 *       "<cliente>": { didsPaquetes:[...] }
 *     }
 *   },
 *   ...
 * }
 */
async function detalleColectasPorChoferDiaCliente(dIdOwner, didChofer, desde, hasta, conn) {
    if (dIdOwner == null) throw new Error("dIdOwner requerido");
    if (didChofer == null) throw new Error("didChofer requerido");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde || '')) throw new Error("desde debe ser YYYY-MM-DD");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) throw new Error("hasta debe ser YYYY-MM-DD");

    const sql = `
    SELECT
      didChofer,
      DATE_FORMAT(dia, '%Y-%m-%d') AS dia,
      didCliente,
      didsPaquete
    FROM home_app
    WHERE dIdOwner   = ?
      AND didChofer  = ?
      AND didCliente <> 0
      AND estado     = '0'
      AND dia BETWEEN ? AND ?
    ORDER BY dia, didCliente
  `;

    const rows = await executeQuery(conn, sql, [dIdOwner, didChofer, desde, hasta], true);

    // formatea YYYY-MM-DD -> DD-MM
    const fmtDia = (val) => {
        if (val == null) return '??-??';
        if (val instanceof Date && !isNaN(val)) {
            const y = val.getFullYear();
            const m = String(val.getMonth() + 1).padStart(2, '0');
            const d = String(val.getDate()).padStart(2, '0');
            return `${d}-${m}`;
        }
        const s = String(val).trim();
        const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
        if (m1) return `${m1[3]}-${m1[2]}`;
        const m2 = /^(\d{2})[\/-](\d{2})[\/-](\d{4})$/.exec(s);
        if (m2) return `${m2[1]}-${m2[2]}`;
        return s;
    };

    // Map<fecha, Map<cliente, number[]>>
    const porDia = new Map();

    for (const r of rows) {
        const ddmm = fmtDia(r.dia);
        const cli = String(r.didCliente);

        const ids = String(r.didsPaquete || '')
            .split(',')
            .map(s => s.trim())
            .filter(Boolean)
            .map(n => Number(n))
            .filter(Number.isFinite);

        // si no hay paquetes, no lo incluimos
        if (ids.length === 0) continue;

        if (!porDia.has(ddmm)) porDia.set(ddmm, new Map());
        const porCliente = porDia.get(ddmm);
        porCliente.set(cli, ids);
    }

    // armar salida final
    const fechas = {};
    for (const [diaKey, clientesMap] of porDia.entries()) {
        const clientesObj = {};
        let didsTotales = [];

        for (const [cli, ids] of clientesMap.entries()) {
            clientesObj[cli] = { didsPaquetes: ids };
            didsTotales.push(...ids);
        }

        // eliminar duplicados por si acaso
        didsTotales = [...new Set(didsTotales)];

        fechas[diaKey] = {
            cantidad: clientesMap.size,  // número de clientes válidos ese día
            dids: didsTotales,           // todos los dids de ese día
            clientes: clientesObj
        };
    }

    return fechas;
}

module.exports = { detalleColectasPorChoferDiaCliente };
