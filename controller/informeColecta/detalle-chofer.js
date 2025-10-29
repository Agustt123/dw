const { executeQuery } = require("../../db");

/**
 * Detalle de colectas (estado '0') para UN chofer, por día y cliente.
 * Salida:
 * [{ chofer, fechas: [ { "DD-MM": { clientes: { "<cliente>": { didsPaquetes:[...] } } } ] }]
 */
async function detalleColectasPorChoferDiaCliente(dIdOwner, didChofer, desde, hasta, conn) {
    if (dIdOwner == null) throw new Error("dIdOwner requerido");
    if (didChofer == null) throw new Error("didChofer requerido");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde || '')) throw new Error("desde debe ser YYYY-MM-DD");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(hasta || '')) throw new Error("hasta debe ser YYYY-MM-DD");

    const sql = `
    SELECT
      didChofer,
      DATE_FORMAT(dia, '%Y-%m-%d') AS dia,   -- <== fuerza formato estable
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

    // dia('YYYY-MM-DD') -> 'DD-MM' (o cambiá a `${d}/${m}` si querés 'DD/MM')
    const fmtDia = (val) => {
        if (val == null) return '??-??';
        // si viene Date, lo pasamos a YYYY-MM-DD
        if (val instanceof Date && !isNaN(val)) {
            const y = val.getFullYear();
            const m = String(val.getMonth() + 1).padStart(2, '0');
            const d = String(val.getDate()).padStart(2, '0');
            return `${d}-${m}`;
        }
        const s = String(val).trim();
        // intenta YYYY-MM-DD
        const m1 = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
        if (m1) return `${m1[3]}-${m1[2]}`;
        // intenta DD/MM/AAAA o DD-MM-AAAA
        const m2 = /^(\d{2})[\/-](\d{2})[\/-](\d{4})$/.exec(s);
        if (m2) return `${m2[1]}-${m2[2]}`;
        // fallback: no rompas
        return s;
    };

    // chofer (único) -> dia -> cliente -> [ids]
    const porDia = new Map(); // Map<string, Map<string, number[]>>

    for (const r of rows) {
        const ddmm = fmtDia(r.dia);
        const cli = String(r.didCliente);

        if (!porDia.has(ddmm)) porDia.set(ddmm, new Map());
        const porCliente = porDia.get(ddmm);
        if (!porCliente.has(cli)) porCliente.set(cli, []);

        if (r.didsPaquete) {
            const ids = String(r.didsPaquete)
                .split(",")
                .map(s => s.trim())
                .filter(Boolean)
                .map(n => Number(n))
                .filter(Number.isFinite);
            if (ids.length) porCliente.get(cli).push(...ids);
        }
    }
    // armar salida como objeto de fechas
    const fechas = {};
    for (const [diaKey, clientesMap] of porDia.entries()) {
        const clientesObj = {};
        for (const [cli, ids] of clientesMap.entries()) {
            clientesObj[cli] = { didsPaquetes: ids };
        }
        fechas[diaKey] = { clientes: clientesObj };
    }

    return fechas;

}
module.exports = { detalleColectasPorChoferDiaCliente };
