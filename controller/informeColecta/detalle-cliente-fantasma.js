const { executeQuery } = require("../../db");

/**
 * Detalle de colectas (estado '0') para un conjunto de choferes (dids), agrupado por día.
 * - Entrada:
 *    - dIdOwner: number
 *    - didsChoferesStr: string con dids de chofer "28,33,41"
 *    - desde, hasta: 'YYYY-MM-DD'
 *    - conn: connection
 * - Salida:
 * {
 *   dids: "<todos los paquetes de todos los días separados por coma>",
 *   "DD-MM": {
 *     colectas: <cantidad de paquetes únicos ese día>,
 *     dids: "<paquetes de ese día separados por coma>"
 *   },
 *   ...
 * }
 */
async function detallesColectasFantasma(dIdOwner, didsChoferesStr, desde, hasta, conn) {
    if (dIdOwner == null) throw new Error("dIdOwner requerido");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(desde || "")) throw new Error("desde debe ser YYYY-MM-DD");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(hasta || "")) throw new Error("hasta debe ser YYYY-MM-DD");

    // Parsear lista de choferes desde string
    const didsChoferes = String(didsChoferesStr || "")
        .split(",")
        .map(s => s.trim())
        .filter(Boolean)
        .map(n => Number(n))
        .filter(Number.isFinite);

    if (didsChoferes.length === 0) {
        return {};
    }

    // Construir placeholders para IN (?,?,?)
    const inPlaceholders = didsChoferes.map(() => "?").join(",");

    const sql = `
    SELECT
      DATE_FORMAT(dia, '%Y-%m-%d') AS dia,
      didsPaquete
    FROM home_app
    WHERE dIdOwner   = ?
      AND estado     = '0'
      AND didCliente <> 0
      AND didChofer  IN (${inPlaceholders})
      AND dia BETWEEN ? AND ?
      AND TRIM(didsPaquete) <> ''
      AND didsPaquete REGEXP '[0-9]'
    ORDER BY dia
  `;

    const params = [dIdOwner, ...didsChoferes, desde, hasta];
    const rows = await executeQuery(conn, sql, params, true);

    // YYYY-MM-DD -> DD-MM
    const fmtDia = (val) => {
        const s = String(val || "").trim();
        const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
        return m ? `${m[3]}-${m[2]}` : s;
    };

    // Agrupar por día: Map<"DD-MM", Set<number>>
    const porDia = new Map();
    const didsGlobal = new Set(); // todos los paquetes de todas las fechas

    for (const r of rows) {
        const diaKey = fmtDia(r.dia);

        const ids = String(r.didsPaquete || "")
            .split(",")
            .map(s => s.trim())
            .filter(Boolean)
            .map(n => Number(n))
            .filter(Number.isFinite);

        if (ids.length === 0) continue;

        if (!porDia.has(diaKey)) porDia.set(diaKey, new Set());
        const setDia = porDia.get(diaKey);

        ids.forEach(id => {
            setDia.add(id);
            didsGlobal.add(id);
        });
    }

    // Armar salida final
    const resultado = {};
    resultado.dids = [...didsGlobal].join(","); // todos los paquetes globales

    for (const [diaKey, idsSet] of porDia.entries()) {
        const idsArray = [...idsSet];
        resultado[diaKey] = {
            colectas: idsArray.length,
            dids: idsArray.join(",")
        };
    }

    return resultado;
}

module.exports = { detallesColectasFantasma };