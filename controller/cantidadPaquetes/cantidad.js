const { executeQuery } = require("../../db");

async function cantidadGlobal(conn, fecha) {
  const sql = `
    WITH RECURSIVE split AS (
      SELECT
        TRIM(SUBSTRING_INDEX(ha.didsPaquete, ',', 1)) AS token,
        CASE
          WHEN INSTR(ha.didsPaquete, ',') > 0
            THEN SUBSTRING(ha.didsPaquete, INSTR(ha.didsPaquete, ',') + 1)
          ELSE ''
        END AS rest
      FROM home_app ha
      WHERE ha.dia = ?
        AND ha.didOwner = 0
        AND ha.didChofer = 0
        AND ha.didCliente = 0
        AND ha.didsPaquete IS NOT NULL
        AND TRIM(ha.didsPaquete) <> ''

      UNION ALL

      SELECT
        TRIM(SUBSTRING_INDEX(rest, ',', 1)) AS token,
        CASE
          WHEN INSTR(rest, ',') > 0
            THEN SUBSTRING(rest, INSTR(rest, ',') + 1)
          ELSE ''
        END AS rest
      FROM split
      WHERE rest <> ''
    )
    SELECT
      COUNT(DISTINCT token) AS cantidad
    FROM split
    WHERE token <> '';
  `;

  const rows = await executeQuery(conn, sql, [fecha], true);
  const cantidad = Number(rows?.[0]?.cantidad ?? 0);

  return { ok: true, cantidad, fecha };
}

module.exports = { cantidadGlobal };
