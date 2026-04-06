const { executeQuery, getConnectionLocalPendientes } = require("./db");

const DEFAULT_START_DIA = "2026-01-01";
const QUERY_TIMEOUT_MS = 20 * 60 * 1000;

function formatDia(value) {
  if (!value) return "";

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    const y = value.getFullYear();
    const m = String(value.getMonth() + 1).padStart(2, "0");
    const d = String(value.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  const s = String(value).trim();
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(s);
  return m ? m[1] : s;
}

async function obtenerDias(conn, startDia, endDia) {
  const filtros = ["dia >= ?"];
  const params = [startDia];

  if (endDia) {
    filtros.push("dia <= ?");
    params.push(endDia);
  }

  const sql = `
    SELECT DISTINCT dia
    FROM home_app
    WHERE ${filtros.join(" AND ")}
    ORDER BY dia ASC
  `;

  const rows = await executeQuery(conn, sql, params, {
    timeoutMs: QUERY_TIMEOUT_MS,
  });

  return rows.map((row) => formatDia(row.dia)).filter(Boolean);
}

async function backfillDia(conn, dia) {
  const sql = `
    INSERT INTO home_app_resumen
      (didOwner, didCliente, didChofer, estado, dia, cantidad)
    SELECT
      didOwner,
      didCliente,
      didChofer,
      estado,
      dia,
      COALESCE(
        SUM(
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ),
        0
      ) AS cantidad
    FROM home_app
    WHERE dia = ?
    GROUP BY didOwner, didCliente, didChofer, estado, dia
    ON DUPLICATE KEY UPDATE
      cantidad = VALUES(cantidad),
      autofecha = CURRENT_TIMESTAMP
  `;

  return await executeQuery(conn, sql, [dia], {
    timeoutMs: QUERY_TIMEOUT_MS,
  });
}

async function contarCombosDelDia(conn, dia) {
  const sql = `
    SELECT COUNT(*) AS total
    FROM (
      SELECT 1
      FROM home_app
      WHERE dia = ?
      GROUP BY didOwner, didCliente, didChofer, estado, dia
    ) t
  `;

  const rows = await executeQuery(conn, sql, [dia], {
    timeoutMs: QUERY_TIMEOUT_MS,
  });

  return Number(rows?.[0]?.total ?? 0);
}

async function runBackfill(options = {}) {
  const startDia = formatDia(options.startDia || process.env.START_DIA || DEFAULT_START_DIA);
  const endDia = formatDia(options.endDia || process.env.END_DIA || "");
  const onlyDia = formatDia(options.onlyDia || process.env.ONLY_DIA || "");
  const continueOnError = options.continueOnError ?? process.env.CONTINUE_ON_ERROR === "1";

  const conn = await getConnectionLocalPendientes();
  const startedAt = Date.now();
  let procesados = 0;
  let errores = 0;

  try {
    const dias = onlyDia ? [onlyDia] : await obtenerDias(conn, startDia, endDia);

    console.log(`[BACKFILL] inicio | start=${startDia}${endDia ? ` end=${endDia}` : ""}${onlyDia ? ` only=${onlyDia}` : ""}`);
    console.log(`[BACKFILL] dias a procesar: ${dias.length}`);

    for (let i = 0; i < dias.length; i += 1) {
      const dia = dias[i];
      const t0 = Date.now();

      try {
        const totalCombos = await contarCombosDelDia(conn, dia);
        const result = await backfillDia(conn, dia);
        const elapsedMs = Date.now() - t0;
        procesados += 1;

        console.log(
          `[BACKFILL] ${i + 1}/${dias.length} dia=${dia} combos=${totalCombos} affected=${result?.affectedRows ?? 0} elapsedMs=${elapsedMs}`
        );
      } catch (error) {
        errores += 1;
        console.error(`[BACKFILL] ERROR dia=${dia}:`, error?.message || error);

        if (!continueOnError) {
          throw error;
        }
      }
    }

    const totalMs = Date.now() - startedAt;
    console.log(`[BACKFILL] fin | procesados=${procesados} errores=${errores} totalMs=${totalMs}`);
  } finally {
    try {
      conn?.release?.();
    } catch (_) {}
  }
}

if (require.main === module) {
  runBackfill()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
}

module.exports = { runBackfill };
