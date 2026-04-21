const { getConnectionLocalCdc, getConnection, executeQuery, closeDWPool, redisClient } = require("./db");

function parseArgs(argv) {
  const args = { batch: 100, dryRun: false, all: false, pauseMs: 0 };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];

    if (token === "--dry-run") {
      args.dryRun = true;
      continue;
    }

    if (token === "--all") {
      args.all = true;
      continue;
    }

    if (token === "--batch" || token === "-b") {
      const value = Number(argv[i + 1]);
      if (Number.isFinite(value) && value > 0) {
        args.batch = Math.trunc(value);
        i += 1;
      }
      continue;
    }

    if (token === "--pause-ms") {
      const value = Number(argv[i + 1]);
      if (Number.isFinite(value) && value >= 0) {
        args.pauseMs = Math.trunc(value);
        i += 1;
      }
    }
  }

  return args;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function closeConn(conn) {
  try {
    if (conn?.release) conn.release();
    else if (conn?.end) await conn.end();
    else if (conn?.destroy) conn.destroy();
  } catch (error) {
    console.error("Error cerrando conexion:", error?.message || error);
  }
}

function buildLookupMap(rows) {
  const map = new Map();
  for (const row of rows) {
    if (!row) continue;
    const owner = Number(row.didOwner);
    const paquete = Number(row.didPaquete);
    const didCliente = Number(row.didClienteRecuperado ?? row.didCliente ?? 0);
    if (!owner || !paquete || !didCliente) continue;
    map.set(`${owner}|${paquete}`, didCliente);
  }
  return map;
}

async function fetchPendingCount(conn) {
  const rows = await executeQuery(
    conn,
    `
    SELECT COUNT(*) AS total
    FROM cdc
    WHERE procesado = 2
      AND didCliente IS NULL
    `
  );

  return Number(rows?.[0]?.total || 0);
}

async function fetchDwCandidates(conn, batch, minIdExclusive = 0) {
  return await executeQuery(
    conn,
    `
    SELECT c.id, c.didOwner, c.didPaquete, c.disparador, c.didCliente,
           COALESCE(e_act.didCliente, e_any.didCliente) AS didClienteRecuperado
    FROM cdc c
    LEFT JOIN envios e_act
      ON e_act.didOwner = c.didOwner
     AND e_act.didEnvio = c.didPaquete
     AND e_act.elim = 0
     AND e_act.superado = 0
    LEFT JOIN envios e_any
      ON e_any.didOwner = c.didOwner
     AND e_any.didEnvio = c.didPaquete
    WHERE c.procesado = 2
      AND c.didCliente IS NULL
      AND c.id > ?
    ORDER BY c.id ASC
    LIMIT ?
    `,
    [minIdExclusive, batch]
  );
}

async function fetchSourceDidCliente(rows) {
  const recovered = new Map();
  const byOwner = new Map();

  for (const row of rows) {
    const owner = Number(row.didOwner);
    const paquete = Number(row.didPaquete);
    if (!owner || !paquete) continue;

    if (!byOwner.has(owner)) byOwner.set(owner, new Set());
    byOwner.get(owner).add(paquete);
  }

  for (const [owner, paquetesSet] of byOwner.entries()) {
    let sourceConn;
    try {
      const paquetes = Array.from(paquetesSet);
      if (!paquetes.length) continue;

      sourceConn = await getConnection(owner);
      const sql = `
        SELECT did, didCliente
        FROM envios
        WHERE did IN (${paquetes.map(() => "?").join(",")})
      `;
      const sourceRows = await executeQuery(sourceConn, sql, paquetes, { timeoutMs: 120000 });

      let recuperadosValidos = 0;
      for (const row of sourceRows) {
        const paquete = Number(row.did);
        const didCliente = Number(row.didCliente || 0);
        if (!paquete || !didCliente) continue;
        recuperadosValidos += 1;
        recovered.set(`${owner}|${paquete}`, didCliente);
      }

      console.log("[CDC-REPAIR] lookup origen", {
        didOwner: owner,
        pedidos: paquetes.length,
        encontrados: sourceRows.length,
        recuperadosValidos,
      });
    } catch (error) {
      console.error(`[CDC-REPAIR] error buscando origen didOwner=${owner}:`, error?.message || error);
    } finally {
      await closeConn(sourceConn);
    }
  }

  return recovered;
}

function enrichRows(rows, dwMap, sourceMap) {
  return rows.map((row) => {
    const key = `${Number(row.didOwner)}|${Number(row.didPaquete)}`;
    const didClienteDw = dwMap.get(key) ?? null;
    const didClienteSource = sourceMap.get(key) ?? null;
    const didClienteFinal = didClienteDw ?? didClienteSource ?? null;

    return {
      ...row,
      didClienteDw,
      didClienteSource,
      didClienteFinal,
      recuperadoPor: didClienteDw ? "dw" : didClienteSource ? "origen" : null,
    };
  });
}

async function updateDwRows(conn, rows) {
  if (!rows.length) return { affectedRows: 0, changedRows: 0 };

  const ids = rows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
  const result = await executeQuery(
    conn,
    `
    UPDATE cdc c
    LEFT JOIN envios e_act
      ON e_act.didOwner = c.didOwner
     AND e_act.didEnvio = c.didPaquete
     AND e_act.elim = 0
     AND e_act.superado = 0
    LEFT JOIN envios e_any
      ON e_any.didOwner = c.didOwner
     AND e_any.didEnvio = c.didPaquete
    SET c.didCliente = COALESCE(e_act.didCliente, e_any.didCliente, c.didCliente),
        c.procesado = 0,
        c.fProcesado = NULL
    WHERE c.id IN (${ids.map(() => "?").join(",")})
      AND c.procesado = 2
      AND c.didCliente IS NULL
      AND COALESCE(e_act.didCliente, e_any.didCliente) IS NOT NULL
    `,
    ids,
    { timeoutMs: 120000 }
  );

  return {
    affectedRows: Number(result?.affectedRows || 0),
    changedRows: Number(result?.changedRows || 0),
  };
}

async function updateOriginRows(conn, rows) {
  let actualizados = 0;

  for (const row of rows) {
    const result = await executeQuery(
      conn,
      `
      UPDATE cdc
      SET didCliente = ?,
          procesado = 0,
          fProcesado = NULL
      WHERE id = ?
        AND procesado = 2
        AND didCliente IS NULL
      `,
      [Number(row.didClienteFinal), Number(row.id)],
      { timeoutMs: 120000 }
    );

    const affectedRows = Number(result?.affectedRows || 0);
    actualizados += affectedRows;

    console.log("[CDC-REPAIR] actualizacion origen aplicada", {
      id: Number(row.id),
      didOwner: Number(row.didOwner),
      didPaquete: Number(row.didPaquete),
      didCliente: Number(row.didClienteFinal),
      affectedRows,
    });
  }

  return actualizados;
}

async function processBatch(conn, { batch, dryRun, minIdExclusive }) {
  const rows = await fetchDwCandidates(conn, batch, minIdExclusive);

  if (!rows.length) {
    return {
      empty: true,
      fetched: 0,
      reparables: 0,
      sinMatch: 0,
      nextCursor: minIdExclusive,
      updated: 0,
    };
  }

  const dwMap = buildLookupMap(
    rows.filter((row) => row.didClienteRecuperado !== null && row.didClienteRecuperado !== undefined)
  );
  const rowsMissing = rows.filter((row) => !dwMap.has(`${Number(row.didOwner)}|${Number(row.didPaquete)}`));
  const sourceMap = await fetchSourceDidCliente(rowsMissing);
  const enrichedRows = enrichRows(rows, dwMap, sourceMap);

  const reparables = enrichedRows.filter((row) => row.didClienteFinal !== null && row.didClienteFinal !== undefined);
  const sinMatch = enrichedRows.filter((row) => row.didClienteFinal === null || row.didClienteFinal === undefined);
  const firstId = Number(rows[0]?.id || 0);
  const lastId = Number(rows[rows.length - 1]?.id || minIdExclusive);

  console.log("[CDC-REPAIR] lote analizado", {
    batchSolicitado: batch,
    encontrados: rows.length,
    reparables: reparables.length,
    reparadosPorDw: reparables.filter((row) => row.recuperadoPor === "dw").length,
    reparadosPorOrigen: reparables.filter((row) => row.recuperadoPor === "origen").length,
    sinMatch: sinMatch.length,
    dryRun,
    firstId,
    lastId,
  });

  if (reparables.length) {
    console.log("[CDC-REPAIR] muestra reparables", reparables.slice(0, 10).map((row) => ({
      id: row.id,
      didOwner: row.didOwner,
      didPaquete: row.didPaquete,
      disparador: row.disparador,
      didClienteFinal: row.didClienteFinal,
      recuperadoPor: row.recuperadoPor,
    })));
  }

  if (sinMatch.length) {
    console.log("[CDC-REPAIR] muestra sin match", sinMatch.slice(0, 10).map((row) => ({
      id: row.id,
      didOwner: row.didOwner,
      didPaquete: row.didPaquete,
      disparador: row.disparador,
    })));
  }

  let updated = 0;

  if (!dryRun && reparables.length) {
    const dwRows = reparables.filter((row) => row.recuperadoPor === "dw");
    const originRows = reparables.filter((row) => row.recuperadoPor === "origen");

    if (dwRows.length) {
      const resultDw = await updateDwRows(conn, dwRows);
      updated += resultDw.affectedRows;
      console.log("[CDC-REPAIR] actualizacion DW aplicada", {
        idsIntentados: dwRows.length,
        affectedRows: resultDw.affectedRows,
        changedRows: resultDw.changedRows,
      });
    }

    if (originRows.length) {
      updated += await updateOriginRows(conn, originRows);
    }
  }

  return {
    empty: false,
    fetched: rows.length,
    reparables: reparables.length,
    sinMatch: sinMatch.length,
    nextCursor: lastId,
    updated,
  };
}

async function main() {
  const { batch, dryRun, all, pauseMs } = parseArgs(process.argv.slice(2));
  let conn;

  try {
    conn = await getConnectionLocalCdc();

    const pendingBefore = await fetchPendingCount(conn);
    console.log("[CDC-REPAIR] pendientes iniciales", {
      procesado2SinCliente: pendingBefore,
      batch,
      dryRun,
      all,
      pauseMs,
    });

    let cursor = 0;
    let loops = 0;
    let totalFetched = 0;
    let totalReparables = 0;
    let totalSinMatch = 0;
    let totalUpdated = 0;

    do {
      loops += 1;
      const result = await processBatch(conn, {
        batch,
        dryRun,
        minIdExclusive: cursor,
      });

      if (result.empty) break;

      cursor = result.nextCursor;
      totalFetched += result.fetched;
      totalReparables += result.reparables;
      totalSinMatch += result.sinMatch;
      totalUpdated += result.updated;

      if (!all) break;
      if (pauseMs > 0) await sleep(pauseMs);
    } while (all);

    const pendingAfter = dryRun ? pendingBefore : await fetchPendingCount(conn);

    console.log("[CDC-REPAIR] resumen final", {
      loops,
      totalFetched,
      totalReparables,
      totalSinMatch,
      totalUpdated,
      pendingBefore,
      pendingAfter,
      dryRun,
      all,
      ultimoIdRecorrido: cursor,
    });
  } finally {
    await closeConn(conn);
    try { await closeDWPool(); } catch (_) {}
    try { await redisClient.quit(); } catch (_) {}
  }
}

main().catch((error) => {
  console.error("[CDC-REPAIR] error fatal:", error?.message || error);
  process.exit(1);
});
