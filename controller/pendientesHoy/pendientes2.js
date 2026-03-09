const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);
const ESTADO_ANY = 999; // existió en el día según fecha_inicio
const ESTADO_ANY_EVENTO = 998; // existió en el día del evento

const TZ = "America/Argentina/Buenos_Aires";
const FETCH = 5000;
const LOOKUP_CHUNK = 500;

let PENDIENTES_HOY_RUNNING = false;

// ----------------- Utils -----------------
function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = Number.isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(ok);
}

const nEstado = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function ensure(o, k) {
  return (o[k] ??= {});
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function pairKey(owner, envio) {
  return `${owner}|${envio}`;
}

function normalizeEnvio(v) {
  return String(v);
}

function uniquePairs(rows, envioField = "didPaquete") {
  const map = new Map();

  for (const row of rows) {
    const owner = Number(row.didOwner);
    const envio = normalizeEnvio(row[envioField]);
    if (!owner || !envio) continue;
    map.set(pairKey(owner, envio), { owner, envio });
  }

  return Array.from(map.values());
}

function groupPairsByOwner(pairs) {
  const grouped = new Map();

  for (const p of pairs) {
    if (!grouped.has(p.owner)) grouped.set(p.owner, []);
    grouped.get(p.owner).push(p.envio);
  }

  return grouped;
}

function buildOwnerEnvioWhere(grouped, ownerField, envioField) {
  const parts = [];
  const params = [];

  for (const [owner, envios] of grouped.entries()) {
    if (!envios.length) continue;

    parts.push(
      `(${ownerField} = ? AND ${envioField} IN (${envios.map(() => "?").join(",")}))`
    );
    params.push(owner, ...envios);
  }

  if (!parts.length) {
    return { sql: "1=0", params: [] };
  }

  return {
    sql: parts.join(" OR "),
    params,
  };
}

function buildCurrentStateFromEstadoRow(row) {
  const estado = nEstado(row.estado);
  const dia = getDiaFromTS(row.fecha);
  const chofer = estado === 0 ? (Number(row.quien) || 0) : (Number(row.didChofer) || 0);
  const cliente = Number(row.didCliente) || 0;

  return {
    estado,
    didChofer: chofer,
    didCliente: cliente,
    dia,
  };
}

// ----------------- Cache en memoria por lote -----------------
// key: "owner|cliente|chofer|estado|dia"
const homeAppCache = new Map();

const makeKey = (owner, cliente, chofer, estado, dia) =>
  `${owner}|${cliente}|${chofer}|${estado}|${dia}`;

function parseCSVToSet(s) {
  const set = new Set();
  if (!s || !String(s).trim()) return set;
  for (const x of String(s).split(",").map((t) => t.trim()).filter(Boolean)) {
    set.add(x);
  }
  return set;
}

async function loadComboFromDB(conn, owner, cliente, chofer, estado, dia) {
  const sel = `
    SELECT didsPaquete, didsPaquetes_cierre
    FROM home_app
    WHERE didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?
    LIMIT 1
  `;

  const rows = await executeQuery(conn, sel, [owner, cliente, chofer, estado, dia]);

  if (rows.length) {
    return {
      historial: parseCSVToSet(rows[0].didsPaquete),
      cierre: parseCSVToSet(rows[0].didsPaquetes_cierre),
      dirty: false,
    };
  }

  return { historial: new Set(), cierre: new Set(), dirty: false };
}

async function getComboEntry(conn, owner, cliente, chofer, estado, dia) {
  const k = makeKey(owner, cliente, chofer, estado, dia);
  let entry = homeAppCache.get(k);

  if (!entry) {
    entry = await loadComboFromDB(conn, owner, cliente, chofer, estado, dia);
    homeAppCache.set(k, entry);
  }

  return entry;
}

function applyDeltas(entry, posArr, negArr, estado) {
  for (const p of posArr) {
    const k = String(p);
    entry.historial.add(k);
    entry.cierre.add(k);
  }

  // ANY / ANY_EVENTO nunca restan del cierre
  if (estado !== ESTADO_ANY && estado !== ESTADO_ANY_EVENTO) {
    for (const p of negArr) entry.cierre.delete(String(p));
  }

  entry.dirty = true;
}

async function flushEntry(conn, owner, cliente, chofer, estado, dia, entry) {
  if (!entry?.dirty) return;

  const didsPaqueteStr = Array.from(entry.historial).join(",");
  const didsPaquetesCierreStr = Array.from(entry.cierre).join(",");

  const upsert = `
    INSERT INTO home_app
      (didOwner, didCliente, didChofer, estado, didsPaquete, didsPaquetes_cierre, fecha, dia)
    VALUES
      (?, ?, ?, ?, ?, ?, NOW(), ?)
    ON DUPLICATE KEY UPDATE
      didsPaquete         = VALUES(didsPaquete),
      didsPaquetes_cierre = VALUES(didsPaquetes_cierre),
      autofecha           = NOW()
  `;

  await executeQuery(
    conn,
    upsert,
    [owner, cliente, chofer, estado, didsPaqueteStr, didsPaquetesCierreStr, dia],
    true
  );

  entry.dirty = false;
}

function resetState() {
  for (const k of Object.keys(Aprocesos)) delete Aprocesos[k];
  idsProcesados.length = 0;
  homeAppCache.clear();
}

// ----------------- Builders base -----------------
function pushNodoConGlobal(owner, cli, cho, est, dia, tipo, envio) {
  pushNodo(owner, cli, cho, est, dia, tipo, envio);
  pushNodo(owner, 0, 0, est, dia, tipo, envio);
}

function pushNodo(owner, cli, cho, est, dia, tipo, envio) {
  ensure(Aprocesos, owner);
  ensure(Aprocesos[owner], cli);
  ensure(Aprocesos[owner][cli], cho);
  ensure(Aprocesos[owner][cli][cho], est);
  ensure(Aprocesos[owner][cli][cho][est], dia);

  if (!Aprocesos[owner][cli][cho][est][dia][1]) {
    Aprocesos[owner][cli][cho][est][dia][1] = [];
  }
  if (!Aprocesos[owner][cli][cho][est][dia][0]) {
    Aprocesos[owner][cli][cho][est][dia][0] = [];
  }

  Aprocesos[owner][cli][cho][est][dia][tipo].push(String(envio));
}

// ----------------- Prefetch batch: previos desde CDC -----------------
// Trae el último estado conocido ANTES del primer id del lote para cada owner+envio afectado.
// Después, dentro del lote, se va actualizando en memoria.
async function preloadPrevEstadosDesdeCDC(conn, rowsEstado) {
  const prevMap = new Map();
  if (!rowsEstado.length) return prevMap;

  const minId = Math.min(...rowsEstado.map((r) => Number(r.id)).filter(Boolean));
  const pairs = uniquePairs(rowsEstado, "didPaquete");

  for (const pairChunk of chunkArray(pairs, LOOKUP_CHUNK)) {
    const grouped = groupPairsByOwner(pairChunk);
    const { sql, params } = buildOwnerEnvioWhere(grouped, "didOwner", "didPaquete");

    const q = `
      SELECT c.didOwner, c.didPaquete, c.didCliente, c.didChofer, c.quien, c.estado, c.fecha, c.fecha_inicio
      FROM cdc c
      INNER JOIN (
        SELECT didOwner, didPaquete, MAX(id) AS maxId
        FROM cdc
        WHERE id < ?
          AND didCliente IS NOT NULL
          AND (ejecutar = "estado" OR ejecutar = "asignaciones")
          AND (${sql})
        GROUP BY didOwner, didPaquete
      ) x
        ON x.maxId = c.id
    `;

    const rows = await executeQuery(conn, q, [minId, ...params]);

    for (const row of rows) {
      const owner = Number(row.didOwner);
      const envio = normalizeEnvio(row.didPaquete);
      const state = buildCurrentStateFromEstadoRow(row);

      if (!owner || !envio || state.estado === null) continue;
      prevMap.set(pairKey(owner, envio), state);
    }
  }

  return prevMap;
}

// ----------------- Prefetch batch: último chofer desde asignaciones -----------------
async function preloadPrevChoferesDesdeAsignaciones(conn, rowsAsignaciones) {
  const prevMap = new Map();
  if (!rowsAsignaciones.length) return prevMap;

  const pairs = uniquePairs(rowsAsignaciones, "didPaquete");

  for (const pairChunk of chunkArray(pairs, LOOKUP_CHUNK)) {
    const grouped = groupPairsByOwner(pairChunk);
    const { sql, params } = buildOwnerEnvioWhere(grouped, "didOwner", "didEnvio");

    const q = `
      SELECT a.didOwner, a.didEnvio, a.operador AS didChofer
      FROM asignaciones a
      INNER JOIN (
        SELECT didOwner, didEnvio, MAX(id) AS maxId
        FROM asignaciones
        WHERE operador IS NOT NULL
          AND (${sql})
        GROUP BY didOwner, didEnvio
      ) x
        ON x.maxId = a.id
    `;

    const rows = await executeQuery(conn, q, params);

    for (const row of rows) {
      const owner = Number(row.didOwner);
      const envio = normalizeEnvio(row.didEnvio);
      const chofer = Number(row.didChofer) || 0;

      if (!owner || !envio) continue;
      prevMap.set(pairKey(owner, envio), chofer);
    }
  }

  return prevMap;
}

// ----------------- Builder para disparador = 'estado' -----------------
async function buildAprocesosEstado(rows, prevStateMap) {
  for (const row of rows) {
    const OW = Number(row.didOwner);
    const CLI = Number(row.didCliente) || 0;
    const EST = nEstado(row.estado);

    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const diaEvento = getDiaFromTS(row.fecha);
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;

    const envio = normalizeEnvio(row.didPaquete);
    const CHO = EST === 0 ? (Number(row.quien) || 0) : (Number(row.didChofer) || 0);

    const prev = prevStateMap.get(pairKey(OW, envio));

    if (prev && prev.estado !== null) {
      const PREV_EST = nEstado(prev.estado);
      const PREV_CHO = Number(prev.didChofer) || 0;
      const PREV_CLI = Number(prev.didCliente) || 0;
      const PREV_DIA = prev.dia || dia;

      pushNodoConGlobal(OW, 0, 0, PREV_EST, PREV_DIA, 0, envio);
      pushNodo(OW, PREV_CLI, 0, PREV_EST, PREV_DIA, 0, envio);

      if (PREV_CHO !== 0) {
        pushNodo(OW, 0, PREV_CHO, PREV_EST, PREV_DIA, 0, envio);
        pushNodo(OW, PREV_CLI, PREV_CHO, PREV_EST, PREV_DIA, 0, envio);
      }

      if (ESTADOS_69.has(PREV_EST)) {
        pushNodoConGlobal(OW, 0, 0, 69, PREV_DIA, 0, envio);
        pushNodo(OW, PREV_CLI, 0, 69, PREV_DIA, 0, envio);

        if (PREV_CHO !== 0) {
          pushNodo(OW, 0, PREV_CHO, 69, PREV_DIA, 0, envio);
          pushNodo(OW, PREV_CLI, PREV_CHO, 69, PREV_DIA, 0, envio);
        }
      }

      if (ESTADOS_70.has(PREV_EST)) {
        pushNodoConGlobal(OW, 0, 0, 70, PREV_DIA, 0, envio);
        pushNodo(OW, PREV_CLI, 0, 70, PREV_DIA, 0, envio);

        if (PREV_CHO !== 0) {
          pushNodo(OW, 0, PREV_CHO, 70, PREV_DIA, 0, envio);
          pushNodo(OW, PREV_CLI, PREV_CHO, 70, PREV_DIA, 0, envio);
        }
      }
    }

    // positivos actuales
    pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
    pushNodo(OW, CLI, 0, EST, dia, 1, envio);

    pushNodo(OW, 0, 0, ESTADO_ANY, diaPaquete, 1, envio);
    pushNodo(OW, CLI, 0, ESTADO_ANY, diaPaquete, 1, envio);

    pushNodo(OW, 0, 0, ESTADO_ANY_EVENTO, dia, 1, envio);
    pushNodo(OW, CLI, 0, ESTADO_ANY_EVENTO, dia, 1, envio);

    if (ESTADOS_69.has(EST)) {
      pushNodoConGlobal(OW, 0, 0, 69, dia, 1, envio);
      pushNodo(OW, CLI, 0, 69, dia, 1, envio);
    } else {
      pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
      pushNodo(OW, CLI, 0, 69, dia, 0, envio);
    }

    if (ESTADOS_70.has(EST)) {
      pushNodoConGlobal(OW, 0, 0, 70, dia, 1, envio);
      pushNodo(OW, CLI, 0, 70, dia, 1, envio);
    } else {
      pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
      pushNodo(OW, CLI, 0, 70, dia, 0, envio);
    }

    if (CHO !== 0) {
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, CHO, 69, dia, 1, envio);
        pushNodo(OW, 0, CHO, 69, dia, 1, envio);
      } else {
        pushNodo(OW, CLI, CHO, 69, dia, 0, envio);
        pushNodo(OW, 0, CHO, 69, dia, 0, envio);
      }

      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, CHO, 70, dia, 1, envio);
        pushNodo(OW, 0, CHO, 70, dia, 1, envio);
      } else {
        pushNodo(OW, CLI, CHO, 70, dia, 0, envio);
        pushNodo(OW, 0, CHO, 70, dia, 0, envio);
      }
    }

    prevStateMap.set(pairKey(OW, envio), {
      estado: EST,
      didChofer: CHO,
      didCliente: CLI,
      dia,
    });

    idsProcesados.push(row.id);
  }

  return Aprocesos;
}

// ----------------- Builder para disparador = 'asignaciones' -----------------
async function buildAprocesosAsignaciones(rows, prevChoferMap) {
  for (const row of rows) {
    const OW = Number(row.didOwner);
    const CLI = Number(row.didCliente) || 0;
    const CHO = Number(row.didChofer) || 0;
    const EST = nEstado(row.estado);
    const envio = normalizeEnvio(row.didPaquete);

    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const diaEvento = getDiaFromTS(row.fecha);
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;

    if (CHO !== 0) {
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
      pushNodo(OW, CLI, 0, EST, dia, 1, envio);

      pushNodo(OW, 0, 0, ESTADO_ANY, diaPaquete, 1, envio);
      pushNodo(OW, CLI, 0, ESTADO_ANY, diaPaquete, 1, envio);

      pushNodo(OW, 0, 0, ESTADO_ANY_EVENTO, dia, 1, envio);
      pushNodo(OW, CLI, 0, ESTADO_ANY_EVENTO, dia, 1, envio);

      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, CHO, 69, dia, 1, envio);
        pushNodo(OW, 0, CHO, 69, dia, 1, envio);

        pushNodoConGlobal(OW, 0, 0, 69, dia, 1, envio);
        pushNodo(OW, CLI, 0, 69, dia, 1, envio);
      } else {
        pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
        pushNodo(OW, CLI, 0, 69, dia, 0, envio);
      }

      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, CHO, 70, dia, 1, envio);
        pushNodo(OW, 0, CHO, 70, dia, 1, envio);

        pushNodoConGlobal(OW, 0, 0, 70, dia, 1, envio);
        pushNodo(OW, CLI, 0, 70, dia, 1, envio);
      } else {
        pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
        pushNodo(OW, CLI, 0, 70, dia, 0, envio);
      }
    }

    const prevChofer = Number(prevChoferMap.get(pairKey(OW, envio))) || 0;

    if (prevChofer !== 0 && prevChofer !== CHO) {
      pushNodo(OW, CLI, prevChofer, EST, dia, 0, envio);
      pushNodo(OW, 0, prevChofer, EST, dia, 0, envio);

      pushNodoConGlobal(OW, 0, 0, EST, dia, 0, envio);
      pushNodo(OW, CLI, 0, EST, dia, 0, envio);

      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, prevChofer, 69, dia, 0, envio);
        pushNodo(OW, 0, prevChofer, 69, dia, 0, envio);

        pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
        pushNodo(OW, CLI, 0, 69, dia, 0, envio);
      }

      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, prevChofer, 70, dia, 0, envio);
        pushNodo(OW, 0, prevChofer, 70, dia, 0, envio);

        pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
        pushNodo(OW, CLI, 0, 70, dia, 0, envio);
      }
    }

    if (CHO !== 0) {
      prevChoferMap.set(pairKey(OW, envio), CHO);
    }

    idsProcesados.push(row.id);
  }

  return Aprocesos;
}

// ----------------- Aplicar batch -----------------
async function aplicarAprocesosAHomeApp(conn) {
  const COMMIT_EVERY = 300;
  let ops = 0;

  const begin = async () => executeQuery(conn, "START TRANSACTION");
  const commit = async () => executeQuery(conn, "COMMIT");
  const rollback = async () => executeQuery(conn, "ROLLBACK");

  await begin();

  try {
    for (const ownerKey in Aprocesos) {
      const owner = Number(ownerKey);
      const porCliente = Aprocesos[ownerKey];

      for (const clienteKey in porCliente) {
        const cliente = Number(clienteKey);
        const porChofer = porCliente[clienteKey];

        for (const choferKey in porChofer) {
          const chofer = Number(choferKey);
          const porEstado = porChofer[choferKey];

          for (const estadoKey in porEstado) {
            const estado = Number(estadoKey);
            const porDia = porEstado[estadoKey];

            for (const dia in porDia) {
              const nodo = porDia[dia];
              const pos = [...new Set(nodo?.[1] || [])];
              const neg = [...new Set(nodo?.[0] || [])];

              if (!pos.length && !neg.length) continue;

              const entry = await getComboEntry(conn, owner, cliente, chofer, estado, dia);
              applyDeltas(entry, pos, neg, estado);
              await flushEntry(conn, owner, cliente, chofer, estado, dia, entry);

              ops += 1;

              if (ops % COMMIT_EVERY === 0) {
                await commit();
                await begin();
              }
            }
          }
        }
      }
    }

    await commit();
  } catch (e) {
    try { await rollback(); } catch (_) { }
    throw e;
  }

  if (idsProcesados.length > 0) {
    const CHUNK = 1000;

    for (let i = 0; i < idsProcesados.length; i += CHUNK) {
      const slice = idsProcesados.slice(i, i + CHUNK);

      const updCdc = `
        UPDATE cdc
        SET procesado = 1, fProcesado = NOW()
        WHERE id IN (${slice.map(() => "?").join(",")})
      `;

      await executeQuery(conn, updCdc, slice);
    }
  }
}

// ----------------- Lote principal -----------------
async function procesarLote(conn) {
  resetState();

  const selectCDC = `
    SELECT id, didOwner, didPaquete, didCliente, didChofer, quien, estado, disparador, ejecutar, fecha, fecha_inicio
    FROM cdc
    WHERE procesado = 0
      AND (ejecutar = "estado" OR ejecutar = "asignaciones")
      AND didCliente IS NOT NULL
    ORDER BY id ASC
    LIMIT ?
  `;

  const rows = await executeQuery(conn, selectCDC, [FETCH]);

  if (!rows.length) {
    return { ok: true, fetched: 0, processedIds: 0 };
  }

  const rowsEstado = rows.filter((r) => r.disparador === "estado");
  const rowsAsignaciones = rows.filter((r) => r.disparador === "asignaciones");

  const prevStateMap = await preloadPrevEstadosDesdeCDC(conn, rowsEstado);
  const prevChoferMap = await preloadPrevChoferesDesdeAsignaciones(conn, rowsAsignaciones);

  await buildAprocesosEstado(rowsEstado, prevStateMap);
  await buildAprocesosAsignaciones(rowsAsignaciones, prevChoferMap);
  await aplicarAprocesosAHomeApp(conn);

  return {
    ok: true,
    fetched: rows.length,
    processedIds: idsProcesados.length,
    lastId: rows[rows.length - 1]?.id || null,
  };
}

// ----------------- Batch principal en loop -----------------
async function pendientesHoy() {
  if (PENDIENTES_HOY_RUNNING) {
    console.log("⏭️ pendientesHoy ya está corriendo, salteo esta ejecución");
    return { ok: true, skipped: true };
  }

  PENDIENTES_HOY_RUNNING = true;

  const conn = await getConnectionLocalPendientes();
  let fatalErr = null;

  try {
    let totalFetched = 0;
    let totalProcessed = 0;
    let batches = 0;

    while (true) {
      const t0 = Date.now();
      const result = await procesarLote(conn);

      if (!result.fetched) {
        console.log("✅ pendientesHoy: no hay más registros con procesado=0");
        break;
      }

      batches += 1;
      totalFetched += result.fetched;
      totalProcessed += result.processedIds;

      const elapsedMs = Date.now() - t0;

      console.log(
        `✅ lote ${batches} | fetched=${result.fetched} | processed=${result.processedIds} | lastId=${result.lastId} | tiempo=${(elapsedMs / 1000).toFixed(1)}s`
      );

      // pequeño respiro para no castigar tanto a la DB
      await sleep(50);
    }

    return {
      ok: true,
      fetched: totalFetched,
      processedIds: totalProcessed,
      batches,
    };
  } catch (err) {
    fatalErr = err;
    console.error("❌ Error batch:", err);
    throw err;
  } finally {
    PENDIENTES_HOY_RUNNING = false;

    try {
      const code = fatalErr?.code;
      const msg = String(fatalErr?.message || "").toLowerCase();

      const shouldDestroy =
        fatalErr?.__shouldDestroyConnection ||
        code === "PROTOCOL_SEQUENCE_TIMEOUT" ||
        msg.includes("query inactivity timeout");

      if (shouldDestroy && typeof conn.destroy === "function") {
        console.log("🔥 Destruyendo conexión DW (timeout/protocol)");
        conn.destroy();
      } else if (conn?.release) {
        conn.release();
      }
    } catch (_) { }
  }
}

module.exports = { pendientesHoy };