const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];
const idsProcesados9 = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);
const ESTADO_ANY = 999; // existió en el día según fecha_inicio
const ESTADO_ANY_EVENTO = 998; // existió en el día del evento

const TZ = "America/Argentina/Buenos_Aires";

// Tunables
const FETCH = Number(process.env.PENDIENTES_FETCH || 2000);
const LOOKUP_CHUNK = Number(process.env.PENDIENTES_LOOKUP_CHUNK || 200);
const COMMIT_EVERY = Number(process.env.PENDIENTES_COMMIT_EVERY || 300);
const LOOP_PAUSE_MS = Number(process.env.PENDIENTES_LOOP_PAUSE_MS || 50);
const SLOW_BATCH_MS = Number(process.env.PENDIENTES_SLOW_BATCH_MS || 30000);
const LOG_EVERY_BATCH = Number(process.env.PENDIENTES_LOG_EVERY_BATCH || 1);

let PENDIENTES_HOY_RUNNING = false;

// ----------------- Utils -----------------
function nowMs() {
  return Date.now();
}

function fmtMs(ms) {
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)}s`;
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return `${m}m ${rem.toFixed(1)}s`;
}

function fmtNum(n) {
  return Number(n || 0).toLocaleString("es-AR");
}

function rate(count, ms) {
  if (!ms || ms <= 0) return "0.00";
  return (count / (ms / 1000)).toFixed(2);
}

function memorySnapshot() {
  const m = process.memoryUsage();
  return {
    rssMB: +(m.rss / 1024 / 1024).toFixed(1),
    heapTotalMB: +(m.heapTotal / 1024 / 1024).toFixed(1),
    heapUsedMB: +(m.heapUsed / 1024 / 1024).toFixed(1),
    externalMB: +(m.external / 1024 / 1024).toFixed(1),
  };
}

function logMemory(prefix = "MEM") {
  const m = memorySnapshot();
  console.log(
    `🧠 [${prefix}] rss=${m.rssMB}MB heapUsed=${m.heapUsedMB}MB heapTotal=${m.heapTotalMB}MB external=${m.externalMB}MB`
  );
}

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

function createMetrics() {
  return {
    totalStartMs: nowMs(),
    batchNo: 0,
    totalFetched: 0,
    totalProcessed: 0,
    totalEstadoRows: 0,
    totalAsignacionesRows: 0,
    totalApplyOps: 0,
    totalFlushes: 0,
    totalMarkedProcessed: 0,
    totalMarked9: 0,
    totalPreloadEstadoPairs: 0,
    totalPreloadChoferPairs: 0,
    maxBatchMs: 0,
    minBatchMs: null,
  };
}

function logBatchSummary(batchMetrics, totals) {
  const slowMark = batchMetrics.totalMs >= SLOW_BATCH_MS ? " 🐢 LENTO" : "";

  console.log(
    `📦 [LOTE ${batchMetrics.batchNo}] fetched=${fmtNum(batchMetrics.fetched)} ` +
    `descartados9=${fmtNum(batchMetrics.rowsDiscarded || 0)} ` +
    `nullCliente=${fmtNum(batchMetrics.rowsDidClienteNull || 0)} ` +
    `ejecutarInvalido=${fmtNum(batchMetrics.rowsEjecutarInvalido || 0)} ` +
    `estado=${fmtNum(batchMetrics.rowsEstado)} asignaciones=${fmtNum(batchMetrics.rowsAsignaciones)} ` +
    `processed=${fmtNum(batchMetrics.processedIds)} marked9=${fmtNum(batchMetrics.marked9 || 0)} ` +
    `applyOps=${fmtNum(batchMetrics.applyOps)} flushes=${fmtNum(batchMetrics.flushes)} ` +
    `lastId=${batchMetrics.lastId} total=${fmtMs(batchMetrics.totalMs)} ${slowMark}`
  );

  console.log(
    `   ⏱ fases | select=${fmtMs(batchMetrics.selectMs)} prevEstado=${fmtMs(batchMetrics.preloadEstadoMs)} ` +
    `prevChofer=${fmtMs(batchMetrics.preloadChoferMs)} buildEstado=${fmtMs(batchMetrics.buildEstadoMs)} ` +
    `buildAsig=${fmtMs(batchMetrics.buildAsignacionesMs)} apply=${fmtMs(batchMetrics.applyMs)}`
  );

  console.log(
    `   🚀 lote | fetched/s=${rate(batchMetrics.fetched, batchMetrics.totalMs)} ` +
    `processed/s=${rate(batchMetrics.processedIds, batchMetrics.totalMs)} ` +
    `applyOps/s=${rate(batchMetrics.applyOps, batchMetrics.totalMs)}`
  );

  console.log(
    `   📊 total | batches=${fmtNum(totals.batchNo)} fetched=${fmtNum(totals.totalFetched)} ` +
    `processed=${fmtNum(totals.totalProcessed)} marked=${fmtNum(totals.totalMarkedProcessed)} ` +
    `marked9=${fmtNum(totals.totalMarked9)} ` +
    `avgProcessed/s=${rate(totals.totalProcessed, nowMs() - totals.totalStartMs)}`
  );

  logMemory(`LOTE ${batchMetrics.batchNo}`);
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

  if (estado !== ESTADO_ANY && estado !== ESTADO_ANY_EVENTO) {
    for (const p of negArr) entry.cierre.delete(String(p));
  }

  entry.dirty = true;
}

async function flushEntry(conn, owner, cliente, chofer, estado, dia, entry) {
  if (!entry?.dirty) return false;

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
  return true;
}

function resetState() {
  for (const k of Object.keys(Aprocesos)) delete Aprocesos[k];
  idsProcesados.length = 0;
  idsProcesados9.length = 0;
  homeAppCache.clear();
}

function esEjecutarValido(v) {
  return v === "estado" || v === "asignaciones";
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
async function aplicarAprocesosAHomeApp(conn, batchMetrics) {
  let ops = 0;
  let flushes = 0;

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
              const didFlush = await flushEntry(conn, owner, cliente, chofer, estado, dia, entry);

              ops += 1;
              if (didFlush) flushes += 1;

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

  if (idsProcesados9.length > 0) {
    const CHUNK = 1000;

    for (let i = 0; i < idsProcesados9.length; i += CHUNK) {
      const slice = idsProcesados9.slice(i, i + CHUNK);

      const updCdc9 = `
        UPDATE cdc
        SET procesado = 9, fProcesado = NOW()
        WHERE id IN (${slice.map(() => "?").join(",")})
      `;

      await executeQuery(conn, updCdc9, slice);
    }
  }

  batchMetrics.applyOps = ops;
  batchMetrics.flushes = flushes;
  batchMetrics.markedProcessed = idsProcesados.length;
  batchMetrics.marked9 = idsProcesados9.length;

  return {
    ops,
    flushes,
    markedProcessed: idsProcesados.length,
    marked9: idsProcesados9.length,
  };
}

// ----------------- Lote principal -----------------
async function procesarLote(conn, totals) {
  resetState();

  const batchMetrics = {
    batchNo: totals.batchNo + 1,
    fetched: 0,
    rowsEstado: 0,
    rowsAsignaciones: 0,
    rowsDidClienteNull: 0,
    rowsEjecutarInvalido: 0,
    rowsDiscarded: 0,
    processedIds: 0,
    lastId: null,
    selectMs: 0,
    preloadEstadoMs: 0,
    preloadChoferMs: 0,
    buildEstadoMs: 0,
    buildAsignacionesMs: 0,
    applyMs: 0,
    totalMs: 0,
    applyOps: 0,
    flushes: 0,
    markedProcessed: 0,
    marked9: 0,
  };

  const lotStart = nowMs();

  // SELECT
  {
    const t = nowMs();

    const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, quien, estado, disparador, ejecutar, fecha, fecha_inicio
      FROM cdc
      WHERE procesado = 0
      ORDER BY id ASC
      LIMIT ?
    `;

    batchMetrics.rows = await executeQuery(conn, selectCDC, [FETCH]);
    batchMetrics.selectMs = nowMs() - t;
  }

  const rows = batchMetrics.rows;

  if (!rows.length) {
    batchMetrics.totalMs = nowMs() - lotStart;
    delete batchMetrics.rows;
    return { batchMetrics, done: true };
  }

  batchMetrics.fetched = rows.length;
  batchMetrics.lastId = rows[rows.length - 1]?.id || null;

  const rowsValidas = [];

  for (const row of rows) {
    if (row.didCliente == null) {
      idsProcesados9.push(row.id);
      batchMetrics.rowsDidClienteNull += 1;
      batchMetrics.rowsDiscarded += 1;
      continue;
    }

    if (!esEjecutarValido(row.ejecutar)) {
      idsProcesados9.push(row.id);
      batchMetrics.rowsEjecutarInvalido += 1;
      batchMetrics.rowsDiscarded += 1;
      continue;
    }

    rowsValidas.push(row);
  }

  const rowsEstado = rowsValidas.filter((r) => r.disparador === "estado");
  const rowsAsignaciones = rowsValidas.filter((r) => r.disparador === "asignaciones");

  for (const row of rowsValidas) {
    if (row.disparador !== "estado" && row.disparador !== "asignaciones") {
      idsProcesados9.push(row.id);
      batchMetrics.rowsDiscarded += 1;
    }
  }

  const rowsEstadoFinal = rowsEstado;
  const rowsAsignacionesFinal = rowsAsignaciones;

  batchMetrics.rowsEstado = rowsEstadoFinal.length;
  batchMetrics.rowsAsignaciones = rowsAsignacionesFinal.length;

  // PRELOAD ESTADO
  let prevStateMap;
  {
    const t = nowMs();
    prevStateMap = await preloadPrevEstadosDesdeCDC(conn, rowsEstadoFinal);
    batchMetrics.preloadEstadoMs = nowMs() - t;
    totals.totalPreloadEstadoPairs += prevStateMap.size;
  }

  // PRELOAD CHOFER
  let prevChoferMap;
  {
    const t = nowMs();
    prevChoferMap = await preloadPrevChoferesDesdeAsignaciones(conn, rowsAsignacionesFinal);
    batchMetrics.preloadChoferMs = nowMs() - t;
    totals.totalPreloadChoferPairs += prevChoferMap.size;
  }

  // BUILD ESTADO
  {
    const t = nowMs();
    await buildAprocesosEstado(rowsEstadoFinal, prevStateMap);
    batchMetrics.buildEstadoMs = nowMs() - t;
  }

  // BUILD ASIGNACIONES
  {
    const t = nowMs();
    await buildAprocesosAsignaciones(rowsAsignacionesFinal, prevChoferMap);
    batchMetrics.buildAsignacionesMs = nowMs() - t;
  }

  // APPLY
  {
    const t = nowMs();
    await aplicarAprocesosAHomeApp(conn, batchMetrics);
    batchMetrics.applyMs = nowMs() - t;
  }

  batchMetrics.processedIds = idsProcesados.length;
  batchMetrics.totalMs = nowMs() - lotStart;

  delete batchMetrics.rows;

  totals.batchNo += 1;
  totals.totalFetched += batchMetrics.fetched;
  totals.totalProcessed += batchMetrics.processedIds;
  totals.totalEstadoRows += batchMetrics.rowsEstado;
  totals.totalAsignacionesRows += batchMetrics.rowsAsignaciones;
  totals.totalApplyOps += batchMetrics.applyOps;
  totals.totalFlushes += batchMetrics.flushes;
  totals.totalMarkedProcessed += batchMetrics.markedProcessed;
  totals.totalMarked9 += batchMetrics.marked9;
  totals.maxBatchMs = Math.max(totals.maxBatchMs, batchMetrics.totalMs);

  if (totals.minBatchMs === null || batchMetrics.totalMs < totals.minBatchMs) {
    totals.minBatchMs = batchMetrics.totalMs;
  }

  return { batchMetrics, done: false };
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

  const totals = createMetrics();

  try {
    console.log(
      `🚀 pendientesHoy iniciado | FETCH=${FETCH} LOOKUP_CHUNK=${LOOKUP_CHUNK} COMMIT_EVERY=${COMMIT_EVERY} LOOP_PAUSE_MS=${LOOP_PAUSE_MS}`
    );
    logMemory("START");

    while (true) {
      const { batchMetrics, done } = await procesarLote(conn, totals);

      if (done) {
        console.log("✅ pendientesHoy: no hay más registros con procesado=0");
        break;
      }

      if (totals.batchNo % LOG_EVERY_BATCH === 0) {
        logBatchSummary(batchMetrics, totals);
      }

      await sleep(LOOP_PAUSE_MS);
    }

    const totalMs = nowMs() - totals.totalStartMs;

    console.log("🏁 pendientesHoy finalizado");
    console.log(
      `📊 RESUMEN FINAL | batches=${fmtNum(totals.batchNo)} fetched=${fmtNum(totals.totalFetched)} ` +
      `processed=${fmtNum(totals.totalProcessed)} estado=${fmtNum(totals.totalEstadoRows)} ` +
      `asignaciones=${fmtNum(totals.totalAsignacionesRows)} applyOps=${fmtNum(totals.totalApplyOps)} ` +
      `flushes=${fmtNum(totals.totalFlushes)} marked=${fmtNum(totals.totalMarkedProcessed)} ` +
      `marked9=${fmtNum(totals.totalMarked9)}`
    );

    console.log(
      `⏱ TIEMPOS | total=${fmtMs(totalMs)} avgBatch=${fmtMs(totals.batchNo ? totalMs / totals.batchNo : 0)} ` +
      `minBatch=${fmtMs(totals.minBatchMs || 0)} maxBatch=${fmtMs(totals.maxBatchMs || 0)}`
    );

    console.log(
      `🚀 THROUGHPUT | fetched/s=${rate(totals.totalFetched, totalMs)} ` +
      `processed/s=${rate(totals.totalProcessed, totalMs)} ` +
      `applyOps/s=${rate(totals.totalApplyOps, totalMs)}`
    );

    console.log(
      `🔎 PRELOAD | prevEstadoPairs=${fmtNum(totals.totalPreloadEstadoPairs)} ` +
      `prevChoferPairs=${fmtNum(totals.totalPreloadChoferPairs)}`
    );

    logMemory("END");

    return {
      ok: true,
      fetched: totals.totalFetched,
      processedIds: totals.totalProcessed,
      marked9: totals.totalMarked9,
      batches: totals.batchNo,
      elapsedMs: totalMs,
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