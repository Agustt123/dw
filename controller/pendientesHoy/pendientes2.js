const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);
const ESTADO_ANY = 999; // "existió en el día en algún estado"
const ESTADO_ANY_EVENTO = 998;

const TZ = "America/Argentina/Buenos_Aires";

// Ajustá esto según presión de BD / RAM
const FETCH = Number(process.env.PENDIENTES_FETCH || 1000);
const MAX_BATCHES_PER_RUN = Number(process.env.PENDIENTES_MAX_BATCHES || 200);
const COMBO_PRELOAD_CHUNK = Number(process.env.PENDIENTES_COMBO_PRELOAD_CHUNK || 250);
const IDS_UPDATE_CHUNK = Number(process.env.PENDIENTES_IDS_UPDATE_CHUNK || 5000);
const COMMIT_EVERY = Number(process.env.PENDIENTES_COMMIT_EVERY || 200);

const dayFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: TZ,
  year: "numeric",
  month: "2-digit",
  day: "2-digit"
});

function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = isNaN(d.getTime()) ? new Date() : d;
  return dayFormatter.format(ok);
}

const nEstado = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// ----------------- Cache en memoria (GLOBAL POR CORRIDA / BATCH) -----------------
// key: "owner|cliente|chofer|estado|dia"
const homeAppCache = new Map();
const dirtyHomeAppKeys = new Set();

// cache para previos de home_app por owner+envio
const prevHomeAppMemo = new Map();

// cache para último chofer previo por owner+envio
const prevChoferMemo = new Map();

const makeKey = (owner, cliente, chofer, estado, dia) =>
  `${owner}|${cliente}|${chofer}|${estado}|${dia}`;

const makePrevKey = (owner, envio) => `${owner}|${envio}`;

function parseCSVToSet(s) {
  const set = new Set();
  if (!s || !String(s).trim()) return set;

  for (const x of String(s).split(",").map(t => t.trim()).filter(Boolean)) {
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
      dirty: false
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

function markDirty(owner, cliente, chofer, estado, dia) {
  dirtyHomeAppKeys.add(makeKey(owner, cliente, chofer, estado, dia));
}

function applyDeltas(entry, posArr, negArr, estado) {
  for (const p of posArr) {
    const k = String(p);
    entry.historial.add(k);
    entry.cierre.add(k);
  }

  // ANY y ANY_EVENTO nunca restan
  if (estado !== ESTADO_ANY && estado !== ESTADO_ANY_EVENTO) {
    for (const p of negArr) {
      entry.cierre.delete(String(p));
    }
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

  await executeQuery(conn, upsert, [
    owner,
    cliente,
    chofer,
    estado,
    didsPaqueteStr,
    didsPaquetesCierreStr,
    dia
  ], true);

  entry.dirty = false;
}

// ----------------- Reset por corrida/batch -----------------
function resetState() {
  for (const k of Object.keys(Aprocesos)) delete Aprocesos[k];
  idsProcesados.length = 0;

  homeAppCache.clear();
  dirtyHomeAppKeys.clear();
  prevHomeAppMemo.clear();
  prevChoferMemo.clear();
}

// ----------------- Helpers -----------------
function ensure(o, k, factory = () => ({})) {
  return (o[k] ??= factory());
}

// NO más didOwner=0
function pushNodoConGlobal(owner, cli, cho, est, dia, tipo, envio) {
  pushNodo(owner, cli, cho, est, dia, tipo, envio);
  pushNodo(owner, 0, 0, est, dia, tipo, envio);
}

function pushNodo(owner, cli, cho, est, dia, tipo, envio) {
  ensure(Aprocesos, owner);
  ensure(Aprocesos[owner], cli);
  ensure(Aprocesos[owner][cli], cho);
  ensure(Aprocesos[owner][cli][cho], est);
  ensure(Aprocesos[owner][cli][cho][est], dia, () => ({
    0: new Set(),
    1: new Set()
  }));

  Aprocesos[owner][cli][cho][est][dia][tipo].add(String(envio));
}

// ----------------- Prev desde HOME_APP (con memo) -----------------
async function getPrevFromHomeApp(conn, owner, envio) {
  const memoKey = makePrevKey(owner, envio);
  if (prevHomeAppMemo.has(memoKey)) {
    return prevHomeAppMemo.get(memoKey);
  }

  const qPrev = `
    SELECT estado, didChofer, didCliente, dia
    FROM home_app
    WHERE didOwner = ?
      AND FIND_IN_SET(?, didsPaquetes_cierre) > 0
    ORDER BY dia DESC, autofecha DESC
    LIMIT 1
  `;

  const rows = await executeQuery(conn, qPrev, [owner, String(envio)]);
  prevHomeAppMemo.set(memoKey, rows);
  return rows;
}

// ----------------- Precarga batch de chofer anterior -----------------
async function preloadPrevChoferes(conn, rows) {
  const paresUnicos = new Map();

  for (const row of rows) {
    const owner = Number(row.didOwner);
    const envio = String(row.didPaquete);
    if (!owner || !envio) continue;
    paresUnicos.set(makePrevKey(owner, envio), { owner, envio });
  }

  const items = Array.from(paresUnicos.values());
  if (!items.length) return;

  const CHUNK = 500;

  for (let i = 0; i < items.length; i += CHUNK) {
    const chunk = items.slice(i, i + CHUNK);

    const conditions = [];
    const params = [];

    for (const it of chunk) {
      conditions.push("(didOwner = ? AND didEnvio = ?)");
      params.push(it.owner, it.envio);
    }

    const q = `
      SELECT a.didOwner, a.didEnvio, a.operador AS didChofer
      FROM asignaciones a
      INNER JOIN (
        SELECT didOwner, didEnvio, MAX(id) AS maxId
        FROM asignaciones
        WHERE operador IS NOT NULL
          AND (${conditions.join(" OR ")})
        GROUP BY didOwner, didEnvio
      ) x
        ON x.didOwner = a.didOwner
       AND x.didEnvio = a.didEnvio
       AND x.maxId = a.id
    `;

    const rowsPrev = await executeQuery(conn, q, params);

    for (const r of rowsPrev) {
      prevChoferMemo.set(
        makePrevKey(Number(r.didOwner), String(r.didEnvio)),
        Number(r.didChofer) || 0
      );
    }

    // completar faltantes con 0 para evitar misses repetidos
    for (const it of chunk) {
      const k = makePrevKey(it.owner, it.envio);
      if (!prevChoferMemo.has(k)) {
        prevChoferMemo.set(k, 0);
      }
    }
  }
}

// ----------------- Builder para disparador = 'estado' -----------------
async function buildAprocesosEstado(rows, connection) {
  for (const row of rows) {
    const OW = Number(row.didOwner);
    const CLI = Number(row.didCliente ?? 0);
    const EST = nEstado(row.estado);

    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const diaEvento = dia;
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;

    const envio = String(row.didPaquete);
    const CHO = EST === 0 ? (Number(row.quien) || 0) : (Number(row.didChofer) || 0);

    // prev desde home_app (con memo)
    const prevRows = await getPrevFromHomeApp(connection, OW, envio);

    for (const prev of prevRows) {
      const PREV_EST = nEstado(prev.estado);
      const PREV_CHO = Number(prev.didChofer) || 0;
      const PREV_CLI = Number(prev.didCliente) || 0;
      const PREV_DIA = prev.dia || dia;

      if (PREV_EST === null) continue;

      // negativos previos
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

    idsProcesados.push(row.id);
  }

  return Aprocesos;
}

// ----------------- Builder para disparador = 'asignaciones' -----------------
async function buildAprocesosAsignaciones(conn, rows) {
  // precarga batch de chofer anterior
  await preloadPrevChoferes(conn, rows);

  for (const row of rows) {
    const OW = Number(row.didOwner);
    const CLI = Number(row.didCliente ?? 0);
    const CHO = Number(row.didChofer ?? 0);
    const EST = nEstado(row.estado);
    const envio = String(row.didPaquete);

    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const diaEvento = dia;
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;

    if (CHO !== 0) {
      // positivos por chofer
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      // global por owner
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

    const choPrev = prevChoferMemo.get(makePrevKey(OW, envio)) || 0;

    if (choPrev !== 0) {
      // negativos por chofer anterior
      pushNodo(OW, CLI, choPrev, EST, dia, 0, envio);
      pushNodo(OW, 0, choPrev, EST, dia, 0, envio);

      pushNodoConGlobal(OW, 0, 0, EST, dia, 0, envio);
      pushNodo(OW, CLI, 0, EST, dia, 0, envio);

      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, choPrev, 69, dia, 0, envio);
        pushNodo(OW, 0, choPrev, 69, dia, 0, envio);

        pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
        pushNodo(OW, CLI, 0, 69, dia, 0, envio);
      }

      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, choPrev, 70, dia, 0, envio);
        pushNodo(OW, 0, choPrev, 70, dia, 0, envio);

        pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
        pushNodo(OW, CLI, 0, 70, dia, 0, envio);
      }
    }

    idsProcesados.push(row.id);
  }

  return Aprocesos;
}

// ----------------- Precarga batch de combos de home_app -----------------
function collectComboKeysFromAprocesos() {
  const keys = [];

  for (const ownerKey in Aprocesos) {
    const porCliente = Aprocesos[ownerKey];

    for (const clienteKey in porCliente) {
      const porChofer = porCliente[clienteKey];

      for (const choferKey in porChofer) {
        const porEstado = porChofer[choferKey];

        for (const estadoKey in porEstado) {
          const porDia = porEstado[estadoKey];

          for (const dia in porDia) {
            const nodo = porDia[dia];
            const pos = nodo?.[1];
            const neg = nodo?.[0];

            if ((!pos || pos.size === 0) && (!neg || neg.size === 0)) continue;

            keys.push({
              owner: Number(ownerKey),
              cliente: Number(clienteKey),
              chofer: Number(choferKey),
              estado: Number(estadoKey),
              dia
            });
          }
        }
      }
    }
  }

  return keys;
}

async function preloadHomeAppCombos(conn) {
  const keys = collectComboKeysFromAprocesos();
  if (!keys.length) return;

  for (let i = 0; i < keys.length; i += COMBO_PRELOAD_CHUNK) {
    const chunk = keys.slice(i, i + COMBO_PRELOAD_CHUNK);

    const conditions = [];
    const params = [];

    for (const k of chunk) {
      const cacheKey = makeKey(k.owner, k.cliente, k.chofer, k.estado, k.dia);
      if (homeAppCache.has(cacheKey)) continue;

      conditions.push("(didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?)");
      params.push(k.owner, k.cliente, k.chofer, k.estado, k.dia);
    }

    if (!conditions.length) continue;

    const q = `
      SELECT didOwner, didCliente, didChofer, estado, dia, didsPaquete, didsPaquetes_cierre
      FROM home_app
      WHERE ${conditions.join(" OR ")}
    `;

    const rows = await executeQuery(conn, q, params);

    for (const row of rows) {
      const key = makeKey(
        Number(row.didOwner),
        Number(row.didCliente),
        Number(row.didChofer),
        Number(row.estado),
        row.dia
      );

      homeAppCache.set(key, {
        historial: parseCSVToSet(row.didsPaquete),
        cierre: parseCSVToSet(row.didsPaquetes_cierre),
        dirty: false
      });
    }

    // completar faltantes para evitar hits posteriores
    for (const k of chunk) {
      const cacheKey = makeKey(k.owner, k.cliente, k.chofer, k.estado, k.dia);
      if (!homeAppCache.has(cacheKey)) {
        homeAppCache.set(cacheKey, {
          historial: new Set(),
          cierre: new Set(),
          dirty: false
        });
      }
    }
  }
}

// ----------------- Flush diferido de home_app -----------------
async function flushDirtyHomeApp(conn) {
  let ops = 0;

  const begin = async () => executeQuery(conn, "START TRANSACTION");
  const commit = async () => executeQuery(conn, "COMMIT");
  const rollback = async () => executeQuery(conn, "ROLLBACK");

  await begin();

  try {
    for (const key of dirtyHomeAppKeys) {
      const [owner, cliente, chofer, estado, dia] = key.split("|");
      const entry = homeAppCache.get(key);
      if (!entry?.dirty) continue;

      await flushEntry(
        conn,
        Number(owner),
        Number(cliente),
        Number(chofer),
        Number(estado),
        dia,
        entry
      );

      ops += 1;

      if (ops % COMMIT_EVERY === 0) {
        await commit();
        await begin();
      }
    }

    await commit();
  } catch (e) {
    try {
      await rollback();
    } catch (_) { }
    throw e;
  }
}

// ----------------- Aplicar batch (cache + flush diferido) -----------------
async function aplicarAprocesosAHommeApp(conn) {
  // Precarga de combos a tocar para bajar queries 1x1
  await preloadHomeAppCombos(conn);

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
            const pos = [...(nodo?.[1] || new Set())];
            const neg = [...(nodo?.[0] || new Set())];

            if (!pos.length && !neg.length) continue;

            const entry = await getComboEntry(conn, owner, cliente, chofer, estado, dia);
            applyDeltas(entry, pos, neg, estado);
            markDirty(owner, cliente, chofer, estado, dia);
          }
        }
      }
    }
  }

  await flushDirtyHomeApp(conn);

  // Marcar CDC como procesado
  if (idsProcesados.length > 0) {
    for (let i = 0; i < idsProcesados.length; i += IDS_UPDATE_CHUNK) {
      const slice = idsProcesados.slice(i, i + IDS_UPDATE_CHUNK);
      const updCdc = `
        UPDATE cdc
        SET procesado=1, fProcesado=NOW()
        WHERE id IN (${slice.map(() => "?").join(",")})
      `;
      await executeQuery(conn, updCdc, slice);
    }
  }
}

// ----------------- Un batch -----------------
let PENDIENTES_HOY_RUNNING = false;

async function pendientesHoy() {
  if (PENDIENTES_HOY_RUNNING) {
    console.log("⏭️ pendientesHoy ya está corriendo, salteo esta ejecución");
    return { ok: true, skipped: true };
  }

  PENDIENTES_HOY_RUNNING = true;
  resetState();

  const conn = await getConnectionLocalPendientes();
  let fatalErr = null;

  try {
    const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, quien, estado, disparador, ejecutar, fecha, fecha_inicio
      FROM cdc
      WHERE procesado=0
        AND (ejecutar="estado" OR ejecutar="asignaciones")
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;

    console.time("selectCDC");
    const rows = await executeQuery(conn, selectCDC, [FETCH]);
    console.timeEnd("selectCDC");

    if (!rows.length) {
      return { ok: true, fetched: 0, processedIds: 0, empty: true };
    }

    const rowsEstado = rows.filter(r => r.disparador === "estado");
    const rowsAsignaciones = rows.filter(r => r.disparador === "asignaciones");

    console.time("buildAprocesosEstado");
    await buildAprocesosEstado(rowsEstado, conn);
    console.timeEnd("buildAprocesosEstado");

    console.time("buildAprocesosAsignaciones");
    await buildAprocesosAsignaciones(conn, rowsAsignaciones);
    console.timeEnd("buildAprocesosAsignaciones");

    console.time("aplicarAprocesosAHommeApp");
    await aplicarAprocesosAHommeApp(conn);
    console.timeEnd("aplicarAprocesosAHommeApp");

    return { ok: true, fetched: rows.length, processedIds: idsProcesados.length };
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

// ----------------- Runner histórico por volumen (sin cortar por fecha) -----------------
async function procesarHistoricoDesdeEnero() {
  if (PENDIENTES_HOY_RUNNING) {
    console.log("⏭️ Ya hay una corrida en curso, salteo");
    return { ok: true, skipped: true };
  }

  const resumen = {
    ok: true,
    batches: 0,
    fetched: 0,
    processedIds: 0
  };

  for (let i = 0; i < MAX_BATCHES_PER_RUN; i++) {
    console.log(`\n📦 Batch ${i + 1}/${MAX_BATCHES_PER_RUN} | FETCH=${FETCH}`);

    const r = await pendientesHoy();

    if (!r || r.skipped) {
      resumen.skipped = true;
      break;
    }

    if (r.empty || !r.fetched) {
      console.log("✅ No quedan registros pendientes para procesar");
      break;
    }

    resumen.batches += 1;
    resumen.fetched += Number(r.fetched || 0);
    resumen.processedIds += Number(r.processedIds || 0);

    if (r.fetched < FETCH) {
      console.log("✅ Último batch parcial, no quedan más pendientes inmediatos");
      break;
    }
  }

  return resumen;
}

// NO ejecutar automáticamente al importar
// pendientesHoy();
// procesarHistoricoDesdeEnero();

module.exports = {
  pendientesHoy,
  procesarHistoricoDesdeEnero
};