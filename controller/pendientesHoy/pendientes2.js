const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);
const ESTADO_ANY = 999; // agregado: "existió en el día en algún estado"


const TZ = "America/Argentina/Buenos_Aires";

function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).format(ok);
}

const nEstado = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// ----------------- Cache en memoria (GLOBAL) -----------------
// key: "owner|cliente|chofer|estado|dia"
const homeAppCache = new Map();

const makeKey = (owner, cliente, chofer, estado, dia) =>
  `${owner}|${cliente}|${chofer}|${estado}|${dia}`;

function parseCSVToSet(s) {
  const set = new Set();
  if (!s || !String(s).trim()) return set;
  for (const x of String(s).split(",").map(t => t.trim()).filter(Boolean)) set.add(x);
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

function applyDeltas(entry, posArr, negArr, estado) {
  // positivos
  for (const p of posArr) {
    const k = String(p);
    entry.historial.add(k);
    entry.cierre.add(k);
  }

  // ✅ ANY nunca resta
  if (estado !== ESTADO_ANY) {
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
      didsPaquete          = VALUES(didsPaquete),
      didsPaquetes_cierre  = VALUES(didsPaquetes_cierre),
      autofecha            = NOW()
  `;
  await executeQuery(conn, upsert, [
    owner, cliente, chofer, estado,
    didsPaqueteStr,
    didsPaquetesCierreStr,
    dia
  ], true);

  entry.dirty = false;
}

// ✅ limpiar estado global por corrida
function resetState() {
  for (const k of Object.keys(Aprocesos)) delete Aprocesos[k];
  idsProcesados.length = 0;

  // ✅ cache solo para la corrida (si querés 2-3 días, esto lo cambiamos)
  homeAppCache.clear();
}

// ----------------- Helpers -----------------
function ensure(o, k) { return (o[k] ??= {}); }

// ✅ NO más didOwner=0
function pushNodoConGlobal(owner, cli, cho, est, dia, tipo, envio) {
  // owner real
  pushNodo(owner, cli, cho, est, dia, tipo, envio);

  // global por owner (empresa completa) - NO didOwner=0
  pushNodo(owner, 0, 0, est, dia, tipo, envio);
}

function pushNodo(owner, cli, cho, est, dia, tipo, envio) {
  ensure(Aprocesos, owner);
  ensure(Aprocesos[owner], cli);
  ensure(Aprocesos[owner][cli], cho);
  ensure(Aprocesos[owner][cli][cho], est);
  ensure(Aprocesos[owner][cli][cho][est], dia);
  if (!Aprocesos[owner][cli][cho][est][dia][1]) Aprocesos[owner][cli][cho][est][dia][1] = [];
  if (!Aprocesos[owner][cli][cho][est][dia][0]) Aprocesos[owner][cli][cho][est][dia][0] = [];
  Aprocesos[owner][cli][cho][est][dia][tipo].push(String(envio));
}

// ----------------- Prev desde HOME_APP (sin idx) -----------------
// Busca combinaciones donde el envío está "en cierre" para ese owner.
// OJO: depende de strings con coma, pero se usa SOLO para resolver "previo" por paquete.
async function getPrevFromHomeApp(conn, owner, envio) {
  const qPrev = `
    SELECT estado, didChofer, didCliente, dia
    FROM home_app
    WHERE didOwner = ?
      AND FIND_IN_SET(?, didsPaquetes_cierre) > 0
    ORDER BY dia DESC, autofecha DESC
    LIMIT 1
  `;
  return await executeQuery(conn, qPrev, [owner, String(envio)]);
}


// ----------------- Builder para disparador = 'estado' -----------------
async function buildAprocesosEstado(rows, connection) {
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const EST = nEstado(row.estado);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const envio = String(row.didPaquete);
    const CHO = EST === 0 ? (Number(row.quien) || 0) : (row.didChofer ?? 0);

    // ✅ prev desde home_app (sin idx)
    const prevRows = await getPrevFromHomeApp(connection, OW, envio);

    for (const prev of prevRows) {
      const PREV_EST = nEstado(prev.estado);
      const PREV_CHO = Number(prev.didChofer) || 0;
      const PREV_CLI = Number(prev.didCliente) || 0;
      const PREV_DIA = prev.dia || dia; // fallback

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

    // positivos actuales (para el dia actual del evento)
    pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
    pushNodo(OW, CLI, 0, EST, dia, 1, envio);
    // ✅ ANY: existió en el día (no depende del estado)
    pushNodo(OW, 0, 0, ESTADO_ANY, dia, 1, envio);
    pushNodo(OW, CLI, 0, ESTADO_ANY, dia, 1, envio);


    // 69
    if (ESTADOS_69.has(EST)) {
      pushNodoConGlobal(OW, 0, 0, 69, dia, 1, envio);
      pushNodo(OW, CLI, 0, 69, dia, 1, envio);
    } else {
      pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
      pushNodo(OW, CLI, 0, 69, dia, 0, envio);
    }

    // 70
    if (ESTADOS_70.has(EST)) {
      pushNodoConGlobal(OW, 0, 0, 70, dia, 1, envio);
      pushNodo(OW, CLI, 0, 70, dia, 1, envio);
    } else {
      pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
      pushNodo(OW, CLI, 0, 70, dia, 0, envio);
    }

    // combinaciones por chofer
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
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const CHO = row.didChofer ?? 0;
    const EST = nEstado(row.estado);
    const envio = String(row.didPaquete);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);

    if (CHO !== 0) {
      // positivos por chofer
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      // global por owner (sin owner=0)
      pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
      pushNodo(OW, CLI, 0, EST, dia, 1, envio);
      // ✅ ANY: existió en el día
      pushNodo(OW, 0, 0, ESTADO_ANY, dia, 1, envio);
      pushNodo(OW, CLI, 0, ESTADO_ANY, dia, 1, envio);


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

    const qChoferAnterior = `
      SELECT operador AS didChofer
      FROM asignaciones
      WHERE didEnvio = ? AND didOwner = ? AND operador IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
    `;
    const prev = await executeQuery(conn, qChoferAnterior, [envio, OW]);

    if (prev.length) {
      const choPrev = prev[0].didChofer || 0;
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
    }

    idsProcesados.push(row.id);
  }
  return Aprocesos;
}

// ----------------- Aplicar batch (cache + flush) -----------------
async function aplicarAprocesosAHommeApp(conn) {
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

              // ✅ 1) cargar combo 1 vez desde DB (lazy)
              const entry = await getComboEntry(conn, owner, cliente, chofer, estado, dia);

              // ✅ 2) aplicar deltas en memoria
              applyDeltas(entry, pos, neg, estado);



              // ✅ 3) flush inmediato (si querés diferido, lo cambiamos)
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

  // Marcar CDC como procesado
  if (idsProcesados.length > 0) {
    const CHUNK = 1000;
    for (let i = 0; i < idsProcesados.length; i += CHUNK) {
      const slice = idsProcesados.slice(i, i + CHUNK);
      const updCdc = `
        UPDATE cdc
        SET procesado=1, fProcesado=NOW()
        WHERE id IN (${slice.map(() => "?").join(",")})
      `;
      await executeQuery(conn, updCdc, slice);
    }
  }
}

// ----------------- Batch principal -----------------
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
    const FETCH = 1000;

    const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, quien, estado, disparador, ejecutar, fecha
      FROM cdc
      WHERE procesado=0
        AND ( ejecutar="estado" OR ejecutar="asignaciones" )
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;
    const rows = await executeQuery(conn, selectCDC, [FETCH]);

    const rowsEstado = rows.filter(r => r.disparador === "estado");
    const rowsAsignaciones = rows.filter(r => r.disparador === "asignaciones");

    console.log("llegamos 1");

    await buildAprocesosEstado(rowsEstado, conn);
    console.log("llegamos 2");

    await buildAprocesosAsignaciones(conn, rowsAsignaciones);
    console.log("llegamos 3");

    await aplicarAprocesosAHommeApp(conn);
    console.log("llegamos 4");

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

// ✅ NO ejecutar automáticamente al importar
pendientesHoy();

module.exports = { pendientesHoy };
