const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);
const ESTADO_ANY = 999; // agregado: "existió en el día en algún estado"
const ESTADO_ANY_EVENTO = 998 // agregado: "existió en el día del evento"

const TZ = "America/Argentina/Buenos_Aires";
const PEN_FETCH = Number(process.env.PEN_FETCH || 3000);
const PEN_PREV_CACHE_DAYS = Number(process.env.PEN_PREV_CACHE_DAYS || 2);
let resumenNoDisponibleLogueado = false;
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
// key: "owner|envio" => { estado, didChofer, didCliente, dia, cachedAt }
const prevStateRecentCache = new Map();

const makeKey = (owner, cliente, chofer, estado, dia) =>
  `${owner}|${cliente}|${chofer}|${estado}|${dia}`;
const makePrevStateKey = (owner, envio) => `${owner}|${envio}`;

function parseCSVToSet(s) {
  const set = new Set();
  if (!s || !String(s).trim()) return set;
  for (const x of String(s).split(",").map(t => t.trim()).filter(Boolean)) set.add(x);
  return set;
}

function getCutoffDia(days = PEN_PREV_CACHE_DAYS) {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - Math.max(0, days));
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit"
  }).format(d);
}

function isDiaRecentEnough(dia, cutoffDia) {
  return Boolean(dia) && String(dia) >= String(cutoffDia);
}

function setRecentPrevState(owner, envio, prev) {
  if (!owner || !envio || !prev) return;
  prevStateRecentCache.set(makePrevStateKey(owner, envio), {
    estado: prev.estado,
    didChofer: Number(prev.didChofer) || 0,
    didCliente: Number(prev.didCliente) || 0,
    dia: prev.dia || null,
    cachedAt: Date.now(),
  });
}

function getRecentPrevState(owner, envio, cutoffDia) {
  const cached = prevStateRecentCache.get(makePrevStateKey(owner, envio));
  if (!cached) return null;
  if (!isDiaRecentEnough(cached.dia, cutoffDia)) return null;
  return cached;
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

  // ✅ ANY y 998 nunca restan
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
      didsPaquete          = VALUES(didsPaquete),
      didsPaquetes_cierre  = VALUES(didsPaquetes_cierre),
      autofecha            = NOW()
  `;
  const result = await executeQuery(conn, upsert, [
    owner, cliente, chofer, estado,
    didsPaqueteStr,
    didsPaquetesCierreStr,
    dia
  ], true);
  entry.dirty = false;

  return {
    affectedRows: Number(result?.affectedRows || 0),
    changedRows: Number(result?.changedRows || 0),
    historialSize: entry.historial.size,
    cierreSize: entry.cierre.size,
  };
}

function esTablaNoExiste(error) {
  return error?.code === "ER_NO_SUCH_TABLE" || error?.errno === 1146;
}

async function upsertResumen(conn, owner, cliente, chofer, estado, dia, entry) {
  const sql = `
    INSERT INTO home_app_resumen
      (didOwner, didCliente, didChofer, estado, dia, cantidad)
    VALUES
      (?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      cantidad = VALUES(cantidad),
      autofecha = CURRENT_TIMESTAMP
  `;

  try {
    await executeQuery(conn, sql, [
      owner,
      cliente,
      chofer,
      estado,
      dia,
      entry?.historial?.size || 0,
    ]);
  } catch (error) {
    if (!esTablaNoExiste(error)) throw error;

    if (!resumenNoDisponibleLogueado) {
      resumenNoDisponibleLogueado = true;
      console.log("home_app_resumen todavia no existe; sigo solo con home_app");
    }
  }
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

async function preloadPrevFromHomeApp(conn, rows) {
  const startedAt = Date.now();
  const prevMap = new Map();
  const enviosPorOwner = new Map();
  const cutoffDia = getCutoffDia(PEN_PREV_CACHE_DAYS);

  for (const row of rows) {
    const owner = Number(row.didOwner);
    const envio = String(row.didPaquete || "");
    if (!owner || !envio) continue;

    if (!enviosPorOwner.has(owner)) enviosPorOwner.set(owner, new Set());
    enviosPorOwner.get(owner).add(envio);
  }

  for (const [owner, enviosSet] of enviosPorOwner.entries()) {
    if (!enviosSet.size) continue;
    console.log(`[PEN2] preload previos owner=${owner} envios=${enviosSet.size}`);

    const enviosPendientes = [];
    for (const envio of enviosSet) {
      const cached = getRecentPrevState(owner, envio, cutoffDia);
      if (cached) {
        prevMap.set(makePrevStateKey(owner, envio), [{
          estado: cached.estado,
          didChofer: cached.didChofer,
          didCliente: cached.didCliente,
          dia: cached.dia,
        }]);
      } else {
        enviosPendientes.push(envio);
      }
    }

    let totalRowsOwner = 0;
    if (enviosPendientes.length) {
      const ownerStartedAt = Date.now();
      const whereFindsRecent = enviosPendientes.map(() => `FIND_IN_SET(?, didsPaquetes_cierre) > 0`).join(" OR ");
      const qPrevRecent = `
        SELECT estado, didChofer, didCliente, dia, didsPaquetes_cierre
        FROM home_app
        WHERE didOwner = ?
          AND dia >= ?
          AND (${whereFindsRecent})
        ORDER BY dia DESC, autofecha DESC
      `;
      const prevRowsRecent = await executeQuery(conn, qPrevRecent, [owner, cutoffDia, ...enviosPendientes], { timeoutMs: 120000 });
      totalRowsOwner += prevRowsRecent.length;

      for (const prev of prevRowsRecent) {
        const cierreSet = parseCSVToSet(prev.didsPaquetes_cierre);

        for (const envio of cierreSet) {
          if (!enviosSet.has(envio)) continue;

          const key = makePrevStateKey(owner, envio);
          if (!prevMap.has(key)) {
            const state = {
              estado: prev.estado,
              didChofer: prev.didChofer,
              didCliente: prev.didCliente,
              dia: prev.dia
            };
            prevMap.set(key, [state]);
            if (isDiaRecentEnough(prev.dia, cutoffDia)) {
              setRecentPrevState(owner, envio, state);
            }
          }
        }
      }

      const enviosFaltantes = enviosPendientes.filter((envio) => !prevMap.has(makePrevStateKey(owner, envio)));
      if (enviosFaltantes.length) {
        const whereFindsFallback = enviosFaltantes.map(() => `FIND_IN_SET(?, didsPaquetes_cierre) > 0`).join(" OR ");
        const qPrevFallback = `
          SELECT estado, didChofer, didCliente, dia, didsPaquetes_cierre
          FROM home_app
          WHERE didOwner = ?
            AND (${whereFindsFallback})
          ORDER BY dia DESC, autofecha DESC
        `;
        const prevRowsFallback = await executeQuery(conn, qPrevFallback, [owner, ...enviosFaltantes], { timeoutMs: 120000 });
        totalRowsOwner += prevRowsFallback.length;

        for (const prev of prevRowsFallback) {
          const cierreSet = parseCSVToSet(prev.didsPaquetes_cierre);

          for (const envio of cierreSet) {
            if (!enviosSet.has(envio)) continue;

            const key = makePrevStateKey(owner, envio);
            if (!prevMap.has(key)) {
              const state = {
                estado: prev.estado,
                didChofer: prev.didChofer,
                didCliente: prev.didCliente,
                dia: prev.dia
              };
              prevMap.set(key, [state]);
              if (isDiaRecentEnough(prev.dia, cutoffDia)) {
                setRecentPrevState(owner, envio, state);
              }
            }
          }
        }

        console.log(
          `[PEN2] preload previos owner=${owner} fallbackMiss=${enviosFaltantes.length} cutoffDia=${cutoffDia}`
        );
      }

      console.log(
        `[PEN2] preload previos owner=${owner} rows=${totalRowsOwner} cacheHits=${enviosSet.size - enviosPendientes.length} elapsedMs=${Date.now() - ownerStartedAt}`
      );
    } else {
      console.log(
        `[PEN2] preload previos owner=${owner} rows=0 cacheHits=${enviosSet.size} elapsedMs=0`
      );
    }
  }

  console.log(`[PEN2] preloadPrevFromHomeApp listo owners=${enviosPorOwner.size} claves=${prevMap.size} elapsedMs=${Date.now() - startedAt}`);

  return prevMap;
}


// ----------------- Builder para disparador = 'estado' -----------------
async function buildAprocesosEstado(rows, connection) {
  const startedAt = Date.now();
  console.log(`[PEN2] buildAprocesosEstado inicio rows=${rows.length}`);
  const prevMap = await preloadPrevFromHomeApp(connection, rows);

  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const EST = nEstado(row.estado);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const diaEvento = getDiaFromTS(row.fecha);
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;

    const envio = String(row.didPaquete);
    const CHO = EST === 0 ? (Number(row.quien) || 0) : (row.didChofer ?? 0);

    // ✅ prev desde home_app precargado antes del loop
    const prevRows = prevMap.get(`${OW}|${envio}`) || [];

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
    pushNodo(OW, 0, 0, ESTADO_ANY, diaPaquete, 1, envio);
    pushNodo(OW, CLI, 0, ESTADO_ANY, diaPaquete, 1, envio);

    pushNodo(OW, 0, 0, ESTADO_ANY_EVENTO, dia, 1, envio);
    pushNodo(OW, CLI, 0, ESTADO_ANY_EVENTO, dia, 1, envio);

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
    setRecentPrevState(OW, envio, {
      estado: EST,
      didChofer: CHO,
      didCliente: CLI,
      dia,
    });
  }
  console.log(`[PEN2] buildAprocesosEstado fin rows=${rows.length} prevMap=${prevMap.size} elapsedMs=${Date.now() - startedAt}`);
  return Aprocesos;
}

// ----------------- Builder para disparador = 'asignaciones' -----------------
async function buildAprocesosAsignaciones(conn, rows) {
  const startedAt = Date.now();
  console.log(`[PEN2] buildAprocesosAsignaciones inicio rows=${rows.length}`);
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const CHO = row.didChofer ?? 0;
    const EST = nEstado(row.estado);
    const envio = String(row.didPaquete);
    if (!OW || EST === null) continue;
    const dia = getDiaFromTS(row.fecha);
    const diaEvento = getDiaFromTS(row.fecha);
    const diaPaquete = row.fecha_inicio ? getDiaFromTS(row.fecha_inicio) : diaEvento;


    if (CHO !== 0) {
      // positivos por chofer
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      // global por owner (sin owner=0)
      pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
      pushNodo(OW, CLI, 0, EST, dia, 1, envio);
      // ✅ ANY: existió en el día
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
    if (CHO !== 0) {
      setRecentPrevState(OW, envio, {
        estado: EST,
        didChofer: CHO,
        didCliente: CLI,
        dia,
      });
    }
  }
  console.log(`[PEN2] buildAprocesosAsignaciones fin rows=${rows.length} elapsedMs=${Date.now() - startedAt}`);
  return Aprocesos;
}

// ----------------- Aplicar batch (cache + flush) -----------------
async function aplicarAprocesosAHommeApp(conn) {
  const startedAt = Date.now();
  const COMMIT_EVERY = 300;
  let ops = 0;
  let owners = 0;
  let upsertsHomeApp = 0;
  let upsertsResumen = 0;
  let totalPositivos = 0;
  let totalNegativos = 0;
  let sampleLogs = 0;

  const begin = async () => executeQuery(conn, "START TRANSACTION");
  const commit = async () => executeQuery(conn, "COMMIT");
  const rollback = async () => executeQuery(conn, "ROLLBACK");

  await begin();

  try {
    for (const ownerKey in Aprocesos) {
      const owner = Number(ownerKey);
      const porCliente = Aprocesos[ownerKey];
      owners += 1;
      console.log(`[PEN2] flush owner=${owner} clientes=${Object.keys(porCliente).length}`);

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
              totalPositivos += pos.length;
              totalNegativos += neg.length;

              // ✅ 1) cargar combo 1 vez desde DB (lazy)
              const entry = await getComboEntry(conn, owner, cliente, chofer, estado, dia);

              // ✅ 2) aplicar deltas en memoria
              applyDeltas(entry, pos, neg, estado);



              // ✅ 3) flush inmediato (si querés diferido, lo cambiamos)
              const flushStats = await flushEntry(conn, owner, cliente, chofer, estado, dia, entry);
              upsertsHomeApp += 1;
              await upsertResumen(conn, owner, cliente, chofer, estado, dia, entry);
              upsertsResumen += 1;

              if (sampleLogs < 20) {
                sampleLogs += 1;
                console.log(
                  `[PEN2] home_app upsert owner=${owner} cliente=${cliente} chofer=${chofer} estado=${estado} dia=${dia} pos=${pos.length} neg=${neg.length} historial=${flushStats?.historialSize ?? "?"} cierre=${flushStats?.cierreSize ?? "?"} affected=${flushStats?.affectedRows ?? 0} changed=${flushStats?.changedRows ?? 0}`
                );
              }

              ops += 1;
              if (ops % COMMIT_EVERY === 0) {
                console.log(`[PEN2] commit parcial ops=${ops}`);
                await commit();
                await begin();
              }
            }
          }
        }
      }
    }

    await commit();
    console.log(
      `[PEN2] aplicarAprocesosAHommeApp fin owners=${owners} ops=${ops} upsertsHomeApp=${upsertsHomeApp} upsertsResumen=${upsertsResumen} pos=${totalPositivos} neg=${totalNegativos} elapsedMs=${Date.now() - startedAt}`
    );
  } catch (e) {
    try { await rollback(); } catch (_) { }
    throw e;
  }

  // Marcar CDC como procesado
  if (idsProcesados.length > 0) {
    const CHUNK = 1000;
    console.log(`[PEN2] marcando cdc procesado ids=${idsProcesados.length} chunks=${Math.ceil(idsProcesados.length / CHUNK)}`);
    for (let i = 0; i < idsProcesados.length; i += CHUNK) {
      const slice = idsProcesados.slice(i, i + CHUNK);
      const updCdc = `
        UPDATE cdc
        SET procesado=1, fProcesado=NOW()
        WHERE id IN (${slice.map(() => "?").join(",")})
      `;
      const result = await executeQuery(conn, updCdc, slice);
      console.log(
        `[PEN2] cdc chunk procesado fromId=${slice[0]} toId=${slice[slice.length - 1]} size=${slice.length} affected=${Number(result?.affectedRows || 0)}`
      );
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
    console.log("[PEN2] pendientesHoy inicio");
    const FETCH = PEN_FETCH;

    const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, quien, estado, disparador, ejecutar, fecha,fecha_inicio
      FROM cdc
      WHERE procesado=0
      ORDER BY id ASC
      LIMIT ?
    `;
    const rows = await executeQuery(conn, selectCDC, [FETCH]);
    console.log(`[PEN2] cdc rows=${rows.length}`);

    if (!rows.length) {
      console.log("[PEN2] sin filas pendientes para procesar");
      return { ok: true, fetched: 0, processedIds: 0, descartados: 0 };
    }

    const firstCdcId = Number(rows[0]?.id || 0);
    const lastCdcId = Number(rows[rows.length - 1]?.id || 0);
    console.log(`[PEN2] lote ids ${firstCdcId}..${lastCdcId}`);

    const rowsClienteNull = rows.filter(r => r.didCliente == null);
    const rowsConCliente = rows.filter(r => r.didCliente != null);
    const rowsDisparadorInvalido = rowsConCliente.filter(
      r => r.disparador !== "estado" && r.disparador !== "asignaciones"
    );
    const rowsValidas = rowsConCliente.filter(
      r => r.disparador === "estado" || r.disparador === "asignaciones"
    );
    console.log(
      `[PEN2] cdc validas=${rowsValidas.length} nullDidCliente=${rowsClienteNull.length} disparadorInvalido=${rowsDisparadorInvalido.length}`
    );

    if (rowsClienteNull.length) {
      const idsNull = rowsClienteNull.map(r => r.id);
      const updNull = `
        UPDATE cdc
        SET procesado=2, fProcesado=NOW()
        WHERE id IN (${idsNull.map(() => "?").join(",")})
      `;
      await executeQuery(conn, updNull, idsNull);
    }

    if (rowsDisparadorInvalido.length) {
      const idsInvalidos = rowsDisparadorInvalido.map(r => r.id);
      const updInvalidos = `
        UPDATE cdc
        SET procesado=3, fProcesado=NOW()
        WHERE id IN (${idsInvalidos.map(() => "?").join(",")})
      `;
      await executeQuery(conn, updInvalidos, idsInvalidos);
      console.log(`[PEN2] cdc descartados por disparador invalido ids=${idsInvalidos.join(",")}`);
    }

    const rowsEstado = rowsValidas.filter(r => r.disparador === "estado");
    const rowsAsignaciones = rowsValidas.filter(r => r.disparador === "asignaciones");

    console.log(`[PEN2] rowsEstado=${rowsEstado.length} rowsAsignaciones=${rowsAsignaciones.length}`);

    await buildAprocesosEstado(rowsEstado, conn);
    console.log("[PEN2] buildAprocesosEstado ok");

    await buildAprocesosAsignaciones(conn, rowsAsignaciones);
    console.log("[PEN2] buildAprocesosAsignaciones ok");

    await aplicarAprocesosAHommeApp(conn);
    console.log("[PEN2] aplicarAprocesosAHommeApp ok");

    return {
      ok: true,
      fetched: rows.length,
      processedIds: idsProcesados.length,
      descartados: rowsClienteNull.length + rowsDisparadorInvalido.length
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

// ✅ NO ejecutar automáticamente al importar
//pendientesHoy();

module.exports = { pendientesHoy };
