const { executeQuery, getConnectionLocalPendientes } = require("../../db");

// ----------------- Config -----------------
const Aprocesos = {};
const idsProcesados = [];

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);

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

// ✅ limpiar estado global por corrida
function resetState() {
  // vaciar Aprocesos sin perder referencia
  for (const k of Object.keys(Aprocesos)) delete Aprocesos[k];
  idsProcesados.length = 0;
}

// ----------------- Helpers -----------------
function ensure(o, k) { return (o[k] ??= {}); }
function pushNodoConGlobal(owner, cli, cho, est, dia, tipo, envio) {
  // owner real
  pushNodo(owner, cli, cho, est, dia, tipo, envio);

  // global absoluto: todas las empresas
  pushNodo(0, 0, 0, est, dia, tipo, envio);
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
async function upsertIndexPositivos(conn, owner, cliente, chofer, estado, dia, paquetes) {
  const uniq = [...new Set((paquetes || []).map(String))];
  if (!uniq.length) return;

  const CHUNK = 500;
  for (let i = 0; i < uniq.length; i += CHUNK) {
    const slice = uniq.slice(i, i + CHUNK);

    const values = slice.map(() => "(?,?,?,?,?,?,1,1,NOW(),NOW())").join(",");
    const sql = `
      INSERT INTO home_app_idx
        (didOwner, didCliente, didChofer, estado, dia, didPaquete, en_historial, en_cierre, updatedAt, createdAt)
      VALUES ${values}
      ON DUPLICATE KEY UPDATE
        en_historial = 1,
        en_cierre    = 1,
        updatedAt    = NOW()
    `;

    const params = [];
    for (const p of slice) {
      params.push(owner, cliente, chofer, estado, dia, p);
    }

    await executeQuery(conn, sql, params, true);
  }
}

async function updateIndexNegativos(conn, owner, cliente, chofer, estado, dia, paquetes) {
  const uniq = [...new Set((paquetes || []).map(String))];
  if (!uniq.length) return;

  const CHUNK = 500;
  for (let i = 0; i < uniq.length; i += CHUNK) {
    const slice = uniq.slice(i, i + CHUNK);

    const sql = `
      UPDATE home_app_idx
      SET en_cierre = 0, updatedAt = NOW()
      WHERE didOwner   = ?
        AND didCliente = ?
        AND didChofer  = ?
        AND estado     = ?
        AND dia        = ?
        AND didPaquete IN (${slice.map(() => "?").join(",")})
    `;

    await executeQuery(conn, sql, [owner, cliente, chofer, estado, dia, ...slice], true);
  }
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

    const qPrev = `
  SELECT estado, didChofer
  FROM home_app_idx
  WHERE didOwner = ?
    AND didPaquete = ?
    AND en_cierre = 1
`;
    const prevRows = await executeQuery(connection, qPrev, [OW, envio]);


    for (const prev of prevRows) {
      const PREV_EST = nEstado(prev.estado);
      const PREV_CHO = Number(prev.didChofer) || 0;

      // negativos previos
      pushNodoConGlobal(OW, 0, 0, PREV_EST, dia, 0, envio);
      pushNodo(OW, CLI, 0, PREV_EST, dia, 0, envio);

      if (PREV_CHO !== 0) {
        pushNodo(OW, 0, PREV_CHO, PREV_EST, dia, 0, envio);
        pushNodo(OW, CLI, PREV_CHO, PREV_EST, dia, 0, envio);
      }

      if (ESTADOS_69.has(PREV_EST)) {
        pushNodoConGlobal(OW, 0, 0, 69, dia, 0, envio);
        pushNodo(OW, CLI, 0, 69, dia, 0, envio);

        if (PREV_CHO !== 0) {
          pushNodo(OW, 0, PREV_CHO, 69, dia, 0, envio);
          pushNodo(OW, CLI, PREV_CHO, 69, dia, 0, envio);
        }
      }

      if (ESTADOS_70.has(PREV_EST)) {
        pushNodoConGlobal(OW, 0, 0, 70, dia, 0, envio);
        pushNodo(OW, CLI, 0, 70, dia, 0, envio);

        if (PREV_CHO !== 0) {
          pushNodo(OW, 0, PREV_CHO, 70, dia, 0, envio);
          pushNodo(OW, CLI, PREV_CHO, 70, dia, 0, envio);
        }
      }
    }

    // positivos actuales
    pushNodoConGlobal(OW, 0, 0, EST, dia, 1, envio);
    pushNodo(OW, CLI, 0, EST, dia, 1, envio);

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

    // combinaciones por chofer (las dejamos igual, NO las agregamos al global,
    // porque vos pediste global solo en owner=0/cliente=0/chofer=0)
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
      // positivos
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      // ✅ global absoluto
      pushNodo(0, 0, 0, EST, dia, 1, envio);

      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, CHO, 69, dia, 1, envio);
        pushNodo(OW, 0, CHO, 69, dia, 1, envio);

        // ✅ global absoluto
        pushNodo(0, 0, 0, 69, dia, 1, envio);
      }

      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, CHO, 70, dia, 1, envio);
        pushNodo(OW, 0, CHO, 70, dia, 1, envio);

        // ✅ global absoluto
        pushNodo(0, 0, 0, 70, dia, 1, envio);
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
        // negativos
        pushNodo(OW, CLI, choPrev, EST, dia, 0, envio);
        pushNodo(OW, 0, choPrev, EST, dia, 0, envio);

        // ✅ global absoluto
        pushNodo(0, 0, 0, EST, dia, 0, envio);

        if (ESTADOS_69.has(EST)) {
          pushNodo(OW, CLI, choPrev, 69, dia, 0, envio);
          pushNodo(OW, 0, choPrev, 69, dia, 0, envio);

          // ✅ global absoluto
          pushNodo(0, 0, 0, 69, dia, 0, envio);
        }

        if (ESTADOS_70.has(EST)) {
          pushNodo(OW, CLI, choPrev, 70, dia, 0, envio);
          pushNodo(OW, 0, choPrev, 70, dia, 0, envio);

          // ✅ global absoluto
          pushNodo(0, 0, 0, 70, dia, 0, envio);
        }
      }
    }

    idsProcesados.push(row.id);
  }
  return Aprocesos;
}

// ----------------- Aplicar batch (chunked commits) -----------------
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

              // ---- 1) Leer actual (sin FOR UPDATE para bajar locks) ----
              const sel = `
                SELECT didsPaquete, didsPaquetes_cierre
                FROM home_app
                WHERE didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?
                LIMIT 1
              `;
              const actual = await executeQuery(conn, sel, [owner, cliente, chofer, estado, dia]);

              let historialSet = new Set();
              let cierreSet = new Set();

              if (actual.length > 0) {
                const sHist = actual[0].didsPaquete || "";
                if (sHist.trim()) {
                  for (const x of sHist.split(",").map(t => t.trim()).filter(Boolean)) historialSet.add(x);
                }
                const sCierre = actual[0].didsPaquetes_cierre || "";
                if (sCierre.trim()) {
                  for (const x of sCierre.split(",").map(t => t.trim()).filter(Boolean)) cierreSet.add(x);
                }
              }

              // ---- 2) Aplicar deltas ----
              for (const p of pos) {
                const k = String(p);
                historialSet.add(k);
                cierreSet.add(k);
              }
              for (const p of neg) cierreSet.delete(String(p));

              const didsPaqueteStr = Array.from(historialSet).join(",");
              const didsPaquetesCierreStr = Array.from(cierreSet).join(",");

              // ---- 3) Upsert legacy ----
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

              // ---- 4) ✅ Mantener índice ----
              // Positivos: historial=1, cierre=1
              await upsertIndexPositivos(conn, owner, cliente, chofer, estado, dia, pos);
              // Negativos: cierre=0 (historial queda)
              await updateIndexNegativos(conn, owner, cliente, chofer, estado, dia, neg);

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
  // ✅ Opción A: no permitir solape
  if (PENDIENTES_HOY_RUNNING) {
    console.log("⏭️ pendientesHoy ya está corriendo, salteo esta ejecución");
    return { ok: true, skipped: true };
  }

  PENDIENTES_HOY_RUNNING = true;

  resetState(); // ✅ importantísimo

  const conn = await getConnectionLocalPendientes();
  let fatalErr = null;

  try {
    const FETCH = 1000; // ✅ bajar de 5000 para no explotar transacción/locks

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
    console.log("llegamos 1 ");

    await buildAprocesosEstado(rowsEstado, conn);
    console.log("llegamos 2 ");

    await buildAprocesosAsignaciones(conn, rowsAsignaciones);
    console.log("llegamos 3 ");

    await aplicarAprocesosAHommeApp(conn);
    console.log("llegamos 4 ");



    return { ok: true, fetched: rows.length, processedIds: idsProcesados.length };
  } catch (err) {
    fatalErr = err;
    console.error("❌ Error batch:", err);
    throw err; // ✅ para que el scheduler no “mienta”
  } finally {
    // ✅ SIEMPRE liberar el candado aunque explote
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
    } catch (_) { /* ignore */ }
  }
}


// ❌ NO ejecutar automáticamente al importar
pendientesHoy();

module.exports = { pendientesHoy };
