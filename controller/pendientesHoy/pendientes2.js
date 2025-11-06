const { executeQuery, getConnectionLocal } = require("../../db");

const Aprocesos = {};
const idsProcesados = [];
const LIMIT = 50;

const ESTADOS_69 = new Set([0, 1, 2, 3, 6, 7, 10, 11, 12]);
const ESTADOS_70 = new Set([5, 9, 17]);

const TZ = 'America/Argentina/Buenos_Aires';
function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(ok); // 'YYYY-MM-DD'
}

const nEstado = v => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// ---------- helpers ----------
function ensure(o, k) { return (o[k] ??= {}); }
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

// ---------- Builder para disparador = 'estado' ----------
async function buildAprocesosEstado(rows, connection) {
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const EST = nEstado(row.estado);
    if (!OW || EST === null) continue;

    // Evitar primer evento duplicado: si existe uno previo en 'estado', saltamos
    const queryEstado = `
      SELECT id FROM estado
      WHERE didEnvio = ? AND didOwner = ? AND id < ?
      ORDER BY id DESC LIMIT 1
    `;
    const estadoPrev = await executeQuery(connection, queryEstado, [row.didPaquete, OW, row.id]);
    if (estadoPrev.length > 0) continue;

    const dia = getDiaFromTS(row.fecha);
    const envio = String(row.didPaquete);

    // CHO: si estado==0 tomamos 'quien'; si no, usamos didChofer
    const CHO = EST === 0
      ? (Number(row.quien) || 0)
      : (row.didChofer ?? 0);

    // 1) Siempre cargar estado REAL en chofer=0
    pushNodo(OW, 0, 0, EST, dia, 1, envio);
    pushNodo(OW, CLI, 0, EST, dia, 1, envio);

    // 2) Si pertenece al conjunto, cargar TAMBIÉN como estado 69 en chofer=0
    if (ESTADOS_69.has(EST)) {
      pushNodo(OW, 0, 0, 69, dia, 1, envio);
      pushNodo(OW, CLI, 0, 69, dia, 1, envio);
    }

    // 2b) Si pertenece al conjunto, cargar TAMBIÉN como estado 70 en chofer=0
    if (ESTADOS_70.has(EST)) {
      pushNodo(OW, 0, 0, 70, dia, 1, envio);
      pushNodo(OW, CLI, 0, 70, dia, 1, envio);
    }

    // 3) Si es estado 0 y viene chofer (desde 'quien'): cargar por chofer en 0 y en 69
    if (EST === 0 && CHO !== 0) {
      // estado 0 (por chofer)
      pushNodo(OW, CLI, CHO, 0, dia, 1, envio);
      pushNodo(OW, 0, CHO, 0, dia, 1, envio);
      // estado 69 (por chofer combinado)
      pushNodo(OW, CLI, CHO, 69, dia, 1, envio);
      pushNodo(OW, 0, CHO, 69, dia, 1, envio);
    }

    idsProcesados.push(row.id);
  }
  return Aprocesos;
}

// ---------- Builder para disparador = 'asignaciones' ----------
async function buildAprocesosAsignaciones(conn, rows) {
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const CHO = row.didChofer ?? 0;  // CHO != 0 => asignación; CHO == 0 => desasignación
    const EST = nEstado(row.estado); // viene desde CDC (asegurado en SELECT)
    const envio = String(row.didPaquete);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);

    // + al chofer actual en el estado indicado
    if (CHO !== 0) {
      pushNodo(OW, CLI, CHO, EST, dia, 1, envio);
      pushNodo(OW, 0, CHO, EST, dia, 1, envio);

      // si el estado pertenece al conjunto, también sumar en 69 por chofer
      if (ESTADOS_69.has(EST)) {
        pushNodo(OW, CLI, CHO, 69, dia, 1, envio);
        pushNodo(OW, 0, CHO, 69, dia, 1, envio);
      }
      // si el estado pertenece al conjunto, también sumar en 70 por chofer
      if (ESTADOS_70.has(EST)) {
        pushNodo(OW, CLI, CHO, 70, dia, 1, envio);
        pushNodo(OW, 0, CHO, 70, dia, 1, envio);
      }
    }

    // DESASIGNACIÓN → SOLO - en chofer anterior (misma combinación de estado)
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
        pushNodo(OW, CLI, choPrev, EST, dia, 0, envio);
        pushNodo(OW, 0, choPrev, EST, dia, 0, envio);

        if (ESTADOS_69.has(EST)) {
          pushNodo(OW, CLI, choPrev, 69, dia, 0, envio);
          pushNodo(OW, 0, choPrev, 69, dia, 0, envio);
        }
        if (ESTADOS_70.has(EST)) {
          pushNodo(OW, CLI, choPrev, 70, dia, 0, envio);
          pushNodo(OW, 0, choPrev, 70, dia, 0, envio);
        }
      }
    }

    idsProcesados.push(row.id);
  }
  return Aprocesos;
}

async function aplicarAprocesosAHommeApp(conn) {
  // Iniciar transacción para evitar carreras entre SELECT/INSERT/UPDATE
  await executeQuery(conn, "START TRANSACTION");
  try {
    // owner -> cliente -> chofer -> estado -> dia -> {1:[],0:[]}
    for (const owner in Aprocesos) {
      const porCliente = Aprocesos[owner];

      for (const cliente in porCliente) {
        const porChofer = porCliente[cliente];

        for (const chofer in porChofer) {
          const porEstado = porChofer[chofer];

          for (const estado in porEstado) {
            const porDia = porEstado[estado];

            for (const dia in porDia) {
              const nodo = porDia[dia];
              const pos = [...new Set(nodo[1] || [])];
              const neg = [...new Set(nodo[0] || [])];
              if (pos.length === 0 && neg.length === 0) continue;

              // 1) Leer con lock por (owner, cliente, chofer, estado, dia)
              const sel = `
                SELECT didsPaquete, pendientes
                FROM home_app
                WHERE didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?
                FOR UPDATE
              `;
              const actual = await executeQuery(conn, sel, [owner, cliente, chofer, estado, dia]);

              // 2) parsear actuales a Set y calcular pendientes partiendo del valor actual
              let paquetes = new Set();
              let pendientes = 0;
              if (actual.length > 0) {
                const s = actual[0].didsPaquete || "";
                if (s) {
                  for (const p of s.split(",").map(x => x.trim()).filter(Boolean)) paquetes.add(p);
                }
                pendientes = actual[0].pendientes || 0;
              }

              // aplicar positivos
              for (const p of pos) {
                const k = String(p);
                if (!paquetes.has(k)) {
                  paquetes.add(k);
                  pendientes += 1;
                }
              }

              // aplicar negativos
              for (const p of neg) {
                const k = String(p);
                if (paquetes.has(k)) {
                  paquetes.delete(k);
                  pendientes = Math.max(0, pendientes - 1);
                }
              }

              const didsPaqueteStr = Array.from(paquetes).join(",");

              // 3) UPSERT atómico (evita ER_DUP_ENTRY)
              const upsert = `
                INSERT INTO home_app
                  (didOwner, didCliente, didChofer, estado, didsPaquete, fecha, dia, pendientes)
                VALUES
                  (?, ?, ?, ?, ?, NOW(), ?, ?)
                ON DUPLICATE KEY UPDATE
                  didsPaquete = VALUES(didsPaquete),
                  pendientes  = VALUES(pendientes),
                  autofecha   = NOW()
              `;
              await executeQuery(conn, upsert, [owner, cliente, chofer, estado, didsPaqueteStr, dia, pendientes]);
            } // dia
          } // estado
        } // chofer
      } // cliente
    } // owner

    await executeQuery(conn, "COMMIT");
  } catch (e) {
    await executeQuery(conn, "ROLLBACK");
    throw e;
  }

  // marcar cdc como procesado (en batches)
  if (idsProcesados.length > 0) {
    const CHUNK = 1000;
    for (let i = 0; i < idsProcesados.length; i += CHUNK) {
      const slice = idsProcesados.slice(i, i + CHUNK);
      const updCdc = `UPDATE cdc SET procesado=1 WHERE id IN (${slice.map(() => '?').join(',')})`;
      await executeQuery(conn, updCdc, slice);
      console.log("✅ CDC marcado como procesado para", slice.length, "rows");
    }
  }
}

async function pendientesHoy() {
  try {
    const conn = await getConnectionLocal();
    const FETCH = 1000;  // cuánto traigo de cdc por batch

    // Traigo 'estado' + 'fecha' desde cdc (incluye 'quien')
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

    await buildAprocesosEstado(rowsEstado, conn);
    await buildAprocesosAsignaciones(conn, rowsAsignaciones);

    await aplicarAprocesosAHommeApp(conn);

  } catch (err) {
    console.error("❌ Error batch:", err);
  }
}

pendientesHoy();

module.exports = { pendientesHoy };
