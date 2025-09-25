// home_app_batch_estados.js
// Ahora procesa "todo": una fila por (owner, cliente, chofer, estado, día)

const { executeQuery, getConnectionLocal } = require("../../db");

const Aprocesos = {};
const idsProcesados = [];
const LIMIT = 50;

const TZ = 'America/Argentina/Buenos_Aires';
function getDiaFromTS(ts) {
    const d = new Date(ts);
    const ok = isNaN(d.getTime()) ? new Date() : d;
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit'
    }).format(ok); // 'YYYY-MM-DD'
}

// ---------- Helpers ----------
function ensureNodo(owner, cliente, chofer, estado, dia) {
    if (!Aprocesos[owner]) Aprocesos[owner] = {};
    if (!Aprocesos[owner][cliente]) Aprocesos[owner][cliente] = {};
    if (!Aprocesos[owner][cliente][chofer]) Aprocesos[owner][cliente][chofer] = {};
    if (!Aprocesos[owner][cliente][chofer][estado]) Aprocesos[owner][cliente][chofer][estado] = {};
    if (!Aprocesos[owner][cliente][chofer][estado][dia]) {
        Aprocesos[owner][cliente][chofer][estado][dia] = { 1: [], 0: [] };
    }
    return Aprocesos[owner][cliente][chofer][estado][dia];
}

function safeEstado(e) {
    const v = (e ?? "").toString().trim();
    return v.length ? v : "DESCONOCIDO";
}

// ---------- Builder para disparador = 'estado' ----------
// Mueve el envío del estado anterior al nuevo, por día (en agregados owner/0/0 y owner/cliente/0).
async function buildAprocesosEstado(rows, conn) {
    for (const row of rows) {
        const OW = row.didOwner;
        const CLI = row.didCliente ?? 0;
        if (!OW) continue;

        const envio = String(row.didPaquete);
        const dia = getDiaFromTS(row.fecha);
        const estadoNuevo = safeEstado(row.estado);

        // Estado anterior (al momento de este evento de estado)
        const qPrevEstado = `
      SELECT estado
      FROM estado
      WHERE didEnvio = ? AND didOwner = ? AND id < ?
      ORDER BY id DESC
      LIMIT 1
    `;
        const prev = await executeQuery(conn, qPrevEstado, [envio, OW, row.id]);
        const estadoPrevio = prev.length ? safeEstado(prev[0].estado) : null;

        // (+) nuevo estado en agregados
        ensureNodo(OW, 0, 0, estadoNuevo, dia)[1].push(envio);
        ensureNodo(OW, CLI, 0, estadoNuevo, dia)[1].push(envio);

        // (−) estado previo en agregados (si existe y es distinto)
        if (estadoPrevio && estadoPrevio !== estadoNuevo) {
            ensureNodo(OW, 0, 0, estadoPrevio, dia)[0].push(envio);
            ensureNodo(OW, CLI, 0, estadoPrevio, dia)[0].push(envio);
        }

        idsProcesados.push(row.id);
    }
    return Aprocesos;
}

// ---------- Builder para disparador = 'asignaciones' ----------
// Mueve el envío entre choferes dentro del MISMO estado vigente en ese momento.
async function buildAprocesosAsignaciones(conn, rows) {
    for (const row of rows) {
        const OW = row.didOwner;
        const CLI = row.didCliente || 0;
        const CHO = row.didChofer || 0; // CHO != 0 => asignación; CHO == 0 => desasignación
        const envio = String(row.didPaquete);
        if (!OW) continue;

        const dia = getDiaFromTS(row.fecha);

        // Estado vigente al momento de esta asignación
        const qEstadoActual = `
      SELECT estado
      FROM estado
      WHERE didEnvio = ? AND didOwner = ? AND id <= ?
      ORDER BY id DESC
      LIMIT 1
    `;
        const cur = await executeQuery(conn, qEstadoActual, [envio, OW, row.id]);
        const estadoVigente = cur.length ? safeEstado(cur[0].estado) : "DESCONOCIDO";

        // (+) chofer actual si hay
        if (CHO !== 0) {
            ensureNodo(OW, CLI, CHO, estadoVigente, dia)[1].push(envio);
        }

        // (−) chofer anterior real (si existía), dentro del mismo estado vigente
        const qChoferAnterior = `
      SELECT operador AS didChofer
      FROM asignaciones
      WHERE didEnvio = ? AND didOwner = ? AND operador IS NOT NULL AND id < ?
      ORDER BY id DESC
      LIMIT 1
    `;
        const prev = await executeQuery(conn, qChoferAnterior, [envio, OW, row.id]);
        const choPrev = prev.length ? (prev[0].didChofer || 0) : 0;

        if (choPrev !== 0) {
            ensureNodo(OW, CLI, choPrev, estadoVigente, dia)[0].push(envio);
        }

        idsProcesados.push(row.id);
    }
    return Aprocesos;
}

// ---------- Aplicación a home_app (incluye 'estado' en la clave) ----------
async function aplicarAprocesosAHommeApp(conn) {
    for (const owner in Aprocesos) {
        const porCliente = Aprocesos[owner];

        for (const cliente in porCliente) {
            const porChofer = porCliente[cliente];

            for (const chofer in porChofer) {
                const porEstado = porChofer[chofer];

                for (const estado in porEstado) {
                    const porDia = porEstado[estado]; // { [dia]: {1:[],0:[]} }

                    for (const dia in porDia) {
                        const nodo = porDia[dia];
                        const pos = [...new Set(nodo[1])]; // positivos
                        const neg = [...new Set(nodo[0])]; // negativos
                        if (pos.length === 0 && neg.length === 0) continue;

                        // leer snapshot actual para MISMO DÍA + ESTADO
                        const sel = `
              SELECT didsPaquete, pendientes
              FROM home_app
              WHERE didOwner = ? AND didCliente = ? AND didChofer = ?
                AND estado = ? AND dia = ?
              LIMIT 1
            `;
                        const actual = await executeQuery(conn, sel, [owner, cliente, chofer, estado, dia]);

                        // parsear paquetes actuales a Set
                        let paquetes = new Set();
                        let contador = 0; // contador por estado
                        if (actual.length > 0) {
                            const s = actual[0].didsPaquete || "";
                            if (s) {
                                for (const p of s.split(",")) {
                                    const t = p.trim();
                                    if (t) paquetes.add(t);
                                }
                            }
                            contador = actual[0].pendientes || 0; // usamos 'pendientes' como "count por estado"
                        }

                        // aplicar positivos
                        for (const p of pos) {
                            const k = String(p);
                            if (!paquetes.has(k)) {
                                paquetes.add(k);
                                contador += 1;
                            }
                        }

                        // aplicar negativos
                        for (const p of neg) {
                            const k = String(p);
                            if (paquetes.has(k)) {
                                paquetes.delete(k);
                                contador = Math.max(0, contador - 1);
                            }
                        }

                        const didsPaqueteStr = Array.from(paquetes).join(",");

                        if (actual.length > 0) {
                            const upd = `
                UPDATE home_app
                SET didsPaquete = ?, pendientes = ?, autofecha = NOW()
                WHERE didOwner = ? AND didCliente = ? AND didChofer = ?
                  AND estado = ? AND dia = ?
              `;
                            await executeQuery(conn, upd, [didsPaqueteStr, contador, owner, cliente, chofer, estado, dia]);
                        } else {
                            const ins = `
                INSERT INTO home_app
                  (didOwner, didCliente, didChofer, estado, didsPaquete, fecha, dia, pendientes)
                VALUES
                  (?, ?, ?, ?, ?, NOW(), ?, ?)
              `;
                            await executeQuery(conn, ins, [owner, cliente, chofer, estado, didsPaqueteStr, dia, contador]);
                        }
                    } // for dia
                } // for estado
            } // for chofer
        } // for cliente
    } // for owner

    // marcar cdc como procesado (en batches)
    if (idsProcesados.length > 0) {
        const CHUNK = 1000;
        for (let i = 0; i < idsProcesados.length; i += CHUNK) {
            const slice = idsProcesados.slice(i, i + CHUNK);
            const updCdc = `UPDATE cdc SET procesado = 1 WHERE id IN (${slice.map(() => '?').join(',')})`;
            await executeQuery(conn, updCdc, slice);
            console.log("✅ CDC marcado como procesado para", slice.length, "rows");
        }
    }
}

// ---------- Job principal ----------
async function homeAppHoy() {
    try {
        const conn = await getConnectionLocal();
        const FETCH = 1000; // cuánto traigo de cdc por batch

        // 🔸 Traigo fecha desde cdc (día del evento) + estado (si viene)
        const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer,
             disparador, ejecutar, fecha, estado
      FROM cdc
      WHERE procesado = 0
        AND ejecutar   = "estado"   
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;

        const rows = await executeQuery(conn, selectCDC, [FETCH]);

        const rowsEstado = rows.filter(r => r.disparador === "estado");
        const rowsAsignaciones = rows.filter(r => r.disparador === "asignaciones");

        // Procesar ESTADO (migración de un estado a otro en agregados)
        await buildAprocesosEstado(rowsEstado, conn);

        // Procesar ASIGNACIONES (mover entre choferes dentro del estado vigente)
        await buildAprocesosAsignaciones(conn, rowsAsignaciones);

        console.log("[Aprocesos]", JSON.stringify(Aprocesos, null, 2));
        console.log("idsProcesados:", idsProcesados);

        await aplicarAprocesosAHommeApp(conn);

    } catch (err) {
        console.error("❌ Error batch:", err);
    }
}

// Ejecutar
homeAppHoy();

module.exports = {
    homeAppHoy
};
