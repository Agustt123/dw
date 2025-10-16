const { executeQuery, getConnectionLocal } = require("../../db");

// Acumuladores
// ESTADOS: owner -> cliente -> estado -> dia -> Set(paquetes)
const AEstados = {};
// CHOFERES: owner -> cliente -> chofer -> dia -> { 1:[], 0:[], est: <num|null> }
const AChoferes = {};
const idsProcesados = [];

const TZ = 'America/Argentina/Buenos_Aires';
function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(ok); // 'YYYY-MM-DD'
}

/* =========================
   HELPERS
   ========================= */

const estadoCache = new Map();
async function resolverEstado(conn, row) {
  // si viene en CDC, usar ese
  if (row.estado !== undefined && row.estado !== null) {
    const n = Number(row.estado);
    return Number.isFinite(n) ? n : null;
  }
  // sino, resolver por fecha (ultimo <= fecha)
  const key = `${row.didOwner}|${row.didPaquete}|${row.fecha}`;
  if (estadoCache.has(key)) return estadoCache.get(key);
  const q = `
    SELECT estado
    FROM estado
    WHERE didOwner = ? AND didEnvio = ? AND autofecha <= ?
    ORDER BY autofecha DESC, id DESC
    LIMIT 1
  `;
  const rs = await executeQuery(conn, q, [row.didOwner, row.didPaquete, row.fecha]);
  const est = rs.length ? Number(rs[0].estado) : null;
  const norm = Number.isFinite(est) ? est : null;
  estadoCache.set(key, norm);
  return norm;
}

const normEstado = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null; // jamás NaN a MySQL
};

/* =========================
   BUILDERS
   ========================= */

// HISTORIAL por ESTADO (solo disparador='estado')
async function buildEstados(conn, rowsEstado) {
  for (const row of rowsEstado) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    if (!OW) continue;

    const EST = await resolverEstado(conn, row);
    if (EST == null) continue; // no contaminar

    const dia = getDiaFromTS(row.fecha);
    const envId = String(row.didPaquete);

    if (!AEstados[OW]) AEstados[OW] = {};
    if (!AEstados[OW][CLI]) AEstados[OW][CLI] = {};
    if (!AEstados[OW][CLI][EST]) AEstados[OW][CLI][EST] = {};
    if (!AEstados[OW][CLI][EST][dia]) AEstados[OW][CLI][EST][dia] = new Set();

    // Histórico: solo sumamos (no resta nunca)
    AEstados[OW][CLI][EST][dia].add(envId);

    idsProcesados.push(row.id);
  }
}

// FOTO por CHOFER (disparador='asignaciones'): + en chofer actual, - en chofer anterior
async function buildChoferes(conn, rowsAsign) {
  for (const row of rowsAsign) {
    const Ow = row.didOwner;
    const Cli = row.didCliente || 0;
    const Cho = row.didChofer || 0;    // Cho != 0 => asignación; Cho == 0 => desasignación
    const envio = String(row.didPaquete);
    if (!Ow) continue;

    const dia = getDiaFromTS(row.fecha);

    // estado “de referencia” del evento
    const est = await resolverEstado(conn, row); // puede ser null

    if (!AChoferes[Ow]) AChoferes[Ow] = {};
    if (!AChoferes[Ow][Cli]) AChoferes[Ow][Cli] = {};
    if (!AChoferes[Ow][Cli][Cho]) AChoferes[Ow][Cli][Cho] = {};
    if (!AChoferes[Ow][Cli][Cho][dia]) AChoferes[Ow][Cli][Cho][dia] = { 1: [], 0: [], est: null };

    const nodo = AChoferes[Ow][Cli][Cho][dia];
    if (est !== null) nodo.est = est; // guardo el último estado visto para esa combinación

    // POSITIVO en chofer actual (si Cho != 0)
    if (Cho !== 0) {
      nodo[1].push(envio);
    }

    // dentro de buildChoferes
    const qPrev = `
  SELECT operador AS didChofer
  FROM asignaciones
  WHERE didEnvio = ? AND didOwner = ?
    AND operador IS NOT NULL
    AND autofecha < ?              -- 👈 anterior a ESTE evento
  ORDER BY autofecha DESC, id DESC
  LIMIT 1
`;
    const prev = await executeQuery(conn, qPrev, [envio, Ow, row.fecha]);

    if (prev.length) {
      const choPrev = prev[0].didChofer || 0;
      if (choPrev !== 0) {
        if (!AChoferes[Ow][Cli][choPrev]) AChoferes[Ow][Cli][choPrev] = {};
        if (!AChoferes[Ow][Cli][choPrev][dia]) AChoferes[Ow][Cli][choPrev][dia] = { 1: [], 0: [], est: null };
        AChoferes[Ow][Cli][choPrev][dia][0].push(envio);
      }
    }

    idsProcesados.push(row.id);
  }
}

/* =========================
   APPLY
   ========================= */

// HISTÓRICO por ESTADO (pendientes=0 siempre)
async function applyEstados(conn) {
  const sel = `
    SELECT didsPaquete
    FROM home_app
    WHERE didOwner = ? AND didCliente = ? AND estado = ? AND dia = ?
    LIMIT 1
  `;
  const upd = `
    UPDATE home_app
    SET didsPaquete = ?, pendientes = 0, autofecha = NOW()
    WHERE didOwner = ? AND didCliente = ? AND estado = ? AND dia = ?
  `;
  const ins = `
    INSERT INTO home_app
      (didOwner, didCliente, estado, didsPaquete, fecha, dia, pendientes)
    VALUES
      (?, ?, ?, ?, NOW(), ?, 0)
  `;

  for (const owner in AEstados) {
    for (const cliente in AEstados[owner]) {
      for (const estado in AEstados[owner][cliente]) {
        const porDia = AEstados[owner][cliente][estado];
        for (const dia in porDia) {
          const setPaq = porDia[dia];
          if (!setPaq || setPaq.size === 0) continue;

          // merge ∪ con existentes
          const actual = await executeQuery(conn, sel, [owner, cliente, estado, dia]);
          let union = new Set();
          if (actual.length && actual[0].didsPaquete) {
            for (const p of actual[0].didsPaquete.split(',')) {
              const t = p.trim(); if (t) union.add(t);
            }
          }
          for (const p of setPaq) union.add(String(p));

          const unionStr = Array.from(union).join(',');

          if (actual.length > 0) {
            await executeQuery(conn, upd, [unionStr, owner, cliente, estado, dia]);
          } else {
            await executeQuery(conn, ins, [owner, cliente, estado, unionStr, dia]);
          }
        }
      }
    }
  }
}

// FOTO por CHOFER: + / - en didsPaquete, pendientes=0 siempre, estado se actualiza con el último conocido (si hay)
async function applyChoferes(conn) {
  const sel = `
    SELECT didsPaquete
    FROM home_app
    WHERE didOwner = ? AND didCliente = ? AND didChofer = ? AND dia = ?
    LIMIT 1
  `;
  const upd = `
    UPDATE home_app
    SET didsPaquete = ?, pendientes = 0, estado = COALESCE(?, estado), autofecha = NOW()
    WHERE didOwner = ? AND didCliente = ? AND didChofer = ? AND dia = ?
  `;
  const ins = `
    INSERT INTO home_app
      (didOwner, didCliente, didChofer, didsPaquete, fecha, dia, pendientes, estado)
    VALUES
      (?, ?, ?, ?, NOW(), ?, 0, ?)
  `;

  for (const owner in AChoferes) {
    for (const cliente in AChoferes[owner]) {
      for (const chofer in AChoferes[owner][cliente]) {
        const porDia = AChoferes[owner][cliente][chofer];
        for (const dia in porDia) {
          const nodo = porDia[dia]; // {1:[],0:[], est}
          const pos = [...new Set(nodo[1])];
          const neg = [...new Set(nodo[0])];
          const estUltimo = normEstado(nodo?.est);

          if (pos.length === 0 && neg.length === 0 && estUltimo === null) continue;

          const actual = await executeQuery(conn, sel, [owner, cliente, chofer, dia]);

          // partimos de lo existente
          const paquetes = new Set(
            actual.length && actual[0].didsPaquete
              ? actual[0].didsPaquete.split(',').map(s => s.trim()).filter(Boolean)
              : []
          );

          // aplicar +/-
          for (const p of pos) paquetes.add(String(p));
          for (const p of neg) paquetes.delete(String(p));

          const didsPaqueteStr = Array.from(paquetes).join(",");

          if (actual.length > 0) {
            await executeQuery(conn, upd, [
              didsPaqueteStr,
              estUltimo,                // si es null, mantiene el anterior
              owner, cliente, chofer, dia
            ]);
          } else {
            await executeQuery(conn, ins, [
              owner, cliente, chofer,
              didsPaqueteStr,
              dia,
              estUltimo                 // puede ser null
            ]);
          }
        }
      }
    }
  }
}

/* =========================
   RUNNER
   ========================= */

async function pendientesHoy() {
  try {
    const conn = await getConnectionLocal();
    const FETCH = 1000;

    // CDC: separar por disparador
    const q = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, fecha, estado, disparador, ejecutar
      FROM cdc
      WHERE procesado = 0
        AND ejecutar = "estado"
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;
    const rows = await executeQuery(conn, q, [FETCH]);

    const rowsEstado = rows.filter(r => r.disparador === "estado");
    const rowsAsign = rows.filter(r => r.disparador === "asignaciones");

    await buildEstados(conn, rowsEstado);   // ← SOLO 'estado' para histórico
    await buildChoferes(conn, rowsAsign);   // ← 'asignaciones' para foto chofer (+/-)

    await applyEstados(conn);
    await applyChoferes(conn);

    // marcar CDC como procesado
    if (idsProcesados.length > 0) {
      const CHUNK = 1000;
      for (let i = 0; i < idsProcesados.length; i += CHUNK) {
        const slice = idsProcesados.slice(i, i + CHUNK);
        const updCdc = `UPDATE cdc SET procesado = 1 WHERE id IN (${slice.map(() => '?').join(',')})`;
        await executeQuery(conn, updCdc, slice);
      }
    }

    console.log("✅ home_app actualizado: HISTORIAL por estado (sin restas) + FOTO por chofer (con desasignación). pendientes=0");

  } catch (err) {
    console.error("❌ Error pendientesHoy:", err);
  }
}

pendientesHoy();

module.exports = { pendientesHoy };
