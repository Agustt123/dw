const { executeQuery, getConnectionLocal } = require("../../db");

// Acumuladores
// HISTORIAL (chofer=0): owner -> cliente -> estado -> dia -> Set(paquetes)
const AEstados = {};
// FOTO por chofer: owner -> cliente -> chofer -> estado -> dia -> { add:Set, del:Set }
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

const nEstado = v => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

/* =========================
   BUILDERS
   ========================= */

// HISTORIAL por ESTADO (disparador='estado'): guardamos en chofer=0
// además, si querés el total por owner (cliente=0), sumo también (owner,0,0,estado,dia)
async function buildEstados(_conn, rowsEstado) {
  for (const row of rowsEstado) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const EST = nEstado(row.estado);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const envio = String(row.didPaquete);

    // owner / cliente / chofer=0 / estado / dia
    if (!AEstados[OW]) AEstados[OW] = {};
    if (!AEstados[OW][CLI]) AEstados[OW][CLI] = {};
    if (!AEstados[OW][CLI][EST]) AEstados[OW][CLI][EST] = {};
    if (!AEstados[OW][CLI][EST][dia]) AEstados[OW][CLI][EST][dia] = new Set();
    AEstados[OW][CLI][EST][dia].add(envio);

    // (opcional) owner / cliente=0 / chofer=0 / estado / dia  → total por owner
    if (!AEstados[OW][0]) AEstados[OW][0] = {};
    if (!AEstados[OW][0][EST]) AEstados[OW][0][EST] = {};
    if (!AEstados[OW][0][EST][dia]) AEstados[OW][0][EST][dia] = new Set();
    AEstados[OW][0][EST][dia].add(envio);

    idsProcesados.push(row.id);
  }
}

// FOTO por CHOFER (disparador='asignaciones'): + al chofer actual, - al chofer anterior
// SIEMPRE usando el estado que viene en CDC (misma clave incluye estado)
async function buildChoferes(conn, rowsAsign) {
  for (const row of rowsAsign) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const CHO = row.didChofer ?? 0;     // 0 = desasignación
    const EST = nEstado(row.estado);
    if (!OW || EST === null) continue;

    const dia = getDiaFromTS(row.fecha);
    const envio = String(row.didPaquete);

    // helper para anidar
    const ensure = (o, k) => (o[k] ??= {});
    const addNode = (owner, cli, cho, est, day) => {
      ensure(AChoferes, owner);
      ensure(AChoferes[owner], cli);
      ensure(AChoferes[owner][cli], cho);
      ensure(AChoferes[owner][cli][cho], est);
      ensure(AChoferes[owner][cli][cho][est], day);
      const cur = AChoferes[owner][cli][cho][est][day];
      if (!cur.add) cur.add = new Set();
      if (!cur.del) cur.del = new Set();
      return cur;
    };

    // + al chofer actual (si CHO != 0)
    if (CHO !== 0) {
      // por cliente específico
      addNode(OW, CLI, CHO, EST, dia).add.add(envio);
      // por chofer con todos los clientes (cliente=0)
      addNode(OW, 0, CHO, EST, dia).add.add(envio);
    }

    // buscar chofer anterior (antes de ESTE evento)
    const qPrev = `
      SELECT operador AS didChofer
      FROM asignaciones
      WHERE didEnvio = ? AND didOwner = ? AND operador IS NOT NULL
        AND autofecha < ?
      ORDER BY autofecha DESC, id DESC
      LIMIT 1
    `;
    const prev = await executeQuery(conn, qPrev, [envio, OW, row.fecha]);
    if (prev.length) {
      const choPrev = prev[0].didChofer ?? 0;
      if (choPrev !== 0) {
        // - al chofer anterior (misma combinación de estado)
        addNode(OW, CLI, choPrev, EST, dia).del.add(envio);
        addNode(OW, 0, choPrev, EST, dia).del.add(envio);
      }
    }

    idsProcesados.push(row.id);
  }
}

/* =========================
   APPLY
   ========================= */

// HISTORIAL: upsert sobre (owner, cliente, chofer=0, estado, dia)
async function applyEstados(conn) {
  const sel = `
    SELECT didsPaquete
    FROM home_app
    WHERE didOwner=? AND didCliente=? AND didChofer=0 AND estado=? AND dia=?
    LIMIT 1
  `;
  const upd = `
    UPDATE home_app
    SET didsPaquete=?, pendientes=0, autofecha=NOW()
    WHERE didOwner=? AND didCliente=? AND didChofer=0 AND estado=? AND dia=?
  `;
  const ins = `
    INSERT INTO home_app
      (didOwner, didCliente, didChofer, estado, didsPaquete, fecha, dia, pendientes)
    VALUES
      (?, ?, 0, ?, ?, NOW(), ?, 0)
  `;

  for (const owner in AEstados) {
    for (const cliente in AEstados[owner]) {
      for (const estado in AEstados[owner][cliente]) {
        const porDia = AEstados[owner][cliente][estado];
        for (const dia in porDia) {
          const setPaq = porDia[dia];
          if (!setPaq || setPaq.size === 0) continue;

          const actual = await executeQuery(conn, sel, [owner, cliente, estado, dia]);

          // merge ∪
          const union = new Set();
          if (actual.length && actual[0].didsPaquete) {
            for (const p of String(actual[0].didsPaquete).split(',')) {
              const t = p.trim(); if (t) union.add(t);
            }
          }
          for (const p of setPaq) union.add(String(p));

          const didsStr = Array.from(union).join(',');

          if (actual.length > 0) {
            await executeQuery(conn, upd, [didsStr, owner, cliente, estado, dia]);
          } else {
            await executeQuery(conn, ins, [owner, cliente, estado, didsStr, dia]);
          }
        }
      }
    }
  }
}

// FOTO por CHOFER: upsert sobre (owner, cliente, chofer, estado, dia)
// aplica + y - y mantiene pendientes=0
async function applyChoferes(conn) {
  const sel = `
    SELECT didsPaquete
    FROM home_app
    WHERE didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?
    LIMIT 1
  `;
  const upd = `
    UPDATE home_app
    SET didsPaquete=?, pendientes=0, autofecha=NOW()
    WHERE didOwner=? AND didCliente=? AND didChofer=? AND estado=? AND dia=?
  `;
  const ins = `
    INSERT INTO home_app
      (didOwner, didCliente, didChofer, estado, didsPaquete, fecha, dia, pendientes)
    VALUES
      (?, ?, ?, ?, ?, NOW(), ?, 0)
  `;

  for (const owner in AChoferes) {
    for (const cliente in AChoferes[owner]) {
      for (const chofer in AChoferes[owner][cliente]) {
        for (const estado in AChoferes[owner][cliente][chofer]) {
          const porDia = AChoferes[owner][cliente][chofer][estado];
          for (const dia in porDia) {
            const nodo = porDia[dia];
            const add = nodo.add ? [...nodo.add] : [];
            const del = nodo.del ? [...nodo.del] : [];
            if (add.length === 0 && del.length === 0) continue;

            const actual = await executeQuery(conn, sel, [owner, cliente, chofer, estado, dia]);

            const paquetes = new Set(
              actual.length && actual[0].didsPaquete
                ? String(actual[0].didsPaquete).split(',').map(s => s.trim()).filter(Boolean)
                : []
            );
            for (const p of add) paquetes.add(String(p));
            for (const p of del) paquetes.delete(String(p));

            const didsStr = Array.from(paquetes).join(',');

            if (actual.length > 0) {
              await executeQuery(conn, upd, [didsStr, owner, cliente, chofer, estado, dia]);
            } else {
              await executeQuery(conn, ins, [owner, cliente, chofer, estado, didsStr, dia]);
            }
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

    const q = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, fecha, estado, disparador, ejecutar
      FROM cdc
      WHERE procesado=0
        AND ejecutar="estado"
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;
    const rows = await executeQuery(conn, q, [FETCH]);

    const rowsEstado = rows.filter(r => r.disparador === "estado");
    const rowsAsign = rows.filter(r => r.disparador === "asignaciones");

    await buildEstados(conn, rowsEstado);   // historial (chofer=0)
    await buildChoferes(conn, rowsAsign);   // foto por chofer (+/-)

    await applyEstados(conn);
    await applyChoferes(conn);

    // marcar CDC como procesado
    if (idsProcesados.length > 0) {
      const CHUNK = 1000;
      for (let i = 0; i < idsProcesados.length; i += CHUNK) {
        const slice = idsProcesados.slice(i, i + CHUNK);
        const updCdc = `UPDATE cdc SET procesado=1 WHERE id IN (${slice.map(() => '?').join(',')})`;
        await executeQuery(conn, updCdc, slice);
      }
    }

    console.log("✅ home_app actualizado: HISTORIAL por estado (chofer=0) + FOTO por chofer (±), todos con clave (owner,cliente,chofer,estado,dia) y pendientes=0");
  } catch (err) {
    console.error("❌ Error pendientesHoy:", err);
  }
}

pendientesHoy();

module.exports = { pendientesHoy };
