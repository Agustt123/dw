const { executeQuery, getConnectionLocal } = require("../../db");

// Mapa acumulador para ESTADOS
const AEstados = {};
const idsProcesados = [];

const TZ = 'America/Argentina/Buenos_Aires';
function getDiaFromTS(ts) {
  const d = new Date(ts);
  const ok = isNaN(d.getTime()) ? new Date() : d;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(ok); // 'YYYY-MM-DD'
}

// ---------- Builder: disparador = 'estado' (histórico acumulativo) ----------
async function buildEstados(rows) {
  for (const row of rows) {
    const OW = row.didOwner;
    const CLI = row.didCliente ?? 0;
    const EST = Number(row.estado);
    if (!OW || EST == null || Number.isNaN(EST)) continue;

    const dia = getDiaFromTS(row.fecha);
    const envId = String(row.didPaquete);

    if (!AEstados[OW]) AEstados[OW] = {};
    if (!AEstados[OW][CLI]) AEstados[OW][CLI] = {};
    if (!AEstados[OW][CLI][EST]) AEstados[OW][CLI][EST] = {};
    if (!AEstados[OW][CLI][EST][dia]) AEstados[OW][CLI][EST][dia] = new Set();

    // Histórico: solo sumamos positivos (no se resta cuando cambia de estado)
    AEstados[OW][CLI][EST][dia].add(envId);

    idsProcesados.push(row.id);
  }
}

// ---------- Apply: upsert a home_app (con columna 'estado') ----------
async function aplicarEstadosAHomeApp(conn) {
  // Requiere UNIQUE(didOwner, didCliente, estado, dia) en home_app
  const sel = `
    SELECT didsPaquete, pendientes
    FROM home_app
    WHERE didOwner = ? AND didCliente = ? AND estado = ? AND dia = ?
    LIMIT 1
  `;
  const upd = `
    UPDATE home_app
    SET didsPaquete = ?, pendientes = ?, autofecha = NOW()
    WHERE didOwner = ? AND didCliente = ? AND estado = ? AND dia = ?
  `;
  const ins = `
    INSERT INTO home_app
      (didOwner, didCliente, estado, didsPaquete, fecha, dia, pendientes)
    VALUES
      (?, ?, ?, ?, NOW(), ?, ?)
  `;

  for (const owner in AEstados) {
    for (const cliente in AEstados[owner]) {
      for (const estado in AEstados[owner][cliente]) {
        const porDia = AEstados[owner][cliente][estado];
        for (const dia in porDia) {
          const paquetesSet = porDia[dia];
          if (!paquetesSet || paquetesSet.size === 0) continue;

          const didsPaqueteStr = Array.from(paquetesSet).join(",");
          const pendientes = paquetesSet.size;

          const actual = await executeQuery(conn, sel, [owner, cliente, estado, dia]);

          if (actual.length > 0) {
            // merge no destructivo: mantené los ya existentes + nuevos
            const existentes = new Set(
              (actual[0].didsPaquete || "")
                .split(",")
                .map(s => s.trim())
                .filter(Boolean)
            );
            for (const p of paquetesSet) existentes.add(p);

            const nuevosStr = Array.from(existentes).join(",");
            const totalPend = existentes.size;

            await executeQuery(conn, upd, [nuevosStr, totalPend, owner, cliente, estado, dia]);
          } else {
            await executeQuery(conn, ins, [owner, cliente, estado, didsPaqueteStr, dia, pendientes]);
          }
        }
      }
    }
  }

  // marcar cdc como procesado (en batches)
  if (idsProcesados.length > 0) {
    const CHUNK = 1000;
    for (let i = 0; i < idsProcesados.length; i += CHUNK) {
      const slice = idsProcesados.slice(i, i + CHUNK);
      const updCdc = `UPDATE cdc SET procesado = 1 WHERE id IN (${slice.map(() => '?').join(',')})`;
      await executeQuery(conn, updCdc, slice);
    }
  }
}

// ---------- Runner ----------
async function pendientesHoy() {
  try {
    const conn = await getConnectionLocal();
    const FETCH = 1000;

    // Traer estados desde CDC (incluye columna 'estado')
    const qEstados = `
      SELECT id, didOwner, didPaquete, didCliente, fecha, estado, disparador, ejecutar
      FROM cdc
      WHERE procesado = 0
        AND ejecutar   = "estado"
        AND disparador = "estado"
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `;
    const rowsEstado = await executeQuery(conn, qEstados, [FETCH]);

    await buildEstados(rowsEstado);
    await aplicarEstadosAHomeApp(conn);

    console.log("✅ home_app actualizado por estado (histórico).");

  } catch (err) {
    console.error("❌ Error pendientesHoy (estados):", err);
  }
}

pendientesHoy();

module.exports = { pendientesHoy };
