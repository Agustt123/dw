const { executeQuery, redisClient } = require("../../db");

const ESTADO_ANY = 999;
const ESTADO_MOV_HOY = 998; // paquetesEnMovimientosHoy
const CANTIDAD_PAQUETES_TIMEOUT_MS = 600000;
const CANTIDAD_CACHE_TTL_SECONDS = 60;
const CANTIDAD_CACHE_VERSION = "v3";
const cantidadCacheEnVuelo = new Map();

function mesNombreES(fechaYYYYMMDD) {
  const MESES = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
  ];

  const partes = String(fechaYYYYMMDD).split("-");
  const mesNum = Number(partes[1]); // "02" -> 2

  return MESES[mesNum - 1] || "";
}

function getFechaPartes(fecha) {
  const [anioStr, mesStr, diaStr] = String(fecha || "").split("-");
  const anio = Number(anioStr);
  const mes = Number(mesStr);
  const dia = Number(diaStr);

  if (!anio || !mes || !dia) {
    throw new Error(`Fecha invalida: ${fecha}`);
  }

  const pad = (n) => String(n).padStart(2, "0");
  const inicioMes = `${anio}-${pad(mes)}-01`;
  const inicioMesSiguiente =
    mes === 12 ? `${anio + 1}-01-01` : `${anio}-${pad(mes + 1)}-01`;
  const inicioAnio = `${anio}-01-01`;
  const inicioAnioSiguiente = `${anio + 1}-01-01`;

  return {
    anio,
    mesPrefix: `${anio}-${pad(mes)}`,
    inicioMes,
    inicioMesSiguiente,
    inicioAnio,
    inicioAnioSiguiente,
  };
}

function getCantidadCacheKey(fecha) {
  return `cantidad:global:${CANTIDAD_CACHE_VERSION}:${fecha}`;
}

async function getCantidadCache(fecha) {
  try {
    const raw = await redisClient.get(getCantidadCacheKey(fecha));
    if (!raw) return null;

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;

    return parsed;
  } catch (_) {
    return null;
  }
}

async function setCantidadCache(fecha, value) {
  try {
    await redisClient.setEx(
      getCantidadCacheKey(fecha),
      CANTIDAD_CACHE_TTL_SECONDS,
      JSON.stringify(value)
    );
  } catch (_) {
    // Si Redis falla, no rompemos el endpoint.
  }
}

async function contarPaquetes(conn, whereSql, params) {
  const sql = `
    SELECT
      COALESCE(
        SUM(
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ),
        0
      ) AS cantidad
    FROM home_app
    WHERE didCliente = 0
      AND didChofer = 0
      AND ${whereSql}
  `;

  const rows = await executeQuery(conn, sql, params, {
    timeoutMs: CANTIDAD_PAQUETES_TIMEOUT_MS,
  });

  return Number(rows?.[0]?.cantidad ?? 0);
}

async function cantidadGlobalDia(conn, fecha) {
  const cantidad = await contarPaquetes(
    conn,
    "dia = ? AND estado = ?",
    [fecha, ESTADO_ANY]
  );

  return { ok: true, cantidad, fecha };
}

// Devuelve total del mes + total del dia (para la fecha que mandes)
async function cantidadGlobalMesYDia(conn, fecha) {
  const { anio, mesPrefix } = getFechaPartes(fecha);
  const hoy = await contarPaquetes(
    conn,
    "dia = ? AND estado = ?",
    [fecha, ESTADO_ANY]
  );
  const mes = await contarPaquetes(
    conn,
    "dia LIKE CONCAT(?, '%') AND estado = ?",
    [mesPrefix, ESTADO_ANY]
  );
  const anioCantidad = await contarPaquetes(
    conn,
    "dia LIKE CONCAT(?, '-%') AND estado = ?",
    [String(anio), ESTADO_ANY]
  );
  const hoyMovimiento = await contarPaquetes(
    conn,
    "dia = ? AND estado = ?",
    [fecha, ESTADO_MOV_HOY]
  );
  const nombre = mesNombreES(fecha);

  return {
    ok: true,
    fecha,
    mes: mesPrefix,
    nombre,
    hoy,
    mesCantidad: mes,
    hoyMovimiento,
    añoCantidad: anioCantidad,
    anioCantidad,
  };
}

async function cantidadGlobalMesYDiaCached(connFactory, fecha) {
  const cached = await getCantidadCache(fecha);
  if (cached) return cached;

  const cacheKey = getCantidadCacheKey(fecha);
  if (cantidadCacheEnVuelo.has(cacheKey)) {
    return cantidadCacheEnVuelo.get(cacheKey);
  }

  const pending = (async () => {
    let conn;
    try {
      conn = await connFactory();
      const resultado = await cantidadGlobalMesYDia(conn, fecha);
      await setCantidadCache(fecha, resultado);
      return resultado;
    } finally {
      cantidadCacheEnVuelo.delete(cacheKey);
      if (conn?.release) {
        try {
          conn.release();
        } catch (_) {}
      }
    }
  })();

  cantidadCacheEnVuelo.set(cacheKey, pending);
  return pending;
}

module.exports = {
  cantidadGlobalDia,
  cantidadGlobalMesYDia,
  cantidadGlobalMesYDiaCached,
};
