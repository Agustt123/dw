const { executeQuery } = require("../../db");

const ESTADO_ANY = 999;
const ESTADO_MOV_HOY = 998; // paquetesEnMovimientosHoy
const CANTIDAD_PAQUETES_TIMEOUT_MS = 600000;

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
  const { mesPrefix, inicioMes, inicioMesSiguiente, inicioAnio, inicioAnioSiguiente } =
    getFechaPartes(fecha);

  const hoy = await contarPaquetes(
    conn,
    "dia = ? AND estado = ?",
    [fecha, ESTADO_ANY]
  );

  const mes = await contarPaquetes(
    conn,
    "dia >= ? AND dia < ? AND estado = ?",
    [inicioMes, inicioMesSiguiente, ESTADO_ANY]
  );

  const anioCantidad = await contarPaquetes(
    conn,
    "dia >= ? AND dia < ? AND estado = ?",
    [inicioAnio, inicioAnioSiguiente, ESTADO_ANY]
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

module.exports = { cantidadGlobalDia, cantidadGlobalMesYDia };
