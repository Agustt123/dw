const { executeQuery } = require("../../db");

const ESTADO_ANY = 999;
const ESTADO_MOV_HOY = 998; // paquetesEnMovimientosHoy
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


async function cantidadGlobalDia(conn, fecha) {
  const sql = `
    SELECT
      SUM(
        CASE
          WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
          ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
        END
      ) AS cantidad
    FROM home_app
    WHERE dia = ?
      AND didCliente = 0
      AND didChofer  = 0
      AND estado     = ?;
  `;

  const rows = await executeQuery(conn, sql, [fecha, ESTADO_ANY], true);
  const cantidad = Number(rows?.[0]?.cantidad ?? 0);

  return { ok: true, cantidad, fecha };
}

// ✅ Devuelve total del mes + total del día (para la fecha que mandes)

async function cantidadGlobalMesYDia(conn, fecha) {
  const mesPrefix = String(fecha).slice(0, 7); // "YYYY-MM"

  const sql = `
    SELECT
      -- HOY (999)
      SUM(CASE
        WHEN estado = ? AND dia = ? THEN
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ELSE 0
      END) AS hoy,

      -- MES (999)
      SUM(CASE
        WHEN estado = ? AND dia LIKE CONCAT(?, '%') THEN
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ELSE 0
      END) AS mes,

      -- HOY MOVIMIENTO (998)
      SUM(CASE
        WHEN estado = ? AND dia = ? THEN
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ELSE 0
      END) AS hoyMovimiento

    FROM home_app
    WHERE didCliente = 0
      AND didChofer  = 0
  `;

  const params = [
    ESTADO_ANY, fecha,
    ESTADO_ANY, mesPrefix,
    ESTADO_MOV_HOY, fecha
  ];

  const rows = await executeQuery(conn, sql, params, true);

  const hoy = Number(rows?.[0]?.hoy ?? 0);
  const mes = Number(rows?.[0]?.mes ?? 0);
  const hoyMovimiento = Number(rows?.[0]?.hoyMovimiento ?? 0);

  const nombre = mesNombreES(fecha);

  return {
    ok: true,
    fecha,
    mes: mesPrefix,
    nombre,
    hoy,
    mesCantidad: mes,
    hoyMovimiento,
  };
}


module.exports = { cantidadGlobalDia, cantidadGlobalMesYDia };
