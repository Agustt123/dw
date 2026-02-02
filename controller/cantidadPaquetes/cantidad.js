const { executeQuery } = require("../../db");

const ESTADO_ANY = 999;
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
      SUM(CASE
        WHEN dia = ? THEN
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ELSE 0
      END) AS cantidadDia,

      SUM(CASE
        WHEN dia LIKE CONCAT(?, '%') THEN
          CASE
            WHEN didsPaquete IS NULL OR didsPaquete = '' THEN 0
            ELSE 1 + (LENGTH(didsPaquete) - LENGTH(REPLACE(didsPaquete, ',', '')))
          END
        ELSE 0
      END) AS cantidadMes
    FROM home_app
    WHERE didCliente = 0
      AND didChofer  = 0
      AND estado     = ?;
  `;

  const rows = await executeQuery(conn, sql, [fecha, mesPrefix, ESTADO_ANY], true);

  const cantidadDia = Number(rows?.[0]?.cantidadDia ?? 0);
  const cantidadMes = Number(rows?.[0]?.cantidadMes ?? 0);

  const nombre = mesNombreES(fecha); // ✅ "Febrero 2026"

  return {
    ok: true,
    fecha,
    mes: mesPrefix,
    nombre,
    cantidadDia,
    cantidadMes,
  };
}


module.exports = { cantidadGlobalDia, cantidadGlobalMesYDia };
