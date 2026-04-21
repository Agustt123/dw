const { getConnectionLocalEnvios, executeQuery } = require("../../db");
const { calculatePenalizada } = require("./penalizadaRules");
const { resolveCountryCodeForOwner } = require("./countryResolver");

const QUERY_TIMEOUT_MS = 5 * 60 * 1000;

async function ensurePenalizadaColumn(conn) {
    const rows = await executeQuery(
        conn,
        "SHOW COLUMNS FROM envios LIKE 'penalizada'",
        [],
        { timeoutMs: QUERY_TIMEOUT_MS }
    );

    if (rows.length > 0) {
        return;
    }

    await executeQuery(
        conn,
        `
        ALTER TABLE envios
        ADD COLUMN penalizada TINYINT NULL DEFAULT NULL
        `,
        [],
        { timeoutMs: QUERY_TIMEOUT_MS }
    );
}

async function fetchEnvios(conn, lookbackDays) {
    return await executeQuery(
        conn,
        `
        SELECT id, didOwner, didEnvio, fecha_despacho, penalizada
        FROM envios
        WHERE superado = 0
          AND elim = 0
          AND fecha_despacho IS NOT NULL
          AND DATE(fecha_despacho) >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        ORDER BY id ASC
        `,
        [lookbackDays],
        { timeoutMs: QUERY_TIMEOUT_MS }
    );
}

async function fetchEstados(conn, envio) {
    return await executeQuery(
        conn,
        `
        SELECT estado, fecha
        FROM estado
        WHERE didOwner = ?
          AND didEnvio = ?
          AND estado IN (5, 6)
        ORDER BY fecha ASC
        `,
        [envio.didOwner, envio.didEnvio],
        { timeoutMs: QUERY_TIMEOUT_MS }
    );
}

async function updatePenalizada(conn, envio, penalizada) {
    return await executeQuery(
        conn,
        `
        UPDATE envios
        SET penalizada = ?
        WHERE id = ?
          AND didOwner = ?
          AND didEnvio = ?
          AND superado = 0
          AND elim = 0
        `,
        [penalizada, envio.id, envio.didOwner, envio.didEnvio],
        { timeoutMs: QUERY_TIMEOUT_MS }
    );
}

async function processPenalizadas(options = {}) {
    const lookbackDays = Number(options.lookbackDays || process.env.PENALIZADAS_LOOKBACK_DAYS || 7);
    let conn;

    try {
        conn = await getConnectionLocalEnvios();
        await ensurePenalizadaColumn(conn);

        const envios = await fetchEnvios(conn, lookbackDays);
        let procesados = 0;
        let actualizados = 0;

        for (const envio of envios) {
            const estados = await fetchEstados(conn, envio);
            const countryCode = resolveCountryCodeForOwner(envio.didOwner);
            const penalizada = await calculatePenalizada(envio, estados, countryCode);

            if (penalizada === null) {
                continue;
            }

            procesados += 1;

            if (Number(envio.penalizada) === penalizada) {
                continue;
            }

            const result = await updatePenalizada(conn, envio, penalizada);
            actualizados += Number(result?.changedRows || result?.affectedRows || 0);
        }

        return {
            lookbackDays,
            enviosLeidos: envios.length,
            procesados,
            actualizados,
        };
    } finally {
        try {
            conn?.release?.();
        } catch (_) { }
    }
}

module.exports = {
    processPenalizadas,
    ensurePenalizadaColumn,
    fetchEnvios,
    fetchEstados,
    updatePenalizada,
};
