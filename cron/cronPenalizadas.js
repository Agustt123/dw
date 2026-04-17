const cron = require("node-cron");
const { getConnectionLocalEnvios, executeQuery } = require("../db");

const CRON_EXPR = process.env.CRON_PENALIZADAS_EXPR || "*/10 * * * *";
const LOOKBACK_DAYS = Number(process.env.PENALIZADAS_LOOKBACK_DAYS || 7);
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

function getDateOnly(value) {
    if (!value) return "";

    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return "";

    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
}

function getHour(value) {
    if (!value) return null;

    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return null;

    return date.getHours();
}

function esMismaFecha(fecha1, fecha2) {
    return getDateOnly(fecha1) === getDateOnly(fecha2);
}

async function procesarPenalizadasMismaFecha(options = {}) {
    const lookbackDays = Number(options.lookbackDays || LOOKBACK_DAYS);
    let conn;

    try {
        conn = await getConnectionLocalEnvios();
        await ensurePenalizadaColumn(conn);

        const envios = await executeQuery(
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

        let procesados = 0;
        let actualizados = 0;

        for (const envio of envios) {
            const estados = await executeQuery(
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

            const fechaDespacho = getDateOnly(envio.fecha_despacho);
            let tieneEstado5MismoDia = false;
            let entregadoAntesDe21 = false;
            let nadieEnCasaAntesDe21 = false;
            let penalizada = 1;

            for (const item of estados) {
                if (!esMismaFecha(item.fecha, envio.fecha_despacho)) {
                    continue;
                }

                if (Number(item.estado) === 5 && getHour(item.fecha) < 21) {
                    tieneEstado5MismoDia = true;
                    entregadoAntesDe21 = true;
                    continue;
                }

                if (Number(item.estado) === 5) {
                    tieneEstado5MismoDia = true;
                }

                if (Number(item.estado) === 6 && getHour(item.fecha) < 21) {
                    nadieEnCasaAntesDe21 = true;
                }
            }

            if (!fechaDespacho || !tieneEstado5MismoDia) {
                continue;
            }

            if (entregadoAntesDe21) {
                penalizada = 0;
            } else if (nadieEnCasaAntesDe21) {
                penalizada = 0;
            } else {
                penalizada = 1;
            }

            procesados += 1;

            if (Number(envio.penalizada) === penalizada) {
                continue;
            }

            const result = await executeQuery(
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

function startPenalizadasJob() {
    let running = false;

    cron.schedule(CRON_EXPR, async () => {
        if (running) {
            console.log("[PENALIZADAS] sigue corriendo, salteo ciclo");
            return;
        }

        running = true;

        try {
            const result = await procesarPenalizadasMismaFecha();
            console.log("[PENALIZADAS] ok", result);
        } catch (error) {
            console.error("[PENALIZADAS] error:", error?.message || error);
        } finally {
            running = false;
        }
    });

    console.log(`[PENALIZADAS] scheduler iniciado (${CRON_EXPR})`);
}

if (require.main === module) {
    const runOnce = process.argv.includes("--once");

    if (runOnce) {
        procesarPenalizadasMismaFecha()
            .then((result) => {
                console.log("[PENALIZADAS] corrida manual ok", result);
                process.exit(0);
            })
            .catch((error) => {
                console.error("[PENALIZADAS] corrida manual error:", error?.message || error);
                process.exit(1);
            });
    } else {
        startPenalizadasJob();
    }
}

module.exports = {
    procesarPenalizadasMismaFecha,
    startPenalizadasJob,
};
