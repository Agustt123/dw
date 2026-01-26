/* backfill_home_app_idx.js
   Pobla home_app_idx desde home_app (CSV legacy).

   Requisitos:
   - Tabla home_app_idx creada (la que te pasé).
   - home_app tiene columna `id` incremental (si no, avisame).
*/

const { executeQuery, getConnectionLocalPendientes } = require("./db");

// ---------- Config ----------
const BATCH_HOMEAPP_ROWS = 1500; // cuántas filas de home_app leemos por vuelta
const CHUNK_INSERT_ROWS = 1000;  // cuántas filas insertamos por query (multi-values)
const LOG_EVERY = 10;            // log cada N páginas
const TZ = "America/Argentina/Buenos_Aires";

// Si querés limitar por fecha/día (para probar), ponelo acá.
// Ej: "2026-01-01" / null para todo
const ONLY_FROM_DIA = null;

// ---------- Helpers ----------
function splitCsv(s) {
    if (!s) return [];
    return s
        .split(",")
        .map(t => t.trim())
        .filter(Boolean);
}

function unique(arr) {
    return [...new Set(arr)];
}

async function flushInsert(conn, rows) {
    // rows: [{owner,cliente,chofer,estado,dia,paquete,en_historial,en_cierre}]
    if (!rows.length) return;

    // Insert multi-values + upsert
    // Clave: no pisar en_cierre=1 con 0. Usamos GREATEST(en_cierre, VALUES(en_cierre)).
    const values = rows.map(() => "(?,?,?,?,?,?,?,?,NOW(),NOW())").join(",");
    const sql = `
    INSERT INTO home_app_idx
      (didOwner, didCliente, didChofer, estado, dia, didPaquete, en_historial, en_cierre, updatedAt, createdAt)
    VALUES ${values}
    ON DUPLICATE KEY UPDATE
      en_historial = 1,
      en_cierre    = GREATEST(en_cierre, VALUES(en_cierre)),
      updatedAt    = NOW()
  `;

    const params = [];
    for (const r of rows) {
        params.push(
            r.owner,
            r.cliente,
            r.chofer,
            r.estado,
            r.dia,
            String(r.paquete),
            r.en_historial ? 1 : 0,
            r.en_cierre ? 1 : 0
        );
    }

    await executeQuery(conn, sql, params, true);
}

async function backfill() {
    const conn = await getConnectionLocalPendientes();

    let lastId = 0;
    let page = 0;

    // buffer de inserts para amortizar
    let buffer = [];
    let totalHomeAppRows = 0;
    let totalIndexRows = 0;

    try {
        while (true) {
            // Traemos filas de home_app por id incremental
            // Importante: dia está guardado como string "YYYY-MM-DD" o DATE? Lo tratamos como string compatible.
            const whereDia = ONLY_FROM_DIA ? "AND dia >= ?" : "";
            const params = ONLY_FROM_DIA ? [lastId, ONLY_FROM_DIA, BATCH_HOMEAPP_ROWS] : [lastId, BATCH_HOMEAPP_ROWS];

            const sel = `
        SELECT id, didOwner, didCliente, didChofer, estado, dia, didsPaquete, didsPaquetes_cierre
        FROM home_app
        WHERE id > ?
        ${whereDia}
        ORDER BY id ASC
        LIMIT ?
      `;

            const rows = await executeQuery(conn, sel, params);
            if (!rows.length) break;

            page += 1;
            totalHomeAppRows += rows.length;
            lastId = rows[rows.length - 1].id;

            for (const r of rows) {
                const owner = Number(r.didOwner) || 0;
                const cliente = Number(r.didCliente) || 0;
                const chofer = Number(r.didChofer) || 0;
                const estado = Number(r.estado);
                const dia = r.dia; // debe venir "YYYY-MM-DD" o Date, MySQL suele devolver string

                if (!owner || !Number.isFinite(estado) || !dia) continue;

                // Primero cierre (en_cierre=1)
                const cierre = unique(splitCsv(r.didsPaquetes_cierre));
                for (const p of cierre) {
                    buffer.push({
                        owner, cliente, chofer, estado, dia,
                        paquete: p,
                        en_historial: 1,
                        en_cierre: 1
                    });
                }

                // Después historial (en_cierre=0, pero no pisa si ya estaba en cierre gracias a GREATEST)
                const hist = unique(splitCsv(r.didsPaquete));
                for (const p of hist) {
                    buffer.push({
                        owner, cliente, chofer, estado, dia,
                        paquete: p,
                        en_historial: 1,
                        en_cierre: 0
                    });
                }

                // Flush por chunks
                while (buffer.length >= CHUNK_INSERT_ROWS) {
                    const slice = buffer.splice(0, CHUNK_INSERT_ROWS);
                    await flushInsert(conn, slice);
                    totalIndexRows += slice.length;
                }
            }

            if (page % LOG_EVERY === 0) {
                console.log(`✅ Backfill page=${page} lastId=${lastId} home_app_rows=${totalHomeAppRows} idx_rows_enqueued=${totalIndexRows + buffer.length}`);
            }
        }

        // Flush final
        if (buffer.length) {
            await flushInsert(conn, buffer);
            totalIndexRows += buffer.length;
            buffer = [];
        }

        console.log("🎉 Backfill terminado");
        console.log(`home_app filas leídas: ${totalHomeAppRows}`);
        console.log(`home_app_idx filas insert/upsert intentadas: ${totalIndexRows}`);

    } catch (e) {
        console.error("❌ Backfill error:", e);
        throw e;
    } finally {
        try { conn?.release?.(); } catch (_) { }
    }
}

// Ejecutar si lo corrés directo
if (require.main === module) {
    backfill()
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
}

module.exports = { backfill };
