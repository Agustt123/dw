const { executeQuery, getConnectionLocal } = require("../db");


const Aprocesos = {};

const idsProcesados = [];
const LIMIT = 50;


// ---------- Builder para disparador = 'estado' ----------
async function buildAprocesosEstado(rows, connection) {
    for (const row of rows) {
        const OW = row.didOwner;
        const CLI = row.didCliente ?? 0;
        if (!OW) continue;

        const queryEstado = "SELECT id FROM estado WHERE didEnvio = ? AND didOwner = ? AND id < ? ORDER BY id DESC LIMIT 1";
        const estadoPrev = await executeQuery(connection, queryEstado, [row.didPaquete, OW, row.id]);
        if (estadoPrev.length > 0) {
            continue;
        }

        if (!Aprocesos[OW]) {
            Aprocesos[OW] = {};
        }
        if (!Aprocesos[OW][0]) Aprocesos[OW][0] = {};
        if (!Aprocesos[OW][0][0]) Aprocesos[OW][0][0] = { 1: [], 0: [] };
        Aprocesos[OW][0][0][1].push(row.didPaquete);
        if (!Aprocesos[OW][CLI]) Aprocesos[OW][CLI] = {};
        if (!Aprocesos[OW][CLI][0]) Aprocesos[OW][CLI][0] = { 1: [], 0: [] };
        Aprocesos[OW][CLI][0][1].push(row.didPaquete);

        idsProcesados.push(row.id);
    }
    return Aprocesos;
}


// ---------- Builder para disparador = 'asignaciones' ----------
async function buildAprocesosAsignaciones(conn, rows) {
    for (const row of rows) {
        const Ow = row.didOwner;
        const Cli = row.didCliente || 0;
        const Cho = row.didChofer || 0;          // Cho != 0 => asignación; Cho == 0 => desasignación
        const envio = String(row.didPaquete);
        if (!Ow) continue;

        if (!Aprocesos[Ow]) Aprocesos[Ow] = {};
        if (!Aprocesos[Ow][0]) Aprocesos[Ow][0] = {};
        if (!Aprocesos[Ow][0][0]) Aprocesos[Ow][0][0] = { 1: [], 0: [] }; // owner agregado (solo +)
        if (!Aprocesos[Ow][Cli]) Aprocesos[Ow][Cli] = {};
        if (!Aprocesos[Ow][Cli][0]) Aprocesos[Ow][Cli][0] = { 1: [], 0: [] };
        if (!Aprocesos[Ow][Cli][Cho]) Aprocesos[Ow][Cli][Cho] = { 1: [], 0: [] };

        if (Cho !== 0) {
            Aprocesos[Ow][Cli][Cho][1].push(envio);            // chofer actual (+)
        }
        // DESASIGNACIÓN → SOLO - en (Cli,choferAnterior real). NO tocar agregados.
        const qChoferAnterior = `
        SELECT operador AS didChofer
        FROM asignaciones
        WHERE didEnvio = ? AND didOwner = ? AND operador IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
      `;
        const prev = await executeQuery(conn, qChoferAnterior, [envio, Ow]);
        if (prev.length) {
            const choPrev = prev[0].didChofer || 0;
            if (choPrev !== 0) {
                if (!Aprocesos[Ow][Cli][choPrev]) Aprocesos[Ow][Cli][choPrev] = { 1: [], 0: [] };
                Aprocesos[Ow][Cli][choPrev][0].push(envio);     // negativo SOLO en chofer anterior
            }
        }

        idsProcesados.push(row.id);
    }
    return Aprocesos;
}




async function aplicarAprocesosAHommeApp(conn) {

    // AGARRO LAS POSIBLES COMBINACIONES DE OWNER/CLIENTE/CHOFER
    for (const owner in Aprocesos) {
        const porCliente = Aprocesos[owner];
        console.log("➡️ Owner:", owner);

        for (const cliente in porCliente) {
            const porChofer = porCliente[cliente];
            console.log("  ↪️ Cliente:", cliente);

            for (const chofer in porChofer) {
                const nodo = porChofer[chofer];
                const pos = [...new Set(nodo[1])]; // positivos
                const neg = [...new Set(nodo[0])]; // negativos

                if (pos.length === 0 && neg.length === 0) continue;

                // leer estado actual de hoy
                const sel = `
          SELECT didsPaquete, pendientes
          FROM home_app
          WHERE didOwner = ? AND didCliente = ? AND didChofer = ? AND fecha = CURDATE()
          LIMIT 1
        `;
                const actual = await executeQuery(conn, sel, [owner, cliente, chofer]);

                // parsear paquetes actuales a Set
                let paquetes = new Set();
                let pendientes = 0;
                if (actual.length > 0) {
                    const s = actual[0].didsPaquete || "";
                    if (s) {
                        for (const p of s.split(",")) {
                            const t = p.trim();
                            if (t) paquetes.add(t);
                        }
                    }
                    pendientes = actual[0].pendientes || 0;
                }

                // aplicar positivos
                for (const p of pos) {
                    const k = String(p);
                    if (!paquetes.has(k)) {
                        paquetes.add(k);
                        pendientes += 1;
                    }
                }

                // aplicar negativos
                for (const p of neg) {
                    const k = String(p);
                    if (paquetes.has(k)) {
                        paquetes.delete(k);
                        pendientes = Math.max(0, pendientes - 1);
                    }
                }

                const didsPaqueteStr = Array.from(paquetes).join(",");

                if (actual.length > 0) {
                    const upd = `
            UPDATE home_app
            SET didsPaquete = ?, pendientes = ?, autofecha = NOW()
            WHERE didOwner = ? AND didCliente = ? AND didChofer = ? AND fecha = CURDATE()
          `;
                    await executeQuery(conn, upd, [didsPaqueteStr, pendientes, owner, cliente, chofer]);
                } else {
                    const ins = `
            INSERT INTO home_app
              (didOwner, didCliente, didChofer, didsPaquete, fecha, pendientes)
            VALUES
              (?, ?, ?, ?, CURDATE(), ? )
          `;
                    await executeQuery(conn, ins, [owner, cliente, chofer, didsPaqueteStr, pendientes]);
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
            console.log("✅ CDC marcado como procesado para", slice.length, "rows");
        }
    }
}



async function pendientesHoy() {
    try {
        const conn = await getConnectionLocal();
        const LIMIT = 50;   // máximo a procesar
        const FETCH = 100;  // cuánto traigo de cdc por batch

        // Un solo SELECT, luego despachamos por disparador
        const selectCDC = `
      SELECT id, didOwner, didPaquete, didCliente, didChofer, disparador, ejecutar
      FROM cdc
      WHERE procesado = 0
        AND ejecutar   = "pendientesHoy"
        AND didCliente IS NOT NULL
      ORDER BY id ASC
      LIMIT ?
    `

        const rows = await executeQuery(conn, selectCDC, [FETCH]);

        const rowsEstado = rows.filter(r => r.disparador === "estado");
        const rowsAsignaciones = rows.filter(r => r.disparador === "asignaciones");

        // Procesar ESTADO
        await buildAprocesosEstado(rowsEstado, conn);

        // Procesar ASIGNACIONES
        await buildAprocesosAsignaciones(conn, rowsAsignaciones);
        console.log("[asignaciones] Aprocesos:", JSON.stringify(Aprocesos, null, 2));

        console.log("idsProcesados:", idsProcesados);

        await aplicarAprocesosAHommeApp(conn);

    } catch (err) {
        console.error("❌ Error batch:", err);
    }
}


module.exports = {
    pendientesHoy
};
