


const { executeQuery, getConnectionLocal } = require("../db");




// NULL/''/undefined -> 0 (wildcard)


async function procesarAsignacionesHoy2(connection) {
    const selectCDC = `
    SELECT id, didOwner, didPaquete, didCliente, didChofer
    FROM cdc
    WHERE procesado = 0
      AND disparador = "asignaciones"
      AND ejecutar   = "pendientesHoy"
    ORDER BY id ASC
    LIMIT 10
  `;

    const rows = await executeQuery(connection, selectCDC, []);

    // procesos[owner][cliente][chofer] = true   (0 = ¿)
    const Aprocesos = {};

    for (const row of rows) {
        const Ow = row.didOwner;
        const Cli = row.didCliente || 0;   // 0 cliente si viene vacío
        const Cho = row.didChofer || 0;   // ¿0 chofer si viene vacío
        const envio = row.didPaquete;

        // Existe Owner?
        if (!Aprocesos[Ow]) {
            Aprocesos[Ow] = {};
        }

        // Existe Owner, Cliente?
        if (!Aprocesos[Ow][Cli]) {
            Aprocesos[Ow][Cli] = {};

            Aprocesos[Ow][Cli][0] = true;     // [Ow][Cli][0] = true
        }

        // Chofer != 0 ?  -> marcar chofer actual
        if (Cho !== 0) {
            if (!Aprocesos[Ow][Cli][Cho]) {
                Aprocesos[Ow][Cli][Cho] = true; // [Ow][Cli][Cho] = true
            }

            // ¿Hay chofer anterior para este envío?
            const qChoferAnterior = `
        SELECT operador AS didChofer
        FROM asignaciones
        WHERE didEnvio = ? AND didOwner = ?
        ORDER BY id DESC
       
      `;
            const prev = await executeQuery(connection, qChoferAnterior, [envio, Ow]);
            if (prev.length) {
                const ChoAnt = prev[0].didChofer || 0;
                // Si existe y es distinto del actual, y aún no está marcado, lo marcamos
                if (ChoAnt !== 0 && ChoAnt !== Cho && !Aprocesos[Ow][Cli][ChoAnt]) {
                    Aprocesos[Ow][Cli][Cho] = true; // [Ow][Cli][ChoAnt] = true
                }
            }
        }
        // (no “cierra”; el flujo ya está cubierto)
    }

    console.log("Aprocesos:", JSON.stringify(Aprocesos, null, 2));
    return { Aprocesos };
}

async function procesarAsignacionesHoy(connection) {
    const selectCDC = `
    SELECT id, didOwner, didPaquete, didCliente, didChofer
    FROM cdc
    WHERE procesado = 0
      AND disparador = "asignaciones"
      AND ejecutar   = "pendientesHoy"
    ORDER BY id ASC
    LIMIT 10
  `;

    const rows = await executeQuery(connection, selectCDC, []);

    // procesos[owner][cliente][chofer] = true   (0 = wildcard chofer)
    const Aprocesos = {};

    for (const row of rows) {
        const Ow = row.didOwner;
        const Cli = row.didCliente || 0; // 0 si viene vacío
        const Cho = row.didChofer || 0; // 0 si viene vacío
        const envio = row.didPaquete;

        if (!Ow) continue;

        // Existe Owner?
        if (!Aprocesos[Ow]) {
            Aprocesos[Ow] = {};
        }

        // Existe Owner, Cliente?
        if (!Aprocesos[Ow][Cli]) {
            Aprocesos[Ow][Cli] = {};
            // crea wildcard por cliente
            Aprocesos[Ow][Cli][0] = true; // [Ow][Cli][0] = true
        }

        // Chofer != 0 ?  -> marcar chofer actual
        if (Cho !== 0) {
            // Si NO existe el chofer actual, crearlo y cerrar
            if (!Aprocesos[Ow][Cli][Cho]) {
                Aprocesos[Ow][Cli][Cho] = true; // [Ow][Cli][Cho] = true
                continue; // ✅ cierra sin hacer COUNT (según diagrama)
            }

            // Si YA existe el chofer actual → ¿es la primera asignación?
            const qCount = `
        SELECT COUNT(*) AS cnt
        FROM asignaciones
        WHERE didEnvio = ? AND didOwner = ?
      `;
            const cntRows = await executeQuery(connection, qCount, [envio, Ow]);
            const totalAsign = (cntRows[0]?.cnt) ? Number(cntRows[0].cnt) : 0;

            // Si es la primera (<=1), cierra; si no, también cierra (no hay acción extra)
            continue;
        }

        // Chofer == 0  -> ¿Hay chofer anterior para este envío?
        const qChoferAnterior = `
      SELECT operador AS didChofer
      FROM asignaciones
      WHERE didEnvio = ? AND didOwner = ?
      ORDER BY id DESC
      LIMIT 1
    `;
        const prev = await executeQuery(connection, qChoferAnterior, [envio, Ow]);

        if (!prev.length) {
            // no hay chofer anterior -> cierra
            continue;
        }

        const ChoAnt = prev[0].didChofer || 0;
        if (ChoAnt === 0) {
            // chofer anterior inválido -> cierra
            continue;
        }

        // ¿Existe Owner, Cliente, Chofer Anterior?
        if (!Aprocesos[Ow][Cli][ChoAnt]) {
            Aprocesos[Ow][Cli][ChoAnt] = true; // ✅ FIX: marcar Chofer Anterior (no Cho)
        }
        // cierra
    }

    console.log("Aprocesos:", JSON.stringify(Aprocesos, null, 2));
    return { Aprocesos };
}


async function main() {
    // cambia según necesites 

    try {
        const connection = await getConnectionLocal();
        const res = await procesarAsignacionesHoy(connection);


    } catch (err) {
        console.error("❌ Error batch:", err);
    }
}

// Si querés ciclo continuo:
// setInterval(main, 60 * 1000);
main();






















