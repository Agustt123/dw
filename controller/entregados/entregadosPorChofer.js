const { executeQuery, getConnection } = require("../../db");

const ESTADOS_ENTREGADO = [5, 9];
const QUERY_TIMEOUT_MS = 600000;
const CLIENTES_CACHE_TTL_MS = 60 * 60 * 1000;
const clientesCache = new Map();

function parseIds(value, fieldName) {
    const ids = (Array.isArray(value) ? value : [value])
        .flatMap((item) => String(item ?? "").split(","))
        .map((item) => Number(String(item).trim()))
        .filter((item) => Number.isInteger(item) && item >= 0);

    const uniqueIds = [...new Set(ids)];
    if (!uniqueIds.length) {
        throw new Error(`${fieldName} invalido`);
    }

    return uniqueIds;
}

function validarFecha(fecha) {
    const value = String(fecha ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        throw new Error("fecha invalida");
    }
    return value;
}

function splitCsvToSet(value) {
    const set = new Set();
    if (!value || !String(value).trim()) return set;

    for (const item of String(value).split(",")) {
        const id = item.trim();
        if (id) set.add(id);
    }

    return set;
}

function addCsv(target, value) {
    for (const id of splitCsvToSet(value)) {
        target.add(id);
    }
}

function formatMysqlDate(value) {
    if (!value) return null;
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
        const pad = (n) => String(n).padStart(2, "0");
        return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())} ${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}`;
    }
    return String(value);
}

function addEntregaContext(target, row, sourceSet) {
    const estado = Number(row.estado);
    const didChofer = Number(row.didChofer) || 0;
    const didCliente = Number(row.didCliente) || 0;

    for (const didPaquete of sourceSet) {
        if (!target.has(didPaquete)) {
            target.set(didPaquete, {
                didPaquete,
                didCliente,
                didChofer,
                estados: new Set(),
            });
        }

        const current = target.get(didPaquete);
        current.estados.add(estado);

        if (!current.didCliente && didCliente) current.didCliente = didCliente;
        if (!current.didChofer && didChofer) current.didChofer = didChofer;
    }
}

function getEmpresaClienteCache(didEmpresa) {
    const key = String(didEmpresa);
    const now = Date.now();
    let cache = clientesCache.get(key);

    if (!cache || cache.expiresAt <= now) {
        cache = {
            clientes: new Map(),
            expiresAt: now + CLIENTES_CACHE_TTL_MS,
        };
        clientesCache.set(key, cache);
    }

    return cache;
}

function resetClientesCache(didEmpresa = null) {
    if (didEmpresa == null || didEmpresa === "") {
        clientesCache.clear();
        return { reset: "all" };
    }

    clientesCache.delete(String(didEmpresa));
    return { reset: String(didEmpresa) };
}

async function cargarNombresClientes(didEmpresa, didsCliente) {
    const ids = [...new Set(didsCliente.map(Number).filter((id) => Number.isInteger(id) && id > 0))];
    if (!ids.length) return new Map();

    const cache = getEmpresaClienteCache(didEmpresa);
    const faltantes = ids.filter((id) => !cache.clientes.has(String(id)));

    if (faltantes.length) {
        let connProd;
        try {
            connProd = await getConnection(didEmpresa);
            const sql = `
                SELECT did, nombre_fantasia
                FROM clientes
                WHERE did IN (${faltantes.map(() => "?").join(",")})
                  AND superado = 0
                  AND elim = 0
            `;
            const rows = await executeQuery(connProd, sql, faltantes, { timeoutMs: QUERY_TIMEOUT_MS });
            const encontrados = new Set();

            for (const row of rows) {
                const didCliente = String(row.did);
                encontrados.add(didCliente);
                cache.clientes.set(didCliente, row.nombre_fantasia || "");
            }

            for (const id of faltantes) {
                const key = String(id);
                if (!encontrados.has(key)) cache.clientes.set(key, "");
            }
        } finally {
            if (connProd?.release) {
                try { connProd.release(); } catch (_) { }
            } else if (connProd?.end) {
                try { await connProd.end(); } catch (_) { }
            }
        }
    }

    const result = new Map();
    for (const id of ids) {
        result.set(String(id), cache.clientes.get(String(id)) || null);
    }

    return result;
}

async function getEnviosColumns(conn) {
    const rows = await executeQuery(conn, "SHOW COLUMNS FROM envios", [], { timeoutMs: QUERY_TIMEOUT_MS });
    return new Set(rows.map((row) => row.Field));
}

function pickColumn(columns, candidates) {
    for (const column of candidates) {
        if (columns.has(column)) return column;
    }
    return null;
}

async function loadEnviosInfo(conn, didEmpresa, didsPaquete) {
    if (!didsPaquete.length) return new Map();

    const columns = await getEnviosColumns(conn);
    const mlShipmentColumn = pickColumn(columns, ["ml_shipment_id"]);
    const flexColumn = pickColumn(columns, ["flex"]);
    const didClienteColumn = pickColumn(columns, ["didCliente"]);

    const selected = ["didEnvio"];
    if (didClienteColumn) selected.push(`${didClienteColumn} AS didCliente`);
    if (mlShipmentColumn) selected.push(`${mlShipmentColumn} AS ml_shipment_id`);
    if (flexColumn) selected.push(`${flexColumn} AS flex`);

    const sql = `
        SELECT ${selected.join(", ")}
        FROM envios
        WHERE didOwner = ?
          AND didEnvio IN (${didsPaquete.map(() => "?").join(",")})
          AND COALESCE(elim, 0) = 0
        ORDER BY superado ASC, id DESC
    `;

    const rows = await executeQuery(conn, sql, [didEmpresa, ...didsPaquete], { timeoutMs: QUERY_TIMEOUT_MS });
    const map = new Map();

    for (const row of rows) {
        const didPaquete = String(row.didEnvio);
        if (map.has(didPaquete)) continue;
        map.set(didPaquete, row);
    }

    return map;
}

async function loadEstadosEntrega(conn, didEmpresa, fecha, didsPaquete) {
    if (!didsPaquete.length) return new Map();

    const sql = `
        SELECT didEnvio, estado, fecha, autofecha
        FROM estado
        WHERE didOwner = ?
          AND didEnvio IN (${didsPaquete.map(() => "?").join(",")})
          AND estado IN (${ESTADOS_ENTREGADO.map(() => "?").join(",")})
          AND DATE(COALESCE(fecha, autofecha)) = ?
        ORDER BY COALESCE(fecha, autofecha) DESC, id DESC
    `;

    const rows = await executeQuery(conn, sql, [
        didEmpresa,
        ...didsPaquete,
        ...ESTADOS_ENTREGADO,
        fecha,
    ], { timeoutMs: QUERY_TIMEOUT_MS });

    const map = new Map();
    for (const row of rows) {
        const didPaquete = String(row.didEnvio);
        if (map.has(didPaquete)) continue;
        map.set(didPaquete, {
            estado: Number(row.estado),
            fecha: formatMysqlDate(row.fecha || row.autofecha),
        });
    }

    return map;
}

function makeBucket(didChofer) {
    return {
        didChofer,
        cierre: new Set(),
        historial: new Set(),
        porEstado: {},
    };
}

function getEstadoBucket(bucket, estado) {
    const key = String(estado);
    if (!bucket.porEstado[key]) {
        bucket.porEstado[key] = {
            estado,
            cierre: new Set(),
            historial: new Set(),
        };
    }
    return bucket.porEstado[key];
}

async function entregadosPorChofer(params, conn) {
    const didEmpresa = Number(params.didEmpresa);
    if (!Number.isInteger(didEmpresa) || didEmpresa <= 0) {
        throw new Error("didEmpresa invalido");
    }

    const fecha = validarFecha(params.fecha);
    const didsChofer = parseIds(params.didChofer, "didChofer");
    const didsCliente = params.didCliente == null || params.didCliente === ""
        ? null
        : parseIds(params.didCliente, "didCliente");

    const filtros = [
        "didOwner = ?",
        "dia = ?",
        `estado IN (${ESTADOS_ENTREGADO.map(() => "?").join(",")})`,
        `didChofer IN (${didsChofer.map(() => "?").join(",")})`,
    ];

    const values = [didEmpresa, fecha, ...ESTADOS_ENTREGADO, ...didsChofer];

    if (didsCliente?.length) {
        filtros.push(`didCliente IN (${didsCliente.map(() => "?").join(",")})`);
        values.push(...didsCliente);
    } else {
        filtros.push("didCliente = 0");
    }

    const sql = `
        SELECT didChofer, didCliente, estado, didsPaquete, didsPaquetes_cierre
        FROM home_app
        WHERE ${filtros.join(" AND ")}
    `;

    const rows = await executeQuery(conn, sql, values, { timeoutMs: QUERY_TIMEOUT_MS });

    const totalCierre = new Set();
    const totalHistorial = new Set();
    const porChoferMap = new Map();
    const entregasContext = new Map();

    for (const row of rows) {
        const didChofer = Number(row.didChofer) || 0;
        if (!porChoferMap.has(didChofer)) {
            porChoferMap.set(didChofer, makeBucket(didChofer));
        }

        const bucket = porChoferMap.get(didChofer);
        const estadoBucket = getEstadoBucket(bucket, Number(row.estado));

        const cierreSet = splitCsvToSet(row.didsPaquetes_cierre);
        const historialSet = splitCsvToSet(row.didsPaquete);

        for (const id of cierreSet) {
            bucket.cierre.add(id);
            estadoBucket.cierre.add(id);
            totalCierre.add(id);
        }

        for (const id of historialSet) {
            bucket.historial.add(id);
            estadoBucket.historial.add(id);
            totalHistorial.add(id);
        }

        addEntregaContext(entregasContext, row, cierreSet);
    }

    const didsPaquete = Array.from(totalCierre);
    const enviosInfo = await loadEnviosInfo(conn, didEmpresa, didsPaquete);
    const estadosEntrega = await loadEstadosEntrega(conn, didEmpresa, fecha, didsPaquete);
    const didsClienteDetalle = [];

    for (const didPaquete of didsPaquete) {
        const context = entregasContext.get(didPaquete);
        const envioInfo = enviosInfo.get(didPaquete) || {};
        const didCliente = Number(envioInfo.didCliente ?? context?.didCliente ?? 0);
        if (didCliente > 0) didsClienteDetalle.push(didCliente);
    }

    const nombresCliente = await cargarNombresClientes(didEmpresa, didsClienteDetalle);
    const entregas = {};

    for (const didPaquete of didsPaquete) {
        const context = entregasContext.get(didPaquete) || {
            didPaquete,
            didCliente: 0,
            didChofer: 0,
            estados: new Set(),
        };
        const envioInfo = enviosInfo.get(didPaquete) || {};
        const estadoInfo = estadosEntrega.get(didPaquete) || {};

        entregas[didPaquete] = {
            estado: estadoInfo.estado ?? Array.from(context.estados)[0] ?? null,
            fecha: estadoInfo.fecha ?? null,
            didCliente: Number(envioInfo.didCliente ?? context.didCliente ?? 0),
            nombreCliente: nombresCliente.get(String(Number(envioInfo.didCliente ?? context.didCliente ?? 0))) || null,
            didChofer: Number(context.didChofer ?? 0),
            ml_shipment_id: envioInfo.ml_shipment_id ?? null,
            flex: envioInfo.flex ?? null,
        };
    }

    const porChofer = didsChofer.map((didChofer) => {
        const bucket = porChoferMap.get(didChofer) || makeBucket(didChofer);
        const porEstado = ESTADOS_ENTREGADO.map((estado) => {
            const estadoBucket = bucket.porEstado[String(estado)] || {
                estado,
                cierre: new Set(),
                historial: new Set(),
            };

            return {
                estado,
                cantidad: estadoBucket.cierre.size,
                cantidadHistorial: estadoBucket.historial.size,
            };
        });

        return {
            didChofer,
            cantidad: bucket.cierre.size,
            cantidadHistorial: bucket.historial.size,
            porEstado,
        };
    });

    return {
        estado: true,
        didEmpresa,
        fecha,
        didChofer: didsChofer,
        didCliente: didsCliente,
        estados: ESTADOS_ENTREGADO,
        criterioCliente: didsCliente?.length ? "cliente" : "global_chofer",
        cantidad: totalCierre.size,
        cantidadHistorial: totalHistorial.size,
        entregas,
        porChofer,
    };
}

module.exports = { entregadosPorChofer, resetClientesCache };
