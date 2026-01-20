const mysql = require("mysql2/promise");
const redis = require("redis");
const { logYellow, logRed } = require("./fuctions/logsCustom");


// --- Redis ---
const redisClient = redis.createClient({
    socket: {
        host: "192.99.190.137",
        port: 50301,
    },
    password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg",
});

redisClient.on("error", (err) => {
    console.error("Error al conectar con Redis:", err);
});

(async () => {
    await redisClient.connect();
    console.log("✅ Redis conectado");
})();


const superPool = mysql.createPool({
    host: "bhsmysql1.lightdata.com.ar",
    user: "lightdat_susanita", // ✅ Reemplazá esto con tu usuario real
    password: "susanitateniaunraton", // ✅ Reemplazá esto con tu password real
    waitForConnections: true,
    connectionLimit: 2,
    queueLimit: 0,
    multipleStatements: true, // Por si necesitás ejecutar varios queries separados por ";"
});

// --- Variables locales ---
let companiesList = {};

// --- Conexión a la base de datos de producción por empresa (usando pool y `USE`)
async function getConnection(idempresa) {
    let connection;
    try {
        console.log("🔄 idempresa recibido:", idempresa);

        if (typeof idempresa !== "string" && typeof idempresa !== "number") {
            throw new Error(`idempresa debe ser string o number, recibido: ${typeof idempresa}`);
        }

        const empresasData = await getFromRedis("empresasData");
        if (!empresasData) throw new Error("No se encontraron datos en Redis.");

        const empresa = empresasData[String(idempresa)];
        if (!empresa) throw new Error(`No se encontró empresa con ID: ${idempresa}`);
        if (!empresa.dbname) throw new Error(`La empresa ${idempresa} no tiene dbname`);

        connection = await superPool.getConnection();

        // Importante: USE puede fallar por permisos => si falla, liberamos el connection en catch
        await connection.query(`USE \`${empresa.dbname}\``);

        return connection;
    } catch (error) {
        // ✅ liberar SIEMPRE si fue tomada del pool
        try {
            if (connection && typeof connection.release === "function") connection.release();
            else if (connection && typeof connection.end === "function") await connection.end();
            else if (connection && typeof connection.destroy === "function") connection.destroy();
        } catch (_) { }

        const msg = error?.message || String(error);
        console.error("❌ Error al obtener conexión:", msg);

        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: msg,
            },
        };
    }
}


// ===== DW/local con POOLS separados =====
const dwConfigBase = {
    host: "149.56.182.49",
    port: 44349,
    user: "root",
    password: "6vWe2M8NyZy9aE",
};

const dwDbName = "data";

let dwInitPromise = null;

let dwPoolEnvios = null;
let dwPoolCdc = null;
let dwPoolPend = null;

async function initDWPool() {
    // crear DB UNA sola vez
    const c = await mysql.createConnection(dwConfigBase);
    try {
        await c.query(`CREATE DATABASE IF NOT EXISTS \`${dwDbName}\``);
    } finally {
        await c.end().catch(() => { });
    }

    const base = {
        ...dwConfigBase,
        database: dwDbName,
        waitForConnections: true,
        enableKeepAlive: true,
        keepAliveInitialDelay: 0,
    };

    // ✅ Pool para ENVIOS (más capacidad)
    dwPoolEnvios = mysql.createPool({
        ...base,
        connectionLimit: 8,
        queueLimit: 200,
    });

    // ✅ Pool para CDC (capado para que no tumbe todo)
    dwPoolCdc = mysql.createPool({
        ...base,
        connectionLimit: 2,
        queueLimit: 200,
    });

    // ✅ Pool para PENDIENTES (chico)
    dwPoolPend = mysql.createPool({
        ...base,
        connectionLimit: 1,
        queueLimit: 200,
    });
}

async function ensurePools() {
    if (!dwInitPromise) dwInitPromise = initDWPool();
    await dwInitPromise;
}

async function getConnectionLocalEnvios() {
    try {
        await ensurePools();
        return await dwPoolEnvios.getConnection();
    } catch (error) {
        console.error("❌ Error al obtener conexión local (ENVIOS):", error.message);
        throw { status: 500, response: { estado: false, error: -1, message: error.message } };
    }
}

async function getConnectionLocalCdc() {
    try {
        await ensurePools();
        return await dwPoolCdc.getConnection();
    } catch (error) {
        console.error("❌ Error al obtener conexión local (CDC):", error.message);
        throw { status: 500, response: { estado: false, error: -1, message: error.message } };
    }
}

async function getConnectionLocalPendientes() {
    try {
        await ensurePools();
        return await dwPoolPend.getConnection();
    } catch (error) {
        console.error("❌ Error al obtener conexión local (PENDIENTES):", error.message);
        throw { status: 500, response: { estado: false, error: -1, message: error.message } };
    }
}

async function closeDWPool() {
    try { if (dwPoolEnvios) await dwPoolEnvios.end(); } catch { }
    try { if (dwPoolCdc) await dwPoolCdc.end(); } catch { }
    try { if (dwPoolPend) await dwPoolPend.end(); } catch { }
}

// ===== Export =====



// --- Redis helpers ---
async function getFromRedis(key) {
    try {
        const value = await redisClient.get(key);
        return value ? JSON.parse(value) : null;
    } catch (error) {
        console.error(`❌ Error obteniendo ${key} de Redis:`, error);
        throw {
            status: 500,
            response: { estado: false, error: -1 },
        };
    }
}

function getProdDbConfig(company) {
    return {
        host: "bhsmysql1.lightdata.com.ar",
        user: company.dbuser,
        password: company.dbpass,
        database: company.dbname,
    };
}

async function loadCompaniesFromRedis() {
    try {
        const companiesListString = await redisClient.get("empresasData");
        companiesList = JSON.parse(companiesListString);
    } catch (error) {
        logRed(`❌ Error en loadCompaniesFromRedis: ${error.message}`);
        throw error;
    }
}

async function executeQuery(connection, query, values = [], opts = {}) {
    const { log = false, timeoutMs = 20000 } = opts;

    try {
        if (log) logYellow(`Ejecutando: ${query} con valores: ${JSON.stringify(values)}`);

        const [results] = await connection.query({
            sql: query,
            values,
            timeout: timeoutMs, // ✅ timeout real (ms)
        });

        if (log) logYellow(`✅ Resultados: ${JSON.stringify(results)}`);
        return results;
    } catch (error) {
        if (log) logRed(`❌ Error en query: ${error.message}`);

        // marcadores típicos de problemas donde conviene destruir conexión
        error.__shouldDestroyConnection =
            error.code === "PROTOCOL_CONNECTION_LOST" ||
            error.code === "ECONNRESET" ||
            error.code === "ETIMEDOUT" ||
            String(error.message || "").toLowerCase().includes("timeout");

        throw error;
    }
}


async function getCompanyById(companyId) {
    try {
        let company = companiesList[companyId];
        if (!company || Object.keys(companiesList).length === 0) {
            await loadCompaniesFromRedis();
            company = companiesList[companyId];
        }
        return company;
    } catch (error) {
        logRed(`❌ Error en getCompanyById: ${error.stack}`);
        throw error;
    }
}

// --- Exportar todo ---
module.exports = {
    getConnection,
    getFromRedis,
    redisClient,
    getProdDbConfig,
    executeQuery,
    getCompanyById,

    // ✅ nuevos getters por pool
    getConnectionLocalEnvios,
    getConnectionLocalCdc,
    getConnectionLocalPendientes,

    closeDWPool,
};