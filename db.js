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
const sistemaPool = mysql.createPool({
    host: "bhsmysql1.lightdata.com.ar",
    user: "lightdat_susanita",
    password: "susanitateniaunraton",
    database: "lightdat_sistema",
    waitForConnections: true,
    connectionLimit: 2,
    queueLimit: 0,
    multipleStatements: true,
});
// --- Variables locales ---
let companiesList = {};

// --- Conexión a la base de datos de producción por empresa (usando pool y `USE`)
async function getConnection(idempresa) {
    let connection;
    try {
        //    console.log("🔄 idempresa recibido:", idempresa);

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
        await tryFlushHosts(error);
        //  console.error("❌ Error al obtener conexión:", msg);

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
async function getConnectionIndividual(idempresa) {
    try {
        //     console.log("idempresa recibido:", idempresa);

        // Validación del tipo de idempresa
        if (typeof idempresa !== 'string' && typeof idempresa !== 'number') {
            throw new Error(`idempresa debe ser un string o un número, pero es: ${typeof idempresa}`);
        }

        // Obtener las empresas desde Redis
        const redisKey = 'empresasData';
        const empresasData = await getFromRedis(redisKey);
        if (!empresasData) {
            throw new Error(`No se encontraron datos de empresas en Redis.`);
        }

        // console.log("Datos obtenidos desde Redis:", empresasData);

        // Buscar la empresa por su id
        const empresa = empresasData[String(idempresa)];
        if (!empresa) {
            throw new Error(`No se encontró la configuración de la empresa con ID: ${idempresa}`);
        }

        //    console.log("Configuración de la empresa encontrada:", empresa);

        // Configurar la conexión a la base de datos
        const config = {
            host: 'bhsmysql1.lightdata.com.ar',  // Host fijo
            database: empresa.dbname,           // Base de datos desde Redis
            user: empresa.dbuser,               // Usuario desde Redis
            password: empresa.dbpass,
            multipleStatements: true,         // Contraseña desde Redis
        };
        /*  const config = {
              host: 'localhost',  // Host fijo
              database: "logisticaa",           // Base de datos desde Redis
              user: "logisticaA",               // Usuario desde Redis
              password: "logisticaa",           // Contraseña desde Redis
          };*/

        return mysql.createConnection(config);
    } catch (error) {
        await tryFlushHosts(error);
        console.error(`Error al obtener la conexión:`, error.message);

        // Lanza un error con una respuesta estándar
        throw {
            status: 500,
            response: {
                estado: false,

                error: -1,

            },
        };
    }
}
async function getConnectionSistema() {
    let connection;
    try {
        connection = await sistemaPool.getConnection();
        return connection;
    } catch (error) {
        try {
            if (connection?.release) connection.release();
        } catch (_) { }
        await tryFlushHosts(error);

        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error?.message || String(error),
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
const FLUSH_HOSTS_COOLDOWN_MS = 15000;
let flushHostsEnCurso = null;
let ultimoFlushHostsMs = 0;

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
        connectionLimit: 10,
        queueLimit: 100,
    });

    // ✅ Pool para PENDIENTES (chico)
    dwPoolPend = mysql.createPool({
        ...base,
        connectionLimit: 3,
        queueLimit: 200,
    });
}

async function ensurePools() {
    if (!dwInitPromise) dwInitPromise = initDWPool();
    await dwInitPromise;
}

function isHostBlockedError(error) {
    const code = String(error?.code || "");
    const msg = String(error?.message || "").toLowerCase();

    return (
        code === "ER_HOST_IS_BLOCKED" ||
        msg.includes("host") && msg.includes("blocked") && msg.includes("connection errors") ||
        msg.includes("flush-hosts") ||
        msg.includes("flush hosts")
    );
}

async function ejecutarFlushHosts(config) {
    let conn;
    try {
        conn = await mysql.createConnection({
            host: config.host,
            port: config.port,
            user: config.user,
            password: config.password,
            multipleStatements: true,
        });
        await conn.query("FLUSH HOSTS");
        console.log(`✅ FLUSH HOSTS ejecutado en ${config.host}`);
        return true;
    } catch (error) {
        console.error(`❌ No se pudo ejecutar FLUSH HOSTS en ${config.host}:`, error?.message || error);
        return false;
    } finally {
        try { if (conn) await conn.end(); } catch { }
    }
}

async function tryFlushHosts(error) {
    if (!isHostBlockedError(error)) return false;

    const ahora = Date.now();
    if (flushHostsEnCurso) return flushHostsEnCurso;
    if (ahora - ultimoFlushHostsMs < FLUSH_HOSTS_COOLDOWN_MS) return true;

    flushHostsEnCurso = (async () => {
        ultimoFlushHostsMs = Date.now();

        const targets = [
            {
                host: dwConfigBase.host,
                port: dwConfigBase.port,
                user: dwConfigBase.user,
                password: dwConfigBase.password,
            },
        ];

        let ok = false;
        for (const target of targets) {
            // seguimos aunque uno falle
            ok = (await ejecutarFlushHosts(target)) || ok;
        }
        return ok;
    })();

    try {
        return await flushHostsEnCurso;
    } finally {
        flushHostsEnCurso = null;
    }
}

async function getConnectionLocalEnvios() {
    try {
        await ensurePools();
        return await dwPoolEnvios.getConnection();
    } catch (error) {
        await tryFlushHosts(error);
        // console.error("❌ Error al obtener conexión local (ENVIOS):", error.message);
        throw { status: 500, response: { estado: false, error: -1, message: error.message } };
    }
}

async function getConnectionLocalCdc() {
    try {
        await ensurePools();
        return await dwPoolCdc.getConnection();
    } catch (error) {
        await tryFlushHosts(error);
        //console.error("❌ Error al obtener conexión local (CDC):", error.message);
        throw { status: 500, response: { estado: false, error: -1, message: error.message } };
    }
}

async function getConnectionLocalPendientes() {
    try {
        await ensurePools();
        return await dwPoolPend.getConnection();
    } catch (error) {
        await tryFlushHosts(error);
        // console.error("❌ Error al obtener conexión local (PENDIENTES):", error.message);
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
    const { log = false, timeoutMs = 200000 } = opts;

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

        if (isHostBlockedError(error)) {
            const flushed = await tryFlushHosts(error);

            if (flushed) {
                try {
                    const [retryResults] = await connection.query({
                        sql: query,
                        values,
                        timeout: timeoutMs,
                    });

                    if (log) logYellow(`✅ Resultados luego de FLUSH HOSTS: ${JSON.stringify(retryResults)}`);
                    return retryResults;
                } catch (retryError) {
                    if (log) logRed(`❌ Error reintentando query despues de FLUSH HOSTS: ${retryError.message}`);
                    retryError.__shouldDestroyConnection =
                        retryError.code === "PROTOCOL_CONNECTION_LOST" ||
                        retryError.code === "ECONNRESET" ||
                        retryError.code === "ETIMEDOUT" ||
                        isHostBlockedError(retryError) ||
                        String(retryError.message || "").toLowerCase().includes("timeout");
                    throw retryError;
                }
            }
        }

        // marcadores típicos de problemas donde conviene destruir conexión
        error.__shouldDestroyConnection =
            error.code === "PROTOCOL_CONNECTION_LOST" ||
            error.code === "ECONNRESET" ||
            error.code === "ETIMEDOUT" ||
            isHostBlockedError(error) ||
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
    getConnectionIndividual,
    getConnectionSistema
};
