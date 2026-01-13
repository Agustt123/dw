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


// --- Conexión local al DW (sin pool, se puede mantener)
// --- DW/local con POOL ---
const dwConfigBase = {
    host: "149.56.182.49",
    port: 44349,
    user: "root",
    password: "6vWe2M8NyZy9aE",
};

const dwDbName = "data";

let dwPool = null;
let dwInitPromise = null;

async function initDWPool() {
    // crear DB UNA sola vez
    const c = await mysql.createConnection(dwConfigBase);
    try {
        await c.query(`CREATE DATABASE IF NOT EXISTS \`${dwDbName}\``);
    } finally {
        await c.end().catch(() => { });
    }

    // pool (reutiliza conexiones)
    dwPool = mysql.createPool({
        ...dwConfigBase,
        database: dwDbName,
        waitForConnections: true,
        connectionLimit: 5,  // 👈 ponelo bajo (5-10). Si querés "1 sola", poné 1.
        queueLimit: 50,      // 👈 evita cola infinita
        enableKeepAlive: true,
        keepAliveInitialDelay: 0,
    });
}

async function getConnectionLocal() {
    try {
        if (!dwInitPromise) dwInitPromise = initDWPool();
        await dwInitPromise;
        return await dwPool.getConnection();
    } catch (error) {
        console.error("❌ Error al obtener conexión local:", error.message);
        throw {
            status: 500,
            response: { estado: false, error: -1, message: error.message },
        };
    }
}

// opcional para cerrar limpio
async function closeDWPool() {
    if (dwPool) await dwPool.end().catch(() => { });
}

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

async function executeQuery(connection, query, values = [], log = false) {
    try {
        if (log) logYellow(`Ejecutando: ${query} con valores: ${JSON.stringify(values)}`);
        const [results] = await connection.query(query, values);
        if (log) logYellow(`✅ Resultados: ${JSON.stringify(results)}`);
        return results;
    } catch (error) {
        if (log) logRed(`❌ Error en query: ${error.message}`);
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
    getConnectionLocal,
    getFromRedis,
    redisClient,
    getProdDbConfig,
    executeQuery,
    getCompanyById,
};
