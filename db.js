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
    try {
        console.log("🔄 idempresa recibido:", idempresa);

        if (typeof idempresa !== "string" && typeof idempresa !== "number") {
            throw new Error(`idempresa debe ser string o number, recibido: ${typeof idempresa}`);
        }

        const redisKey = "empresasData";
        const empresasData = await getFromRedis(redisKey);

        if (!empresasData) throw new Error("No se encontraron datos en Redis.");
        const empresa = empresasData[String(idempresa)];
        if (!empresa) throw new Error(`No se encontró empresa con ID: ${idempresa}`);

        const connection = await superPool.getConnection();
        await connection.query(`USE \`${empresa.dbname}\``); // Cambia a la base específica

        return connection;
    } catch (error) {
        console.error("❌ Error al obtener conexión:", error.message);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error.message,
            },
        };
    }
}

// --- Conexión local al DW (sin pool, se puede mantener)
async function getConnectionLocal() {
    try {


        const config = {
            host: "149.56.182.49",
            port: 44349,
            user: "root",
            password: "6vWe2M8NyZy9aE",
        };

        const dbName = `data`;
        const connection = await mysql.createConnection(config);
        await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
        await connection.end();
        await new Promise((resolve) => setTimeout(resolve, 500));

        const dbConnection = await mysql.createConnection({ ...config, database: dbName });
        return dbConnection;
    } catch (error) {
        console.error(`❌ Error al obtener conexión local:`, error.message);
        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
                message: error.message,
            },
        };
    }
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
