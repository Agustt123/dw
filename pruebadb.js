const mysql = require("mysql2/promise");

const config = {
  host: process.env.PRUEBADB_HOST || "10.70.0.77",
  port: Number(process.env.PRUEBADB_PORT || 44349),
  user: process.env.PRUEBADB_USER || "root",
  password: process.env.PRUEBADB_PASSWORD || "6vWe2M8NyZy9aE",
  database: process.env.PRUEBADB_DATABASE || "data",
};

async function main() {
  const startedAt = Date.now();
  let conn;

  try {
    console.log("[PRUEBADB] conectando...", {
      host: config.host,
      port: config.port,
      user: config.user,
      database: config.database,
    });

    const connectStartedAt = Date.now();
    conn = await mysql.createConnection(config);
    console.log(`[PRUEBADB] conexion OK elapsedMs=${Date.now() - connectStartedAt}`);

    const pingStartedAt = Date.now();
    await conn.query("SELECT 1 AS ok");
    console.log(`[PRUEBADB] ping OK elapsedMs=${Date.now() - pingStartedAt}`);

    const basicStartedAt = Date.now();
    const [rows] = await conn.query(
      "SELECT NOW() AS nowDb, DATABASE() AS currentDb, @@hostname AS dbHost LIMIT 1"
    );
    console.log(`[PRUEBADB] query basica OK elapsedMs=${Date.now() - basicStartedAt}`, rows[0]);

    const cdcStartedAt = Date.now();
    const [cdcRows] = await conn.query(
      "SELECT COUNT(*) AS total FROM cdc"
    );
    console.log(`[PRUEBADB] count cdc OK elapsedMs=${Date.now() - cdcStartedAt}`, cdcRows[0]);

    console.log(`[PRUEBADB] total elapsedMs=${Date.now() - startedAt}`);
  } catch (error) {
    console.error("[PRUEBADB] error", {
      message: error?.message || String(error),
      code: error?.code,
      errno: error?.errno,
    });
    process.exitCode = 1;
  } finally {
    try {
      if (conn) await conn.end();
    } catch (_) {
    }
  }
}

main();
