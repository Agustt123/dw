async function obtenerMetricasProcesslist(connection) {
    const query = `
        SELECT 
            COUNT(*) AS procesos,
            SUM(time) AS total_segundos,
            ROUND(AVG(time), 2) AS promedio_segundos,
            MAX(time) AS max_segundos
        FROM information_schema.processlist
        WHERE command NOT IN ('Daemon', 'Sleep');
    `;

    const results = await executeQuery(connection, query);
    return results[0];
}

module.exports = { obtenerMetricasProcesslist };