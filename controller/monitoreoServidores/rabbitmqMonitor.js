const axios = require("axios");
const { getConnectionLocalCdc, executeQuery } = require("../../db");

const MONITOREO_TIMEOUT_MS = Number(process.env.RABBITMQ_MONITOR_TIMEOUT_MS || 15000);
const RABBITMQ_HOST = process.env.RABBITMQ_MONITOR_HOST || "158.69.131.226";
const RABBITMQ_PROTOCOL = process.env.RABBITMQ_MONITOR_PROTOCOL || "http";
const RABBITMQ_PORT = Number(process.env.RABBITMQ_MONITOR_PORT || 15672);
const RABBITMQ_USER = process.env.RABBITMQ_MONITOR_USER || "lightdata";
const RABBITMQ_PASSWORD = process.env.RABBITMQ_MONITOR_PASSWORD || "QQyfVBKRbw6fBb";
const RABBITMQ_VHOST = process.env.RABBITMQ_MONITOR_VHOST || "/";
const RABBITMQ_MAX_QUEUES_IN_SUMMARY = Number(process.env.RABBITMQ_MONITOR_TOP_QUEUES || 5);

function buildRabbitBaseUrl() {
    return `${RABBITMQ_PROTOCOL}://${RABBITMQ_HOST}:${RABBITMQ_PORT}/api`;
}

function rabbitAuth() {
    return {
        username: RABBITMQ_USER,
        password: RABBITMQ_PASSWORD,
    };
}

function toNum(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function toInt(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function safeJson(value) {
    if (value === null || value === undefined) return null;
    return JSON.stringify(value);
}

function queueActivityState(queue) {
    const consumers = toInt(queue?.consumers, 0);
    const ready = toInt(queue?.messages_ready, 0);
    const unacked = toInt(queue?.messages_unacknowledged, 0);
    const total = toInt(queue?.messages, 0);

    if (consumers === 0 && total > 0) return "sin_consumidor";
    if (unacked > 0 && consumers > 0) return "procesando";
    if (ready > 0 && consumers > 0) return "con_backlog";
    if (total > 0) return "activa";
    return "idle";
}

function queueSeverity(queue) {
    const consumers = toInt(queue?.consumers, 0);
    const ready = toInt(queue?.messages_ready, 0);
    const unacked = toInt(queue?.messages_unacknowledged, 0);

    if (consumers === 0 && ready > 0) return "rojo";
    if (ready >= 1000 || unacked >= 1000) return "rojo";
    if (ready >= 200 || unacked >= 200) return "naranja";
    if (ready > 0 || unacked > 0) return "amarillo";
    return "verde";
}

function worstSeverity(items) {
    const rank = { verde: 0, amarillo: 1, naranja: 2, rojo: 3 };
    return (items || []).reduce((acc, item) => (rank[item] > rank[acc] ? item : acc), "verde");
}

function normalizeOverview(data = {}) {
    const objectTotals = data?.object_totals || {};
    const queueTotals = data?.queue_totals || {};
    const messageStats = data?.message_stats || {};

    return {
        managementVersion: data?.management_version || null,
        rabbitmqVersion: data?.rabbitmq_version || null,
        clusterName: data?.cluster_name || null,
        erlangVersion: data?.erlang_version || null,
        connections: toInt(objectTotals?.connections, 0),
        channels: toInt(objectTotals?.channels, 0),
        consumers: toInt(objectTotals?.consumers, 0),
        exchanges: toInt(objectTotals?.exchanges, 0),
        queues: toInt(objectTotals?.queues, 0),
        messages: toInt(queueTotals?.messages, 0),
        messagesReady: toInt(queueTotals?.messages_ready, 0),
        messagesUnacked: toInt(queueTotals?.messages_unacknowledged, 0),
        publishRate: toNum(messageStats?.publish_details?.rate) ?? 0,
        deliverRate: toNum(messageStats?.deliver_get_details?.rate) ?? 0,
        ackRate: toNum(messageStats?.ack_details?.rate) ?? 0,
    };
}

function normalizeQueue(queue = {}) {
    const messageStats = queue?.message_stats || {};
    const backing = queue?.backing_queue_status || {};

    return {
        queueName: String(queue?.name || ""),
        vhost: String(queue?.vhost || "/"),
        state: queue?.state || null,
        durable: toInt(queue?.durable, 0),
        autoDelete: toInt(queue?.auto_delete, 0),
        exclusive: toInt(queue?.exclusive, 0),
        consumers: toInt(queue?.consumers, 0),
        consumerUtilisation: toNum(queue?.consumer_utilisation),
        messages: toInt(queue?.messages, 0),
        messagesReady: toInt(queue?.messages_ready, 0),
        messagesUnacked: toInt(queue?.messages_unacknowledged, 0),
        memoryBytes: toInt(queue?.memory, 0),
        idleSince: queue?.idle_since || null,
        ingressRate: toNum(messageStats?.publish_details?.rate) ?? 0,
        deliverRate: toNum(messageStats?.deliver_get_details?.rate) ?? 0,
        ackRate: toNum(messageStats?.ack_details?.rate) ?? 0,
        redeliverRate: toNum(messageStats?.redeliver_details?.rate) ?? 0,
        diskReadsRate: toNum(backing?.avg_ingress_rate) ?? 0,
        diskWritesRate: toNum(backing?.avg_egress_rate) ?? 0,
        activityState: queueActivityState(queue),
        sev: queueSeverity(queue),
    };
}

async function fetchRabbitOverview() {
    const { data } = await axios.get(`${buildRabbitBaseUrl()}/overview`, {
        timeout: MONITOREO_TIMEOUT_MS,
        auth: rabbitAuth(),
    });
    return normalizeOverview(data);
}

async function fetchRabbitQueues() {
    const encodedVhost = encodeURIComponent(RABBITMQ_VHOST);
    const { data } = await axios.get(`${buildRabbitBaseUrl()}/queues/${encodedVhost}`, {
        timeout: MONITOREO_TIMEOUT_MS,
        auth: rabbitAuth(),
        params: {
            page: 1,
            page_size: 500,
            use_regex: false,
            pagination: false,
        },
    });

    const rows = Array.isArray(data?.items) ? data.items : Array.isArray(data) ? data : [];
    return rows.map(normalizeQueue);
}

async function getNextDid(db) {
    const rows = await executeQuery(
        db,
        "SELECT IFNULL(MAX(did), 0) + 1 AS did FROM sat_rabbitmq_overview",
        [],
        { timeoutMs: MONITOREO_TIMEOUT_MS }
    );
    return Number(rows?.[0]?.did || 0) || 1;
}

async function insertOverview(db, did, overview) {
    await executeQuery(
        db,
        `
        INSERT INTO sat_rabbitmq_overview
        (did, management_version, rabbitmq_version, cluster_name, erlang_version,
         connections, channels, consumers, exchanges, queues,
         messages, messages_ready, messages_unacked,
         publish_rate, deliver_rate, ack_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
            did,
            overview.managementVersion,
            overview.rabbitmqVersion,
            overview.clusterName,
            overview.erlangVersion,
            overview.connections,
            overview.channels,
            overview.consumers,
            overview.exchanges,
            overview.queues,
            overview.messages,
            overview.messagesReady,
            overview.messagesUnacked,
            overview.publishRate,
            overview.deliverRate,
            overview.ackRate,
        ],
        { timeoutMs: MONITOREO_TIMEOUT_MS }
    );
}

async function insertQueues(db, did, queues) {
    for (const queue of queues) {
        await executeQuery(
            db,
            `
            INSERT INTO sat_rabbitmq_queues
            (did, queue_name, vhost, state, durable, auto_delete, exclusive_queue,
             consumers, consumer_utilisation,
             messages, messages_ready, messages_unacked,
             memory_bytes, idle_since,
             ingress_rate, deliver_rate, ack_rate, redeliver_rate,
             disk_reads_rate, disk_writes_rate,
             activity_state, sev)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
                did,
                queue.queueName,
                queue.vhost,
                queue.state,
                queue.durable,
                queue.autoDelete,
                queue.exclusive,
                queue.consumers,
                queue.consumerUtilisation,
                queue.messages,
                queue.messagesReady,
                queue.messagesUnacked,
                queue.memoryBytes,
                queue.idleSince,
                queue.ingressRate,
                queue.deliverRate,
                queue.ackRate,
                queue.redeliverRate,
                queue.diskReadsRate,
                queue.diskWritesRate,
                queue.activityState,
                queue.sev,
            ],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );
    }
}

async function computeLastHourActivity(db, queueNames) {
    if (!queueNames.length) return new Map();

    const rows = await executeQuery(
        db,
        `
        SELECT
            queue_name,
            MAX(CASE WHEN COALESCE(ingress_rate, 0) > 0 OR COALESCE(deliver_rate, 0) > 0 OR COALESCE(ack_rate, 0) > 0 THEN 1 ELSE 0 END) AS had_activity,
            MAX(autofecha) AS last_seen,
            MAX(CASE WHEN consumers > 0 THEN 1 ELSE 0 END) AS had_consumers,
            MAX(messages_ready) AS max_ready,
            MAX(messages_unacked) AS max_unacked
        FROM sat_rabbitmq_queues
        WHERE autofecha >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
          AND queue_name IN (${queueNames.map(() => "?").join(",")})
        GROUP BY queue_name
        `,
        queueNames,
        { timeoutMs: MONITOREO_TIMEOUT_MS }
    );

    const map = new Map();
    for (const row of rows) {
        map.set(String(row.queue_name), {
            hadActivity: Number(row.had_activity || 0) === 1,
            lastSeen: row.last_seen || null,
            hadConsumers: Number(row.had_consumers || 0) === 1,
            maxReady: toInt(row.max_ready, 0),
            maxUnacked: toInt(row.max_unacked, 0),
        });
    }
    return map;
}

function buildSummary({ overview, queues, activityMap }) {
    const enriched = queues.map((queue) => {
        const activity = activityMap.get(queue.queueName) || null;
        return {
            ...queue,
            activeLastHour: Boolean(activity?.hadActivity) || (queue.ingressRate > 0 || queue.deliverRate > 0 || queue.ackRate > 0),
            lastSeenLastHour: activity?.lastSeen || null,
            maxReadyLastHour: activity?.maxReady ?? queue.messagesReady,
            maxUnackedLastHour: activity?.maxUnacked ?? queue.messagesUnacked,
        };
    });

    const sinConsumidor = enriched.filter((q) => q.consumers === 0);
    const conBacklog = enriched.filter((q) => q.messagesReady > 0 || q.messagesUnacked > 0);
    const activasUltimaHora = enriched.filter((q) => q.activeLastHour);
    const trabadas = enriched.filter((q) => q.messagesUnacked > 0 && q.consumers > 0);
    const topBacklog = [...enriched]
        .sort((a, b) => (b.messagesReady + b.messagesUnacked) - (a.messagesReady + a.messagesUnacked) || a.queueName.localeCompare(b.queueName))
        .slice(0, Math.max(1, RABBITMQ_MAX_QUEUES_IN_SUMMARY))
        .map((q) => ({
            queue: q.queueName,
            messages: q.messages,
            ready: q.messagesReady,
            unacked: q.messagesUnacked,
            consumers: q.consumers,
            sev: q.sev,
            state: q.activityState,
        }));

    const sev = worstSeverity(enriched.map((q) => q.sev));
    const resumen = `colas=${enriched.length} activas_1h=${activasUltimaHora.length} backlog=${conBacklog.length} sin_consumidor=${sinConsumidor.length} conexiones=${overview.connections}`;

    return {
        sev,
        resumen,
        queuesTotal: enriched.length,
        queuesActiveLastHour: activasUltimaHora.length,
        queuesWithoutConsumer: sinConsumidor.length,
        queuesWithBacklog: conBacklog.length,
        queuesStuck: trabadas.length,
        connections: overview.connections,
        channels: overview.channels,
        consumers: overview.consumers,
        messagesReadyTotal: overview.messagesReady,
        messagesUnackedTotal: overview.messagesUnacked,
        detail: {
            topBacklog,
            withoutConsumer: sinConsumidor.slice(0, 20).map((q) => q.queueName),
            stuck: trabadas.slice(0, 20).map((q) => q.queueName),
        },
    };
}

async function insertSummary(db, did, summary) {
    await executeQuery(
        db,
        `
        INSERT INTO sat_rabbitmq_resumen
        (did, sev, resumen,
         queues_total, queues_active_last_hour, queues_without_consumer,
         queues_with_backlog, queues_stuck,
         connections, channels, consumers,
         messages_ready_total, messages_unacked_total,
         detalle_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
            did,
            summary.sev,
            summary.resumen,
            summary.queuesTotal,
            summary.queuesActiveLastHour,
            summary.queuesWithoutConsumer,
            summary.queuesWithBacklog,
            summary.queuesStuck,
            summary.connections,
            summary.channels,
            summary.consumers,
            summary.messagesReadyTotal,
            summary.messagesUnackedTotal,
            safeJson(summary.detail),
        ],
        { timeoutMs: MONITOREO_TIMEOUT_MS }
    );
}

async function collectRabbitSnapshot() {
    let db;

    try {
        db = await getConnectionLocalCdc();
        const [overview, queues] = await Promise.all([
            fetchRabbitOverview(),
            fetchRabbitQueues(),
        ]);

        const did = await getNextDid(db);
        await insertOverview(db, did, overview);
        await insertQueues(db, did, queues);

        const activityMap = await computeLastHourActivity(db, queues.map((q) => q.queueName));
        const summary = buildSummary({ overview, queues, activityMap });
        await insertSummary(db, did, summary);

        return {
            did,
            overview,
            queuesCount: queues.length,
            summary,
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

async function getLatestRabbitOverview() {
    let db;
    try {
        db = await getConnectionLocalCdc();
        const rows = await executeQuery(
            db,
            `
            SELECT *
            FROM sat_rabbitmq_overview
            ORDER BY id DESC
            LIMIT 1
            `,
            [],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );
        return rows?.[0] || null;
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

async function getLatestRabbitQueues(limit = 100) {
    let db;
    try {
        db = await getConnectionLocalCdc();
        const didRows = await executeQuery(
            db,
            "SELECT IFNULL(MAX(did), 0) AS did FROM sat_rabbitmq_queues",
            [],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );
        const did = Number(didRows?.[0]?.did || 0);
        if (!did) return [];

        return await executeQuery(
            db,
            `
            SELECT *
            FROM sat_rabbitmq_queues
            WHERE did = ?
            ORDER BY (messages_ready + messages_unacked) DESC, queue_name ASC
            LIMIT ?
            `,
            [did, Number(limit) || 100],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

async function getLatestRabbitSummary() {
    let db;
    try {
        db = await getConnectionLocalCdc();
        const rows = await executeQuery(
            db,
            `
            SELECT *
            FROM sat_rabbitmq_resumen
            ORDER BY id DESC
            LIMIT 1
            `,
            [],
            { timeoutMs: MONITOREO_TIMEOUT_MS }
        );

        const row = rows?.[0] || null;
        if (!row) return null;
        return {
            ...row,
            detalle_json: row?.detalle_json ? JSON.parse(row.detalle_json) : null,
        };
    } finally {
        if (db?.release) {
            try { db.release(); } catch { }
        }
    }
}

module.exports = {
    collectRabbitSnapshot,
    getLatestRabbitOverview,
    getLatestRabbitQueues,
    getLatestRabbitSummary,
};
