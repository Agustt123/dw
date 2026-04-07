const os = require("os");
const fs = require("fs");
const path = require("path");

function safeNumber(n) {
    return Number.isFinite(n) ? n : null;
}

function round1(n) {
    return Math.round(n * 10) / 10;
}

function readCpuTimes() {
    const cpus = os.cpus();
    let user = 0;
    let nice = 0;
    let sys = 0;
    let idle = 0;
    let irq = 0;

    for (const cpu of cpus) {
        user += cpu.times.user;
        nice += cpu.times.nice;
        sys += cpu.times.sys;
        idle += cpu.times.idle;
        irq += cpu.times.irq;
    }

    const total = user + nice + sys + idle + irq;
    return { user, nice, sys, idle, irq, total };
}

let lastCpu = readCpuTimes();

function cpuHostPercentSinceLast() {
    const now = readCpuTimes();
    const idleDelta = now.idle - lastCpu.idle;
    const totalDelta = now.total - lastCpu.total;

    lastCpu = now;

    if (totalDelta <= 0) return null;

    const usage = 1 - idleDelta / totalDelta;
    return round1(usage * 100);
}

function getDiskUsage(targetPath = process.cwd()) {
    try {
        if (typeof fs.statfsSync !== "function") return null;

        const resolvedPath = path.resolve(targetPath);
        const stat = fs.statfsSync(resolvedPath);

        const blockSize = Number(stat.bsize ?? stat.frsize ?? 0);
        const totalBlocks = Number(stat.blocks ?? 0);
        const freeBlocks = Number(stat.bavail ?? stat.bfree ?? 0);

        if (!blockSize || !totalBlocks) return null;

        const totalBytes = totalBlocks * blockSize;
        const freeBytes = freeBlocks * blockSize;
        const usedBytes = totalBytes - freeBytes;
        const usePct = totalBytes > 0 ? round1((usedBytes / totalBytes) * 100) : null;

        return {
            path: resolvedPath,
            total_bytes: totalBytes,
            used_bytes: usedBytes,
            free_bytes: freeBytes,
            use_pct: usePct,
        };
    } catch {
        return null;
    }
}

function getTempC() {
    if (process.platform !== "linux") return null;

    try {
        const zones = fs.readdirSync("/sys/class/thermal").filter((entry) =>
            entry.startsWith("thermal_zone")
        );

        for (const zone of zones) {
            const tempPath = `/sys/class/thermal/${zone}/temp`;
            if (!fs.existsSync(tempPath)) continue;

            const raw = fs.readFileSync(tempPath, "utf8").trim();
            const value = Number(raw);

            if (Number.isFinite(value)) {
                return value > 200 ? round1(value / 1000) : round1(value);
            }
        }
    } catch {
        return null;
    }

    return null;
}

function buildSimple(raw) {
    const host = raw.host ?? {};
    const proc = raw.process ?? {};
    const disk = host.disk ?? {};

    const memTotal = Number(host.mem_total_bytes ?? 0);
    const memFree = Number(host.mem_free_bytes ?? 0);
    const ramUsedBytes = memTotal > 0 ? memTotal - memFree : 0;

    return {
        servicio: raw.service,
        timestamp: raw.ts,
        estado: raw.status,
        host: host.hostname ?? null,
        cpuCores: host.cpus ?? null,
        usoCpuPct: safeNumber(host.cpu_usage_pct_estimate),
        carga1m: Array.isArray(host.loadavg_1_5_15) ? safeNumber(host.loadavg_1_5_15[0]) : null,
        usoRamPct: memTotal > 0 ? round1((ramUsedBytes / memTotal) * 100) : null,
        libreRamPct: memTotal > 0 ? round1((memFree / memTotal) * 100) : null,
        usoDiscoPct: safeNumber(disk.use_pct),
        discoLibreGb: Number.isFinite(disk.free_bytes) ? round1(disk.free_bytes / 1024 / 1024 / 1024) : null,
        discoUsadoGb: Number.isFinite(disk.used_bytes) ? round1(disk.used_bytes / 1024 / 1024 / 1024) : null,
        tempC: safeNumber(host.temp_c),
        pid: proc.pid ?? null,
        uptimeSec: proc.uptime_sec ?? null,
        ramProcesoMB: Number.isFinite(proc.rss_bytes) ? round1(proc.rss_bytes / 1024 / 1024) : null,
        heapUsadoMB: Number.isFinite(proc.heap_used_bytes) ? round1(proc.heap_used_bytes / 1024 / 1024) : null,
    };
}

async function collectSatMetrics(options = {}) {
    const serviceName = options.serviceName || process.env.SERVICE_NAME || "dw";
    const mem = process.memoryUsage();

    const raw = {
        service: serviceName,
        ts: new Date().toISOString(),
        status: "ok",
        process: {
            pid: process.pid,
            uptime_sec: Math.round(process.uptime()),
            rss_bytes: mem.rss,
            heap_used_bytes: mem.heapUsed,
            heap_total_bytes: mem.heapTotal,
        },
        host: {
            hostname: os.hostname(),
            platform: os.platform(),
            arch: os.arch(),
            cpus: os.cpus().length,
            loadavg_1_5_15: os.loadavg(),
            mem_total_bytes: os.totalmem(),
            mem_free_bytes: os.freemem(),
            cpu_usage_pct_estimate: cpuHostPercentSinceLast(),
            temp_c: getTempC(),
            disk: getDiskUsage(options.diskPath || process.cwd()),
        },
    };

    const simple = buildSimple(raw);
    return options.returnRaw === false ? simple : { simple, raw };
}

module.exports = { collectSatMetrics };
