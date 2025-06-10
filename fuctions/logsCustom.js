const os = require("os");


// const isLocal = os.hostname() != "asignaciones";

const isLocal = true;

function logGreen(message) {
    if (isLocal) {
        console.log(`\x1b[32m%s\x1b[0m`, `✅ ${message}
--------------------------------------------------`);
    }
}

function logRed(message) {
    if (isLocal) {
        console.log(`\x1b[31m%s\x1b[0m`, `❌ ${message}
--------------------------------------------------`);
    }
}

function logBlue(message) {
    if (isLocal) {
        console.log(`\x1b[34m%s\x1b[0m`, `🔵 ${message}
--------------------------------------------------`);
    }
}

function logYellow(message) {
    if (isLocal) {
        console.log(`\x1b[33m%s\x1b[0m`, `⚠️  ${message}
--------------------------------------------------`);
    }
}

function logPurple(message) {
    if (isLocal) {
        console.log(`\x1b[35m%s\x1b[0m`, `💜 ${message}
--------------------------------------------------`);;
    }
}

function logCyan(message) {
    if (isLocal) {
        console.log(`\x1b[36m%s\x1b[0m`, `💎 ${message}
--------------------------------------------------`);
    }
}

module.exports = { logGreen, logRed, logBlue, logYellow, logPurple, logCyan };