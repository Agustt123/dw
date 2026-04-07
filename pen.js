const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");

const LOOP_PAUSE_MS = Number(process.env.PEN_LOOP_PAUSE_MS || 5000);
const RUN_TIMEOUT_MS = Number(process.env.PEN_TIMEOUT_MS || 250000);

let running = false;
let stopRequested = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout(promise, timeoutMs, label) {
  let timer = null;

  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timeout despues de ${timeoutMs} ms`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function runPendientesTick() {
  if (running) {
    console.log("⏭️ [PEN] pendientesHoy sigue corriendo, salteo tick");
    return;
  }

  running = true;
  const startedAt = Date.now();

  try {
    console.log("🔁 [PEN] pendientesHoy: iniciando...");
    const result = await withTimeout(pendientesHoy(), RUN_TIMEOUT_MS, "pendientesHoy");
    const elapsedMs = Date.now() - startedAt;
    console.log("✅ [PEN] pendientesHoy completado", {
      elapsedMs,
      result,
    });
  } catch (err) {
    const elapsedMs = Date.now() - startedAt;
    console.error("❌ [PEN] Error en pendientesHoy:", {
      elapsedMs,
      message: err?.message || String(err),
    });
  } finally {
    running = false;
  }
}

async function main() {
  console.log("🚀 [PEN] Iniciando loop continuo de pendientesHoy...", {
    LOOP_PAUSE_MS,
    RUN_TIMEOUT_MS,
  });

  while (!stopRequested) {
    await runPendientesTick();

    if (stopRequested) break;
    await sleep(LOOP_PAUSE_MS);
  }

  console.log("🛑 [PEN] Loop detenido");
}

function requestStop(signal) {
  console.log(`🛑 [PEN] Recibi ${signal}, freno despues de la corrida actual...`);
  stopRequested = true;
}

process.on("SIGINT", () => requestStop("SIGINT"));
process.on("SIGTERM", () => requestStop("SIGTERM"));

process.on("unhandledRejection", (reason) => {
  console.error("❌ [PEN] unhandledRejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("❌ [PEN] uncaughtException:", err);
});

main().catch((err) => {
  console.error("❌ [PEN] Error fatal:", err?.message || err);
  process.exit(1);
});
