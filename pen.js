const { pendientesHoy } = require("./controller/pendientesHoy/pendientes2.js");

const LOOP_PAUSE_MS = 1000;
const STOP_ON_SIGNAL = false;

let running = false;
let stopRequested = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    const result = await pendientesHoy();
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
    STOP_ON_SIGNAL,
  });

  let tick = 0;
  while (!stopRequested) {
    tick += 1;
    console.log(`🧭 [PEN] tick=${tick} stopRequested=${stopRequested} running=${running}`);
    await runPendientesTick();

    if (stopRequested) break;
    console.log(`😴 [PEN] sleep ${LOOP_PAUSE_MS} ms antes del proximo tick`);
    await sleep(LOOP_PAUSE_MS);
  }

  console.log("🛑 [PEN] Loop detenido");
}

function requestStop(signal) {
  console.log(`🛑 [PEN] Recibi ${signal}, freno despues de la corrida actual...`);
  stopRequested = true;
}

process.on("SIGINT", () => {
  if (!STOP_ON_SIGNAL) {
    console.log("[PEN] SIGINT recibido; lo ignoro para mantener viva la cache en memoria");
    return;
  }
  requestStop("SIGINT");
});

process.on("SIGTERM", () => {
  if (!STOP_ON_SIGNAL) {
    console.log("[PEN] SIGTERM recibido; lo ignoro para mantener viva la cache en memoria");
    return;
  }
  requestStop("SIGTERM");
});

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
