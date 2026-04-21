const { isBefore21, isSameDate, toDateOnly } = require("./dateUtils");
const { getNextBusinessDay } = require("./holidayService");

function getEstadosByCode(estados, code) {
    return estados.filter((item) => Number(item.estado) === code);
}

function hasEstadoBefore21OnDate(estados, code, targetDate) {
    return estados.some((item) =>
        Number(item.estado) === code &&
        isSameDate(item.fecha, targetDate) &&
        isBefore21(item.fecha)
    );
}

function hasEntregaOnDateBefore21(estados, targetDate) {
    return estados.some((item) =>
        Number(item.estado) === 5 &&
        isSameDate(item.fecha, targetDate) &&
        isBefore21(item.fecha)
    );
}

async function calculatePenalizada(envio, estados, countryCode) {
    const entregas = getEstadosByCode(estados, 5);
    if (!entregas.length) {
        return null;
    }

    const fechaDespacho = envio.fecha_despacho;
    const hayEntregaMismoDia = entregas.some((item) => isSameDate(item.fecha, fechaDespacho));

    if (hayEntregaMismoDia) {
        const entregaAntesDe21 = hasEntregaOnDateBefore21(estados, fechaDespacho);
        if (entregaAntesDe21) {
            return 0;
        }

        const nadieEnCasaAntesDe21 = hasEstadoBefore21OnDate(estados, 6, fechaDespacho);
        return nadieEnCasaAntesDe21 ? 0 : 1;
    }

    const nadieEnCasaAntesDe21 = hasEstadoBefore21OnDate(estados, 6, fechaDespacho);
    if (!nadieEnCasaAntesDe21) {
        return 1;
    }

    const proximoDiaHabil = await getNextBusinessDay(fechaDespacho, countryCode);
    if (!proximoDiaHabil) {
        return 1;
    }

    const entregaValidaDiaHabil = hasEntregaOnDateBefore21(estados, proximoDiaHabil);
    return entregaValidaDiaHabil ? 0 : 1;
}

module.exports = {
    calculatePenalizada,
    getEstadosByCode,
    hasEstadoBefore21OnDate,
    hasEntregaOnDateBefore21,
    toDateOnly,
};
