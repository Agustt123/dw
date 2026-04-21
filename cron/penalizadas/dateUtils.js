function toDate(value) {
    if (!value) return null;

    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return null;

    return date;
}

function toDateOnly(value) {
    const date = toDate(value);
    if (!date) return "";

    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
}

function getHour(value) {
    const date = toDate(value);
    if (!date) return null;
    return date.getHours();
}

function isBefore21(value) {
    const hour = getHour(value);
    return hour !== null && hour < 21;
}

function isSameDate(a, b) {
    return toDateOnly(a) === toDateOnly(b);
}

function addDays(value, days) {
    const date = toDate(value);
    if (!date) return null;

    const next = new Date(date);
    next.setDate(next.getDate() + days);
    return next;
}

function getYear(value) {
    const date = toDate(value);
    if (!date) return null;
    return date.getFullYear();
}

module.exports = {
    toDate,
    toDateOnly,
    getHour,
    isBefore21,
    isSameDate,
    addDays,
    getYear,
};
