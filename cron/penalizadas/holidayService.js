const axios = require("axios");
const { addDays, getYear, toDateOnly } = require("./dateUtils");

const holidayCache = new Map();
const HOLIDAY_API_TIMEOUT_MS = Number(process.env.HOLIDAY_API_TIMEOUT_MS || 15000);

async function loadHolidaySet(countryCode, year) {
    const cacheKey = `${countryCode}-${year}`;
    if (holidayCache.has(cacheKey)) {
        return holidayCache.get(cacheKey);
    }

    const response = await axios.get(
        `https://date.nager.at/api/v3/publicholidays/${year}/${countryCode}`,
        { timeout: HOLIDAY_API_TIMEOUT_MS }
    );

    const holidaySet = new Set(
        (response.data || [])
            .map((item) => item?.date)
            .filter(Boolean)
    );

    holidayCache.set(cacheKey, holidaySet);
    return holidaySet;
}

async function isBusinessDay(dateValue, countryCode) {
    const date = addDays(dateValue, 0);
    if (!date) return false;

    const dayOfWeek = date.getDay();
    if (dayOfWeek === 0) {
        return false;
    }

    const year = getYear(date);
    const holidaySet = await loadHolidaySet(countryCode, year);
    return !holidaySet.has(toDateOnly(date));
}

async function getNextBusinessDay(dateValue, countryCode) {
    let cursor = addDays(dateValue, 1);

    while (cursor) {
        const businessDay = await isBusinessDay(cursor, countryCode);
        if (businessDay) {
            return cursor;
        }

        cursor = addDays(cursor, 1);
    }

    return null;
}

module.exports = {
    holidayCache,
    loadHolidaySet,
    isBusinessDay,
    getNextBusinessDay,
};
