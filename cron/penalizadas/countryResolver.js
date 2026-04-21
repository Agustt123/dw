const DEFAULT_COUNTRY_CODE = "AR";

const SUPPORTED_COUNTRIES = {
    argentina: "AR",
    ar: "AR",
    chile: "CL",
    cl: "CL",
    colombia: "CO",
    co: "CO",
    peru: "PE",
    pe: "PE",
    mexico: "MX",
    mx: "MX",
    uruguay: "UY",
    uy: "UY",
};

const OWNER_COUNTRY_BY_ID = {
    // Ejemplo futuro:
    // 131: "CL",
};

function normalizeCountryValue(value) {
    if (!value) return DEFAULT_COUNTRY_CODE;

    const normalized = String(value).trim().toLowerCase();
    return SUPPORTED_COUNTRIES[normalized] || DEFAULT_COUNTRY_CODE;
}

function resolveCountryCodeForOwner(didOwner) {
    const configuredCountry = OWNER_COUNTRY_BY_ID[String(didOwner)] || OWNER_COUNTRY_BY_ID[Number(didOwner)];
    return normalizeCountryValue(configuredCountry);
}

module.exports = {
    DEFAULT_COUNTRY_CODE,
    SUPPORTED_COUNTRIES,
    OWNER_COUNTRY_BY_ID,
    normalizeCountryValue,
    resolveCountryCodeForOwner,
};
