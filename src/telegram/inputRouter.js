function normalizeText(value) {
    return String(value || "").trim();
}

function resolveReportType(text, state = {}) {
    const explicit = normalizeText(state.reportType || state.report_type).toLowerCase();
    if (["cashflow", "pl", "balance", "cashflow_and_pl", "dashboard"].includes(explicit)) {
        return explicit;
    }

    const source = String(text || "").toLowerCase();
    if ((/(кешфлоу|кеш\s*флоу|cash\s*flow|cashflow)/i.test(source)) && (/(p&l|\bpl\b|profit\s*(and|&)\s*loss|прибутк(и|у)?\s*і\s*збитк(и|ів))/i.test(source))) return "cashflow_and_pl";
    if (/(кешфлоу|кеш\s*флоу|кефлоу|cash\s*flow|cashflow)/i.test(source)) return "cashflow";
    if (/(p&l|\bpl\b|п\s*&\s*л|profit\s*(and|&)\s*loss|прибутк(и|у)?\s*і\s*збитк(и|ів))/i.test(source)) return "pl";
    if (/(баланс|balance)/i.test(source)) return "balance";
    if (/(дашборд|dashboard)/i.test(source)) return "dashboard";
    return "";
}

function isArticleOnlyCashflowSource(text, state = {}) {
    const source = String(text || "");
    const fileName = normalizeText(state.fileName || state.file_name).toLowerCase();

    if (fileName === "cashflow_articles.md") {
        return true;
    }

    const normalizedLines = source
        .split(/\r?\n/)
        .map((line) => normalizeText(line))
        .filter(Boolean);

    if (normalizedLines.length === 0) {
        return false;
    }

    const keyedLines = normalizedLines.filter((line) => /^[a-zа-яіїє_][a-zа-яіїє0-9_\s-]*\s*:/i.test(line));
    const allowedKeys = /^(business_name|report_type|inflows|outflows|надходження|витрати)\s*:/i;
    const hasArticleSections = keyedLines.some((line) => /^(inflows|outflows|надходження|витрати)\s*:/i.test(line));
    const hasOnlyAllowedKeys = keyedLines.every((line) => allowedKeys.test(line));

    if (keyedLines.length > 0) {
        return hasArticleSections && hasOnlyAllowedKeys;
    }

    const compactArticleList = /(?:^|\n)\s*(надходження|витрати)\s*:/i.test(source)
        && !/[.!?]/.test(source)
        && normalizedLines.length <= 6;

    return compactArticleList;
}

function isMinimalCashflowTz(text, state = {}) {
    const source = String(text || "");
    const fileName = normalizeText(state.fileName || state.file_name).toLowerCase();
    const reportType = resolveReportType(source, state);
    const isCashflowFile = fileName === "cashflow_articles.md";
    const hasCashflowType = reportType === "cashflow" || isCashflowFile;

    if (!hasCashflowType) return false;
    if (!isArticleOnlyCashflowSource(source, state)) return false;
    if (/(платіжний\s*календар|відповідальн|форма|налаштування|логи|лог\b|персональні\s*аркуші)/i.test(source)) return false;
    if (/(^|\b)mode\s*:\s*(full|similar)\b/i.test(source)) return false;

    return true;
}

function looksLikeStructuredTz(text) {
    const source = String(text || "");
    if (!source.trim()) return false;
    if (/```\s*tz\s*\n/i.test(source)) return true;
    return /(business_name\s*:|report_type\s*:|inflows\s*:|outflows\s*:|assets\s*:|liabilities\s*:|equity\s*:)/i.test(source);
}

function classifyInput(text, state = {}) {
    const source = String(text || "");
    const normalized = normalizeText(source);

    if (!normalized) return "empty";
    if (normalized.startsWith("/")) return "command";

    const activeQueue = Array.isArray(state.questionQueue) ? state.questionQueue : [];
    if (state.stage === "collecting" && activeQueue.length > 0) {
        return "clarification_answer";
    }

    if (isMinimalCashflowTz(source, state)) {
        return "minimal_cashflow_tz";
    }

    if (looksLikeStructuredTz(source)) {
        return "structured_tz";
    }

    return "free_text";
}

module.exports = {
    classifyInput,
    looksLikeStructuredTz,
    isMinimalCashflowTz
};
