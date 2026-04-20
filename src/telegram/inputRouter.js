function normalizeText(value) {
    return String(value || "").trim();
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

    if (looksLikeStructuredTz(source)) {
        return "structured_tz";
    }

    return "free_text";
}

module.exports = {
    classifyInput,
    looksLikeStructuredTz
};
