const DEFAULT_TIMEOUT_MS = Number(process.env.APPS_SCRIPT_TIMEOUT_MS || 120000);

function getAppsScriptUrl() {
    const url = process.env.APPS_SCRIPT_URL;
    if (!url) {
        throw new Error("Missing APPS_SCRIPT_URL");
    }

    return url;
}

async function callAppsScript(payload, options = {}) {
    const url = getAppsScriptUrl();
    const timeoutMs = Number(options.timeoutMs || DEFAULT_TIMEOUT_MS);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload || {}),
            signal: controller.signal
        });

        const text = await response.text();
        let data = null;

        try {
            data = text ? JSON.parse(text) : null;
        } catch {
            data = null;
        }

        if (!response.ok) {
            throw new Error(`Apps Script HTTP ${response.status}: ${text || "empty response"}`);
        }

        if (!data || typeof data !== "object") {
            throw new Error("Apps Script returned non-JSON response");
        }

        if (data.status && String(data.status).toLowerCase() === "error") {
            const details = [data.message, data.details].filter(Boolean).join(" | ");
            throw new Error(details || "Apps Script returned status=error");
        }

        return data;
    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error(`Apps Script timeout after ${timeoutMs}ms`);
        }

        throw error;
    } finally {
        clearTimeout(timer);
    }
}

async function pingAppsScript() {
    return callAppsScript({ action: "ping" });
}

async function buildTableViaAppsScript(payload) {
    return callAppsScript({
        action: "build_table",
        ...(payload || {})
    });
}

async function updateTableViaAppsScript(payload) {
    return callAppsScript({
        action: "update_table",
        ...(payload || {})
    });
}

async function listTablesViaAppsScript(payload) {
    return callAppsScript({
        action: "list_tables",
        ...(payload || {})
    });
}

module.exports = {
    callAppsScript,
    pingAppsScript,
    buildTableViaAppsScript,
    updateTableViaAppsScript,
    listTablesViaAppsScript
};
