const DEFAULT_TIMEOUT_MS = Number(process.env.APPS_SCRIPT_TIMEOUT_MS || 120000);
const APPS_SCRIPT_DEBUG = String(process.env.APPS_SCRIPT_DEBUG || "").toLowerCase() === "true";

function buildTraceId() {
    return `as_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}

function summarizePayload(payload) {
    const p = payload || {};
    return {
        action: p.action,
        report_type: p.report_type,
        spreadsheet_id: p.spreadsheet_id,
        telegram_id: p.telegram_id,
        has_articles: Boolean(p.articles),
        inflows: Array.isArray(p.articles?.inflows) ? p.articles.inflows.length : undefined,
        outflows: Array.isArray(p.articles?.outflows) ? p.articles.outflows.length : undefined,
        changes: Array.isArray(p.changes) ? p.changes.length : undefined
    };
}

function getAppsScriptUrl() {
    const url = process.env.APPS_SCRIPT_URL;
    if (!url) {
        throw new Error("Missing APPS_SCRIPT_URL");
    }

    return url;
}

function withDriveParentFolder(payload) {
    const driveParentFolderId = process.env.GOOGLE_DRIVE_PARENT_FOLDER_ID;
    if (!driveParentFolderId) {
        return { ...(payload || {}) };
    }

    return {
        ...(payload || {}),
        drive_parent_folder_id: driveParentFolderId
    };
}

async function callAppsScript(payload, options = {}) {
    const url = getAppsScriptUrl();
    const timeoutMs = Number(options.timeoutMs || DEFAULT_TIMEOUT_MS);
    const traceId = buildTraceId();
    const startedAt = Date.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const requestPayload = withDriveParentFolder(payload);

    console.info("AppsScript request start", {
        traceId,
        timeoutMs,
        summary: summarizePayload(requestPayload)
    });
    if (APPS_SCRIPT_DEBUG) {
        console.info("AppsScript request payload", { traceId, payload: requestPayload || {} });
    }

    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(requestPayload || {}),
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

        console.info("AppsScript request success", {
            traceId,
            durationMs: Date.now() - startedAt,
            httpStatus: response.status,
            responseStatus: data.status,
            validationValid: data.validation?.valid,
            warnings: Array.isArray(data.validation?.warnings) ? data.validation.warnings.length : undefined,
            errors: Array.isArray(data.validation?.errors) ? data.validation.errors.length : undefined
        });
        if (APPS_SCRIPT_DEBUG) {
            console.info("AppsScript response payload", { traceId, data });
        }

        return data;
    } catch (error) {
        if (error.name === "AbortError") {
            console.error("AppsScript request timeout", {
                traceId,
                timeoutMs,
                durationMs: Date.now() - startedAt,
                summary: summarizePayload(requestPayload)
            });
            throw new Error(`Apps Script timeout after ${timeoutMs}ms`);
        }

        console.error("AppsScript request failed", {
            traceId,
            durationMs: Date.now() - startedAt,
            summary: summarizePayload(requestPayload),
            error: String(error?.message || error)
        });

        throw error;
    } finally {
        clearTimeout(timer);
    }
}

async function pingAppsScript(payload) {
    return callAppsScript({
        action: "ping",
        ...(payload || {})
    });
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

async function validateTableViaAppsScript(payload) {
    return callAppsScript({
        action: "validate_table",
        ...(payload || {})
    });
}

module.exports = {
    callAppsScript,
    pingAppsScript,
    buildTableViaAppsScript,
    updateTableViaAppsScript,
    listTablesViaAppsScript,
    validateTableViaAppsScript
};
