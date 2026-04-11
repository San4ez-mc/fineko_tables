const { buildReports } = require("../google/reportBuilder");
const { sendMessage, sendPhoto } = require("./bot");

const BUILD_REPORTS_ACTION = "build_reports";
const DRAFTS = new Map();

function extractMessage(update) {
    if (update.message) {
        return update.message;
    }

    if (update.callback_query && update.callback_query.message) {
        return update.callback_query.message;
    }

    return null;
}

function extractCommand(text) {
    if (!text || !text.startsWith("/")) {
        return "";
    }

    return text.trim().split(/\s+/)[0].toLowerCase();
}

function extractAction(update) {
    if (update.callback_query?.data) {
        return update.callback_query.data;
    }

    return "";
}

function getChatId(message) {
    return message?.chat?.id;
}

function getTelegramIdentity(message) {
    return {
        telegram_id: message?.from?.id,
        telegram_username: message?.from?.username || null
    };
}

function getDraft(chatId) {
    return DRAFTS.get(chatId) || null;
}

function setDraft(chatId, draft) {
    DRAFTS.set(chatId, {
        ...draft,
        updatedAt: new Date().toISOString()
    });
}

function clearDraft(chatId) {
    DRAFTS.delete(chatId);
}

function tryParseJson(text) {
    const trimmed = String(text || "").trim();
    if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
        return null;
    }

    try {
        return JSON.parse(trimmed);
    } catch {
        return null;
    }
}

function mergeTzText(previous, incoming) {
    const trimmedIncoming = String(incoming || "").trim();
    if (!trimmedIncoming) {
        return previous || "";
    }

    const replacePrefix = trimmedIncoming.match(/^(replace|заміни|перезапиши)\s*[:\-]\s*/i);
    if (replacePrefix) {
        return trimmedIncoming.slice(replacePrefix[0].length).trim();
    }

    if (!previous) {
        return trimmedIncoming;
    }

    return `${previous}\n${trimmedIncoming}`;
}

function normalizeIncomingPayload(rawPayload, message) {
    if (!rawPayload || typeof rawPayload !== "object" || Array.isArray(rawPayload)) {
        return null;
    }

    const identity = getTelegramIdentity(message);

    return {
        ...rawPayload,
        telegram_id: rawPayload.telegram_id || identity.telegram_id,
        telegram_username: rawPayload.telegram_username ?? identity.telegram_username
    };
}

async function captureUserTzMessage(message) {
    const chatId = getChatId(message);
    const text = String(message?.text || "").trim();
    if (!chatId || !text) {
        return false;
    }

    if (text.startsWith("/")) {
        return false;
    }

    const parsedJson = tryParseJson(text);
    const draft = getDraft(chatId) || {};

    if (parsedJson) {
        const normalizedPayload = normalizeIncomingPayload(parsedJson, message);
        if (!normalizedPayload) {
            await sendMessage(chatId, "Не зміг розпізнати JSON. Перевір формат і спробуй ще раз.");
            return true;
        }

        setDraft(chatId, {
            ...draft,
            mode: "json",
            payload: normalizedPayload
        });

        return { captured: true, payload: normalizedPayload, mode: "json" };
    }

    const mergedText = mergeTzText(draft.tzText || "", text);
    const identity = getTelegramIdentity(message);
    const payload = {
        telegram_id: identity.telegram_id,
        telegram_username: identity.telegram_username,
        tz_text: mergedText,
        process_model: {}
    };

    setDraft(chatId, {
        ...draft,
        mode: "text",
        tzText: mergedText,
        payload
    });

    return { captured: true, payload, mode: "text" };
}

function buildDefaultPayload(message) {
    const telegramId = message.from?.id;
    const telegramUsername = message.from?.username || null;

    return {
        telegram_id: telegramId,
        telegram_username: telegramUsername,
        business_type: "unknown",
        process_model: {},
        financial_reports_model: {
            business_type: "unknown",
            cashflow_items: {
                income: [],
                cogs: [],
                team: [],
                operations: [],
                taxes: []
            },
            pl_structure: {
                revenue: [],
                cogs: [],
                gross_profit: "revenue - cogs",
                opex: [],
                operating_profit: "gross_profit - opex",
                owner_payout: [],
                pre_tax_profit: "operating_profit - owner_payout",
                taxes: [],
                net_profit: "pre_tax_profit - taxes"
            },
            items_count: 0,
            status: "complete"
        }
    };
}

async function fetchPayloadFromSource(telegramId) {
    const sourceUrl = process.env.REPORTS_SOURCE_API_URL;
    if (!sourceUrl) {
        return null;
    }

    const url = new URL(sourceUrl);
    url.searchParams.set("telegram_id", String(telegramId));

    const headers = {};
    if (process.env.REPORTS_SOURCE_API_KEY) {
        headers.Authorization = `Bearer ${process.env.REPORTS_SOURCE_API_KEY}`;
    }

    const response = await fetch(url, { headers });
    if (!response.ok) {
        throw new Error(`Failed to load user payload from source API: ${response.status}`);
    }

    return response.json();
}

function formatSuccessMessage(result) {
    const lines = ["Готово! Таблиці створені.", `Папка: ${result.folder_url}`];

    if (Array.isArray(result.generated_files) && result.generated_files.length > 0) {
        result.generated_files.forEach((file) => {
            lines.push(`${file.title}: ${file.spreadsheet_url}`);
        });
    } else {
        lines.push(`Cashflow: ${result.cashflow_url}`);
        lines.push(`P&L: ${result.pl_url}`);
    }

    lines.push(`Validation: ${result.validation.valid ? "OK" : "FAILED"}`);

    if (result.share_warnings?.length) {
        lines.push("Увага: автоматичний доступ по лінку не вдалося повністю налаштувати.");
    }

    return lines.join("\n");
}

function formatErrorMessage(error) {
    const details = error.message.includes("The caller does not have permission")
        ? "Google не дозволив операцію. Найчастіше це означає, що service account не має ролі Editor/Content manager на цільову папку або в цьому домені заборонено змінювати sharing."
        : error.message.includes("Drive storage quota has been exceeded")
            ? "Перевищено квоту Drive для service account. Потрібно створювати файли у Shared Drive (або через акаунт з реальною квотою), а не в особистому root service account."
            : error.message;

    return [
        "Не вдалося згенерувати таблиці.",
        "Спробуй ще раз через 1-2 хвилини.",
        `Технічна причина: ${details}`
    ].join("\n");
}

function buildWelcomeMessage() {
    return [
        "Привіт! Це бот Олександра Мацука для автоматичного створення фінансових таблиць.",
        "Надішли мені або JSON ТЗ, або текстовий опис (простими словами), і я одразу згенерую таблиці.",
        "Можеш надсилати додаткові повідомлення з правками, я оновлю чернетку ТЗ.",
        "Для очищення чернетки використай команду /clear."
    ].join("\n\n");
}

function getBrandPhotoUrl() {
    const appBaseUrl = process.env.APP_BASE_URL;
    if (!appBaseUrl) {
        return "";
    }

    return `${appBaseUrl.replace(/\/$/, "")}/brand-photo`;
}

async function handleBuildReports(message) {
    const telegramId = message.from?.id;
    if (!telegramId) {
        throw new Error("Missing Telegram user id");
    }

    const chatId = getChatId(message);
    const draft = chatId ? getDraft(chatId) : null;
    if (draft?.payload) {
        return buildReports(draft.payload);
    }

    const payload =
        (await fetchPayloadFromSource(telegramId)) ||
        buildDefaultPayload(message);

    return buildReports(payload);
}

async function runBuildAndReply(message, commandLabel) {
    await sendMessage(message.chat.id, "Починаю побудову таблиць. Це може зайняти до хвилини...");

    try {
        const result = await handleBuildReports(message);
        await sendMessage(message.chat.id, formatSuccessMessage(result));
        return { handled: true, command: commandLabel, result };
    } catch (error) {
        await sendMessage(message.chat.id, formatErrorMessage(error));
        return { handled: true, command: commandLabel, error: error.message };
    }
}

async function handleTelegramUpdate(update) {
    const message = extractMessage(update);
    if (!message || !message.chat) {
        return { handled: false, reason: "No message context" };
    }

    const chatId = getChatId(message);

    const command = extractCommand(message.text || "");
    const action = extractAction(update);
    const shouldStart = command === "/start";
    const shouldBuild = ["/build_reports", "/tables"].includes(command) || action === BUILD_REPORTS_ACTION;
    const shouldClearDraft = command === "/clear";

    if (!shouldStart && !shouldBuild && !shouldClearDraft) {
        const captured = await captureUserTzMessage(message);
        if (captured?.captured) {
            const ack = captured.mode === "json"
                ? "Отримав JSON ТЗ, запускаю генерацію."
                : "Отримав текст ТЗ/правки, запускаю генерацію.";
            await sendMessage(message.chat.id, ack);
            return runBuildAndReply(message, "tz_update_auto_build");
        }

        return {
            handled: false,
            reason: "Unknown command",
            help: "Send JSON TZ or text description"
        };
    }

    if (shouldClearDraft) {
        if (chatId) {
            clearDraft(chatId);
        }

        await sendMessage(
            message.chat.id,
            "Чернетку ТЗ очищено. Надішли новий JSON або текстовий опис."
        );

        return { handled: true, command: command || action };
    }

    if (shouldStart) {
        const welcomeMessage = buildWelcomeMessage();
        const brandPhotoUrl = getBrandPhotoUrl();

        if (brandPhotoUrl) {
            await sendPhoto(message.chat.id, brandPhotoUrl, welcomeMessage);
        } else {
            await sendMessage(message.chat.id, welcomeMessage);
        }

        return { handled: true, command: "/start" };
    }

    return runBuildAndReply(message, command || action || "manual_build");
}

module.exports = {
    handleTelegramUpdate
};
