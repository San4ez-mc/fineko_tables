const { buildReports } = require("../google/reportBuilder");
const { answerCallbackQuery, sendMessage, sendPhoto } = require("./bot");

const BUILD_REPORTS_ACTION = "build_reports";
const CLEAR_DRAFT_ACTION = "clear_draft";
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

        await sendMessage(
            chatId,
            "Отримав JSON ТЗ і зберіг як поточну чернетку. Натисни кнопку \"Згенерувати таблиці\", щоб застосувати зміни.",
            { reply_markup: buildStartKeyboard() }
        );

        return true;
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

    await sendMessage(
        chatId,
        "Отримав правки до ТЗ і оновив чернетку. Натисни \"Згенерувати таблиці\", щоб перебудувати таблиці з новими даними.",
        { reply_markup: buildStartKeyboard() }
    );

    return true;
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
        "Надішли мені або JSON ТЗ, або текстовий опис (простими словами), і я зберу з цього таблиці.",
        "Можеш надсилати додаткові повідомлення з правками, я оновлю чернетку ТЗ.",
        "Потім натисни кнопку \"Згенерувати таблиці\"."
    ].join("\n\n");
}

function buildStartKeyboard() {
    return {
        inline_keyboard: [
            [
                {
                    text: "Згенерувати таблиці",
                    callback_data: BUILD_REPORTS_ACTION
                },
                {
                    text: "Очистити чернетку",
                    callback_data: CLEAR_DRAFT_ACTION
                }
            ]
        ]
    };
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
    const shouldClearDraft = action === CLEAR_DRAFT_ACTION || command === "/clear";

    if (!shouldStart && !shouldBuild && !shouldClearDraft) {
        const captured = await captureUserTzMessage(message);
        if (captured) {
            return { handled: true, command: "tz_update" };
        }

        return {
            handled: false,
            reason: "Unknown command",
            help: "Send JSON TZ or text description, then press Generate"
        };
    }

    if (shouldClearDraft) {
        if (chatId) {
            clearDraft(chatId);
        }

        if (update.callback_query?.id) {
            await answerCallbackQuery(update.callback_query.id, "Чернетку очищено");
        }

        await sendMessage(
            message.chat.id,
            "Чернетку ТЗ очищено. Надішли новий JSON або текстовий опис.",
            { reply_markup: buildStartKeyboard() }
        );

        return { handled: true, command: command || action };
    }

    if (shouldStart) {
        const welcomeMessage = buildWelcomeMessage();
        const brandPhotoUrl = getBrandPhotoUrl();
        const replyMarkup = { reply_markup: buildStartKeyboard() };

        if (brandPhotoUrl) {
            await sendPhoto(message.chat.id, brandPhotoUrl, welcomeMessage, replyMarkup);
        } else {
            await sendMessage(message.chat.id, welcomeMessage, replyMarkup);
        }

        return { handled: true, command: "/start" };
    }

    if (update.callback_query?.id) {
        await answerCallbackQuery(update.callback_query.id, "Запускаю генерацію...");
    }

    await sendMessage(message.chat.id, "Починаю побудову таблиць. Це може зайняти до хвилини...");

    try {
        const result = await handleBuildReports(message);
        await sendMessage(message.chat.id, formatSuccessMessage(result));
        return { handled: true, command: command || action, result };
    } catch (error) {
        await sendMessage(message.chat.id, formatErrorMessage(error));
        return { handled: true, command: command || action, error: error.message };
    }
}

module.exports = {
    handleTelegramUpdate
};
