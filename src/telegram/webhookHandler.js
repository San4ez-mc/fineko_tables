const { buildReports } = require("../google/reportBuilder");
const { sendMessage, sendPhoto } = require("./bot");
const { parseTzFromTelegramMessage, analyzeArchitecture } = require("./tzParser");

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

function splitAnswers(text) {
    return String(text || "")
        .split(/\r?\n/)
        .map((line) => line.replace(/^\d+[\).]\s*/, "").trim())
        .filter(Boolean);
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

function buildArchitectureMessage(analysis) {
    const lines = [
        "Проаналізував ТЗ. Ось що я бачу:",
        "",
        `Загальна кількість операцій: ${analysis.totalOps}/міс`
    ];

    if (analysis.noAccessPeople.length > 0) {
        lines.push(
            `Без доступу до Sheets: ${analysis.noAccessPeople.map((item) => `${item.name} (${item.ops} оп/міс)`).join(", ")}`
        );
    } else {
        lines.push("Без доступу до Sheets: немає");
    }

    lines.push("");
    lines.push("Архітектура:");
    lines.push(`-> Надходження: режим ${analysis.inflowsMode}`);
    lines.push(`-> Витрати: режим ${analysis.outflowsMode}`);
    lines.push("");
    lines.push("Перед тим як будувати, поставлю кілька уточнень.");

    return lines.join("\n");
}

function buildQuestionQueue(tz, analysis) {
    const questions = [
        {
            key: "google_email",
            text: "1) Вкажи Google email, на який створюємо/шаримо файл."
        },
        {
            key: "business_name",
            text: `2) Підтверди назву компанії для папки/файлу (зараз: ${tz.business_name || "Business"}).`
        },
        {
            key: "language",
            text: "3) Мова таблиці: українська чи англійська?"
        }
    ];

    for (const person of analysis.noAccessPeople) {
        questions.push({
            key: `no_access_method_${person.name}`,
            text: `${person.name} не має доступу до Sheets. Як зручно: Google Form чи ти вносиш дані за нього?`
        });
    }

    for (const item of analysis.highOpsItems) {
        questions.push({
            key: `high_ops_${item.article}`,
            text: `${item.article} - ${item.ops} операцій/міс. ${item.responsible} вносить кожну транзакцію чи пакетом раз на тиждень?`
        });
    }

    if (String(tz.report_type || "").toLowerCase() === "cashflow") {
        questions.push({
            key: "include_payment_calendar",
            text: "Додати аркуш Платіжного календаря? (так/ні)"
        });
    }

    questions.push(
        {
            key: "bank_accounts",
            text: "Є кілька банківських рахунків? Якщо так, зводити в один залишок чи вести окремо?"
        },
        {
            key: "counterparties",
            text: "Потрібен облік по контрагентах? (так/ні)"
        },
        {
            key: "file_conflict_strategy",
            text: "Якщо файл з такою назвою вже існує: перезаписати чи створити новий з датою?"
        }
    );

    return questions;
}

function nextQuestionBatch(queue = [], size = 3) {
    return queue.slice(0, size);
}

function normalizeAnswerValue(value, fallback) {
    const text = String(value || "").trim();
    return text || fallback || "";
}

function applyAnswers(draft, text) {
    const answers = splitAnswers(text);
    const batch = Array.isArray(draft.pendingQuestions) ? draft.pendingQuestions : [];
    const resolved = { ...(draft.answers || {}) };

    if (batch.length === 1) {
        resolved[batch[0].key] = normalizeAnswerValue(text);
    } else {
        batch.forEach((question, index) => {
            resolved[question.key] = normalizeAnswerValue(answers[index], answers[answers.length - 1] || "");
        });
    }

    const remainingQueue = Array.isArray(draft.questionsQueue)
        ? draft.questionsQueue.slice(batch.length)
        : [];

    return {
        answers: resolved,
        questionsQueue: remainingQueue,
        pendingQuestions: nextQuestionBatch(remainingQueue)
    };
}

function asYesNo(value) {
    const text = String(value || "").trim().toLowerCase();
    return ["так", "yes", "y", "true", "1"].includes(text);
}

function buildPayloadFromTzDraft(draft, message) {
    const identity = getTelegramIdentity(message);
    const answers = draft.answers || {};
    const tz = draft.tz || {};

    return {
        telegram_id: identity.telegram_id,
        telegram_username: identity.telegram_username,
        business_name: answers.business_name || tz.business_name || "Business",
        business_type: tz.business_type || "unknown",
        report_type: String(tz.report_type || "cashflow").toLowerCase(),
        tz_struct: tz,
        architecture: draft.architecture || {},
        answers,
        language: answers.language || "українська",
        include_payment_calendar: asYesNo(answers.include_payment_calendar),
        needs_counterparties: asYesNo(answers.counterparties),
        file_conflict_strategy: /перезапис/i.test(String(answers.file_conflict_strategy || "")) ? "overwrite" : "new_dated",
        process_model: {}
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

    const tzParsed = parseTzFromTelegramMessage(message);
    if (tzParsed.detected) {
        if (!tzParsed.parsed) {
            await sendMessage(
                chatId,
                "Бачу code-блок, але не зміг розпарсити ТЗ. Надішли ще раз у форматі ```tz ...```, перевір відступи і ключі."
            );

            return { captured: true, mode: "tz_invalid" };
        }

        const analysis = analyzeArchitecture(tzParsed.tz);
        const questionsQueue = buildQuestionQueue(tzParsed.tz, analysis);
        const pendingQuestions = nextQuestionBatch(questionsQueue);

        setDraft(chatId, {
            ...draft,
            mode: "tz_code_block",
            status: "clarifying",
            tz: tzParsed.tz,
            architecture: analysis,
            questionsQueue,
            pendingQuestions,
            answers: {}
        });

        return {
            captured: true,
            mode: "tz_code_block",
            analysis,
            pendingQuestions
        };
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
    if (result.mode === "cashflow_v23") {
        const lines = [
            "✅ Таблиця Cashflow готова",
            "",
            `📁 Папка: ${result.system_folder_name || "Фінансова система"}`,
            `🔗 ${result.folder_url}`
        ];

        const cashflowFile = (result.generated_files || []).find((item) => item.type === "cashflow");
        if (cashflowFile) {
            lines.push("");
            lines.push(`📊 ${cashflowFile.title}`);
            lines.push(`🔗 ${cashflowFile.spreadsheet_url}`);
        }

        lines.push("");
        lines.push("Що побудовано:");
        for (const item of result.built_summary || []) {
            lines.push(`✓ ${item}`);
        }

        lines.push("");
        lines.push("Наступні кроки:");
        lines.push("1. Відкрий файл і перевір, що статті відображаються правильно");
        lines.push("2. Внеси тестову операцію і перевір зведений аркуш");
        lines.push("3. Якщо є люди без доступу до Sheets, передай їм інструкцію по вводу");
        lines.push("");
        lines.push("➡️ Наступний урок: 2.4 — Платіжний календар");

        return lines.join("\n");
    }

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
        "Надішли ТЗ у code-блоці з тегом tz (```tz ... ```), і я підготую архітектуру та уточнення.",
        "Після уточнень згенерую таблицю автоматично.",
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
        const existingDraft = chatId ? getDraft(chatId) : null;
        if (existingDraft?.status === "clarifying" && existingDraft?.pendingQuestions?.length) {
            const updated = applyAnswers(existingDraft, message.text || "");
            const nextDraft = {
                ...existingDraft,
                ...updated,
                status: updated.pendingQuestions.length > 0 ? "clarifying" : "ready_to_build"
            };

            setDraft(chatId, nextDraft);

            if (nextDraft.pendingQuestions.length > 0) {
                await sendMessage(
                    message.chat.id,
                    nextDraft.pendingQuestions.map((question, index) => `${index + 1}. ${question.text}`).join("\n")
                );

                return { handled: true, command: "clarify_answers" };
            }

            const payload = buildPayloadFromTzDraft(nextDraft, message);
            setDraft(chatId, {
                ...nextDraft,
                status: "ready",
                mode: "tz_code_block",
                payload
            });

            await sendMessage(message.chat.id, "Дякую, все зібрав. Запускаю побудову таблиці.");
            return runBuildAndReply(message, "tz_clarified_auto_build");
        }

        const captured = await captureUserTzMessage(message);
        if (captured?.captured) {
            if (captured.mode === "tz_code_block") {
                await sendMessage(message.chat.id, buildArchitectureMessage(captured.analysis));
                if (captured.pendingQuestions?.length) {
                    await sendMessage(
                        message.chat.id,
                        captured.pendingQuestions.map((question, index) => `${index + 1}. ${question.text}`).join("\n")
                    );
                }

                return { handled: true, command: "tz_clarification_started" };
            }

            if (captured.mode === "tz_invalid") {
                return { handled: true, command: "tz_invalid" };
            }

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
