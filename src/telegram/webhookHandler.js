const { buildReports } = require("../google/reportBuilder");
const { buildTableViaAppsScript, updateTableViaAppsScript, listTablesViaAppsScript, validateTableViaAppsScript } = require("../google/appsScriptClient");
const { sendMessage, sendPhoto } = require("./bot");
const { parseTzFromTelegramMessage, analyzeArchitecture } = require("./tzParser");
const {
    isEnabled: isLlmEnabled,
    getConfigSummary,
    generateClarificationBundle,
    generateUpdatePayloadFromText,
    generateTzFromFreeText,
    generateBusinessNameFromText
} = require("../ai/agentBrain");

const DRAFTS = new Map();
const MAX_TOTAL_QUESTIONS = 15;

const STAGES = {
    IDLE: "idle",
    COLLECTING: "collecting",
    CONFIRMING: "confirming",
    BUILDING: "building",
    EDITING: "editing"
};

function getDraft(chatId) {
    return DRAFTS.get(chatId) || {
        stage: STAGES.IDLE,
        report_type: null,
        raw_input: "",
        extracted: {},
        clarifications: [],
        payload: null,
        spreadsheet_id: null,
        history: [],
        answers: {},
        questionsQueue: [],
        pendingQuestions: [],
        lastPayload: null,
        legacyFallbackUsed: false,
        activeTableId: null,
        activeTableName: null,
        askedQuestionsCount: 0,
        businessNameResolved: null,
        updatedAt: new Date().toISOString()
    };
}

function setDraft(chatId, draft) {
    DRAFTS.set(chatId, {
        ...draft,
        updatedAt: new Date().toISOString()
    });
}

function clearDraft(chatId) {
    const current = getDraft(chatId);
    DRAFTS.set(chatId, {
        stage: STAGES.IDLE,
        report_type: null,
        raw_input: "",
        extracted: {},
        clarifications: [],
        payload: null,
        spreadsheet_id: null,
        history: [],
        answers: {},
        questionsQueue: [],
        pendingQuestions: [],
        lastPayload: null,
        legacyFallbackUsed: false,
        activeTableId: null,
        activeTableName: null,
        askedQuestionsCount: 0,
        businessNameResolved: null,
        updatedAt: new Date().toISOString()
    });
}

function extractMessage(update) {
    if (update.message) return update.message;
    if (update.callback_query?.message) return update.callback_query.message;
    return null;
}

function extractCommand(text) {
    if (!text || !text.startsWith("/")) return "";
    return text.trim().split(/\s+/)[0].toLowerCase();
}

function extractCommandArg(text) {
    const parts = String(text || "").trim().split(/\s+/);
    return parts.length > 1 ? parts.slice(1).join(" ") : "";
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

function normalizeText(value) {
    return String(value || "").trim();
}

function splitAnswers(text) {
    return String(text || "")
        .split(/\r?\n/)
        .map((line) => line.replace(/^\d+[\).]\s*/, "").trim())
        .filter(Boolean);
}

function asYesNo(value) {
    const text = normalizeText(value).toLowerCase();
    return ["yes", "y", "true", "1", "так", "да"].includes(text);
}

function asNo(value) {
    const text = normalizeText(value).toLowerCase();
    return ["no", "n", "false", "0", "ні"].includes(text);
}

function isConfirmBuildText(value) {
    return asYesNo(value) || /^(будуємо|підтверджую|ок|запускай|build|confirm|ok|go)$/i.test(normalizeText(value));
}

function isRejectBuildText(value) {
    const text = normalizeText(value);
    return asNo(text) || /change|edit|do not build|змінити|не будувати/i.test(text);
}

function nextQuestionBatch(queue = [], size = 3) {
    return queue.slice(0, size);
}

function normalizeAnswerValue(value, fallback) {
    const text = normalizeText(value);
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

function tryParseJson(text) {
    const trimmed = normalizeText(text);
    if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return null;

    try {
        return JSON.parse(trimmed);
    } catch {
        return null;
    }
}

function buildArchitectureMessage(analysis) {
    return [
        "Проаналізував ТЗ.",
        `Загальна кількість операцій: ${analysis.totalOps}/міс`,
        analysis.noAccessPeople.length > 0
            ? `Без доступу до Sheets: ${analysis.noAccessPeople.map((i) => `${i.name} (${i.ops})`).join(", ")}`
            : "Без доступу до Sheets: немає",
        `Надходження: архітектура ${analysis.inflowsMode}`,
        `Витрати: архітектура ${analysis.outflowsMode}`
    ].join("\n");
}

function hasAtLeastOneArticle(tz) {
    const inflows = Array.isArray(tz?.inflows) ? tz.inflows : [];
    const outflows = Array.isArray(tz?.outflows) ? tz.outflows : [];
    return inflows.length + outflows.length > 0;
}

function normalizeReportType(value) {
    const text = normalizeText(value).toLowerCase();
    const allowed = ["cashflow", "pl", "balance", "dashboard"];
    return allowed.includes(text) ? text : "";
}

function findNoAccessItemsWithoutMode(tz) {
    const all = [...(Array.isArray(tz?.inflows) ? tz.inflows : []), ...(Array.isArray(tz?.outflows) ? tz.outflows : [])];
    return all
        .filter((item) => item && item.has_sheets_access === false)
        .filter((item) => !normalizeText(item.input_mode));
}

function buildQuestionQueue(tz, _analysis, askedQuestionsCount = 0) {
    const questions = [];

    if (!normalizeReportType(tz.report_type)) {
        questions.push({
            key: "report_type",
            text: "Вкажи тип таблиці: cashflow / pl / balance / dashboard"
        });
    }

    if (!hasAtLeastOneArticle(tz)) {
        questions.push({
            key: "articles_seed",
            text: "Дай мінімум 1-2 статті (формат: Надходження: ..., Витрати: ...)"
        });
    }

    const unresolvedNoAccess = findNoAccessItemsWithoutMode(tz);
    unresolvedNoAccess.forEach((item, index) => {
        const person = normalizeText(item.responsible || item.owner || "Співробітник");
        const article = normalizeText(item.article || item.name || "Стаття");
        questions.push({
            key: `money_flow_${index}`,
            text: `${article} — ${person}: сам платить (підзвіт) чи заявка через бухгалтера?`
        });
        questions.push({
            key: `no_access_method_${index}`,
            text: `${article} — якщо підзвіт, що зручніше: Google Form чи окремий аркуш?`
        });
    });

    const budgetLeft = Math.max(0, MAX_TOTAL_QUESTIONS - Number(askedQuestionsCount || 0));
    return questions.slice(0, budgetLeft);
}

function buildResponsibleMap(tz, answers = {}) {
    const result = {};
    let noAccessIndex = 0;

    const addItem = (item) => {
        const article = normalizeText(item.article || item.name);
        if (!article) return;

        const person = normalizeText(item.responsible || item.owner || "Owner");
        const hasAccess = item.has_sheets_access !== false;
        let inputMode = hasAccess ? "direct" : "sheet";
        let payment = hasAccess ? "centralized" : "accountable";

        if (!hasAccess) {
            const flowDecision = normalizeText(answers[`money_flow_${noAccessIndex}`]).toLowerCase();
            const methodDecision = normalizeText(answers[`no_access_method_${noAccessIndex}`]).toLowerCase();

            if (flowDecision.includes("бух") || flowDecision.includes("accountant") || flowDecision.includes("через")) {
                inputMode = "direct";
                payment = "centralized";
            } else if (methodDecision.includes("form") || methodDecision.includes("гугл форм")) {
                inputMode = "form";
                payment = "accountable";
            } else {
                inputMode = "sheet";
                payment = "accountable";
            }

            noAccessIndex += 1;
        }

        result[article] = {
            name: person,
            access: hasAccess,
            input_mode: inputMode,
            payment
        };
    };

    (Array.isArray(tz.inflows) ? tz.inflows : []).forEach(addItem);
    (Array.isArray(tz.outflows) ? tz.outflows : []).forEach(addItem);

    return result;
}

function buildPayloadFromTzDraft(draft, message) {
    const identity = getTelegramIdentity(message);
    const tz = draft.extracted || {};
    const answers = draft.answers || {};

    const inflows = (Array.isArray(tz.inflows) ? tz.inflows : [])
        .map((item) => normalizeText(item.article || item.name))
        .filter(Boolean);
    const outflows = (Array.isArray(tz.outflows) ? tz.outflows : [])
        .map((item) => normalizeText(item.article || item.name))
        .filter(Boolean);

    return {
        action: "build_table",
        report_type: normalizeReportType(answers.report_type || tz.report_type) || "cashflow",
        business_name: normalizeText(draft.businessNameResolved || tz.business_name || identity.telegram_username || "Business"),
        language: "uk",
        telegram_id: identity.telegram_id,
        telegram_username: identity.telegram_username,
        raw_input: draft.raw_input,
        architecture: {
            inflows: draft.analysis?.inflowsMode || "A",
            outflows: draft.analysis?.outflowsMode || "A"
        },
        articles: { inflows, outflows },
        responsible: buildResponsibleMap(tz, answers),
        options: {
            payment_calendar: false,
            multi_account: false,
            counterparty_tracking: false,
            formatting: true
        }
    };
}

function inferSheetsFromPayload(payload) {
    const sheets = [];
    if (payload.report_type === "cashflow") sheets.push("Cashflow", "Надходження", "Витрати");
    sheets.push("Довідники", "Налаштування", "References");
    if (payload.options?.payment_calendar) sheets.push("Платіжний календар");

    const byPerson = new Set();
    Object.values(payload.responsible || {}).forEach((item) => {
        if (item.input_mode === "sheet") byPerson.add(`Витрати - ${item.name}`);
        if (item.input_mode === "form") sheets.push("Лог");
    });

    byPerson.forEach((name) => sheets.push(name));
    return Array.from(new Set(sheets));
}

function buildConfirmationMessage(payload) {
    return [
        "Готовий будувати.",
        `Файл: ${payload.report_type}_${payload.business_name}_${new Date().getFullYear()}`,
        `Аркуші: ${inferSheetsFromPayload(payload).join(", ")}`,
        `Надходження: ${(payload.articles?.inflows || []).join(", ") || "-"}`,
        `Витрати: ${(payload.articles?.outflows || []).join(", ") || "-"}`,
        "Будуємо? (так/ні/змінити)"
    ].join("\n");
}

function buildTableEntry(file, payload) {
    if (!file) return null;

    const id = file.spreadsheet_id || file.id || null;
    if (!id) return null;

    return {
        spreadsheet_id: id,
        name: file.name || file.title || `Table_${id.slice(0, 6)}`,
        url: file.url || file.spreadsheet_url || "",
        report_type: payload?.report_type || "unknown",
        created_at: new Date().toISOString()
    };
}

function resolveActiveTable(draft) {
    const activeId = draft.activeTableId || draft.spreadsheet_id || null;
    if (!activeId) return null;
    return {
        spreadsheet_id: activeId,
        name: draft.activeTableName || "(unknown)"
    };
}

async function loadTablesForUser(message) {
    const identity = getTelegramIdentity(message);
    const response = await listTablesViaAppsScript({
        telegram_id: identity.telegram_id,
        telegram_username: identity.telegram_username
    });

    return {
        folderUrl: response.folder_url || "",
        folderExists: response.folder_exists !== false,
        clientFolder: response.client_folder || "",
        tables: Array.isArray(response.tables) ? response.tables : []
    };
}

function formatTablesList(tablesInfo, draft) {
    const tables = tablesInfo.tables || [];
    if (tables.length === 0) {
        const folderLine = tablesInfo.clientFolder ? `Папка: ${tablesInfo.clientFolder}` : "";
        return ["У папці клієнта поки немає таблиць.", folderLine].filter(Boolean).join("\n");
    }

    const activeId = draft.activeTableId || draft.spreadsheet_id || null;
    const lines = ["Список таблиць:"];
    if (tablesInfo.clientFolder) lines.push(`Папка клієнта: ${tablesInfo.clientFolder}`);
    if (tablesInfo.folderUrl) lines.push(`Folder URL: ${tablesInfo.folderUrl}`);
    lines.push("");

    tables.forEach((item, index) => {
        const mark = item.spreadsheet_id === activeId ? " [ACTIVE]" : "";
        lines.push(`${index + 1}. ${item.name}${mark}`);
        lines.push(`   id: ${item.spreadsheet_id}`);
        if (item.url) lines.push(`   url: ${item.url}`);
    });

    lines.push("Використай /use <номер або spreadsheet_id> для перемикання.");
    return lines.join("\n");
}

function selectTableFromArg(tables, argRaw) {
    const arg = normalizeText(argRaw);
    if (!arg) return null;

    const byId = tables.find((item) => item.spreadsheet_id === arg);
    if (byId) return byId;

    const asNumber = Number(arg);
    if (Number.isInteger(asNumber) && asNumber >= 1 && asNumber <= tables.length) {
        return tables[asNumber - 1];
    }

    const lower = arg.toLowerCase();
    return tables.find((item) => String(item.name || "").toLowerCase().includes(lower)) || null;
}

function formatAppsScriptResult(result, payload, draft) {
    const files = Array.isArray(result.files) ? result.files : [];
    const forms = Array.isArray(result.forms) ? result.forms : [];
    const firstFile = files[0] || {};
    const lines = ["Таблиця готова", ""];

    if (firstFile.name) lines.push(`Файл: ${firstFile.name}`);
    if (result.folder_url) lines.push(`Папка: ${result.folder_url}`);
    if (firstFile.url) lines.push(`Посилання на файл: ${firstFile.url}`);

    if (Array.isArray(result.sheets_built) && result.sheets_built.length > 0) {
        lines.push("", "Що побудовано:");
        result.sheets_built.forEach((name) => lines.push(`- ${name}`));
    }

    if (forms.length > 0) {
        lines.push("");
        forms.forEach((form) => lines.push(`Google Form ${form.name}: ${form.url}`));
    }

    const active = resolveActiveTable(draft);
    if (active) {
        lines.push("", `Активна таблиця: ${active.name}`);
        lines.push(`Active ID: ${active.spreadsheet_id}`);
    }

    lines.push("", "Наступні кроки:");
    lines.push("1. Відкрий файл і перевір структуру");
    lines.push("2. Додай одну тестову операцію");
    if (payload.options?.counterparty_tracking) lines.push("3. Перевір дропдаун контрагентів");
    lines.push("", "Для правок напиши текст або надішли update_table JSON.");
    lines.push("Для списку таблиць: /tables");

    return lines.join("\n");
}

function formatLegacyResult(result, draft) {
    const lines = ["Побудовано через legacy fallback.", ""];
    if (result.folder_url) lines.push(`Папка: ${result.folder_url}`);
    if (Array.isArray(result.generated_files)) {
        result.generated_files.forEach((f) => lines.push(`${f.title}: ${f.spreadsheet_url}`));
    }

    const active = resolveActiveTable(draft);
    if (active) {
        lines.push("", `Активна таблиця: ${active.name}`);
        lines.push(`Active ID: ${active.spreadsheet_id}`);
    }

    lines.push("", "Налаштуй APPS_SCRIPT_URL, щоб основним був Apps Script.");
    return lines.join("\n");
}

function formatBuildError(error) {
    return [
        "Не вдалося побудувати.",
        "Спробуй ще раз через /retry.",
        `Причина: ${normalizeText(error?.message || "unknown")}`
    ].join("\n");
}

function hasBrokenFormulaError(validation) {
    const errors = Array.isArray(validation?.errors) ? validation.errors : [];
    return errors.some((item) => /зламані формули|#ref|#error|#name/i.test(String(item || "")));
}

function buildWelcomeMessage() {
    const llm = getConfigSummary();
    const llmLine = llm.enabled
        ? `AI: ${llm.provider}/${llm.model}`
        : "AI: off (set ANTHROPIC_API_KEY or OPENROUTER_API_KEY)";

    return [
        "Привіт. Цей бот будує фінансові таблиці через Apps Script.",
        "Надішли ТЗ у code-блоці з тегом tz або просто текстом.",
        "Команди: /clear /retry /status /tables /use /new",
        llmLine
    ].join("\n\n");
}

function getBrandPhotoUrl() {
    const appBaseUrl = process.env.APP_BASE_URL;
    if (!appBaseUrl) return "";
    return `${appBaseUrl.replace(/\/$/, "")}/brand-photo`;
}

function buildStatusMessage(draft) {
    const llm = getConfigSummary();
    const active = resolveActiveTable(draft);

    return [
        `stage: ${draft.stage}`,
        `report_type: ${draft.report_type || "unknown"}`,
        `questions_left: ${Array.isArray(draft.questionsQueue) ? draft.questionsQueue.length : 0}`,
        `questions_asked: ${Number(draft.askedQuestionsCount || 0)} / ${MAX_TOTAL_QUESTIONS}`,
        `has_payload: ${draft.payload ? "yes" : "no"}`,
        `active_table_id: ${active?.spreadsheet_id || "-"}`,
        `active_table_name: ${draft.activeTableName || "-"}`,
        `legacy_fallback_used: ${draft.legacyFallbackUsed ? "yes" : "no"}`,
        `ai_enabled: ${llm.enabled ? "yes" : "no"}`,
        `ai_provider: ${llm.provider}`,
        `ai_model: ${llm.model}`
    ].join("\n");
}

async function executeBuild(payload) {
    try {
        const result = await buildTableViaAppsScript(payload);
        return { engine: "apps_script", result };
    } catch (appsScriptError) {
        const legacyResult = await buildReports(payload);
        return { engine: "legacy", result: legacyResult, fallbackReason: appsScriptError.message };
    }
}

async function runBuildAndReply(message, draft, payload, commandLabel) {
    const chatId = message.chat.id;
    setDraft(chatId, { ...draft, stage: STAGES.BUILDING, lastPayload: payload });
    await sendMessage(chatId, "Будую таблицю, це займе 30-60 секунд...");
    await sendMessage(chatId, "Планую структуру таблиці і готую аркуші...");

    try {
        const build = await executeBuild(payload);
        const nowDraft = getDraft(chatId);

        if (build.engine === "apps_script") {
            const files = Array.isArray(build.result.files) ? build.result.files : [];
            const entries = files.map((file) => buildTableEntry(file, payload)).filter(Boolean);
            const activeId = entries[0]?.spreadsheet_id || nowDraft.activeTableId || null;
            const activeName = entries[0]?.name || nowDraft.activeTableName || null;

            const updatedDraft = {
                ...nowDraft,
                stage: STAGES.EDITING,
                payload,
                spreadsheet_id: activeId,
                activeTableId: activeId,
                activeTableName: activeName,
                legacyFallbackUsed: false
            };
            setDraft(chatId, updatedDraft);

            await sendMessage(chatId, "Перевіряю формули і цілісність файлу...");

            let validationResult = null;
            if (activeId) {
                try {
                    validationResult = await validateTableViaAppsScript({ spreadsheet_id: activeId });
                } catch (error) {
                    const message = String(error?.message || "");
                    const missingValidateAction = /unknown action:\s*validate_table/i.test(message);
                    if (missingValidateAction) {
                        validationResult = {
                            valid: true,
                            errors: [],
                            warnings: ["Валідація пропущена: у поточному Apps Script ще немає action validate_table"]
                        };
                    } else {
                        validationResult = {
                            valid: false,
                            errors: [`Помилка validate_table: ${message}`],
                            warnings: []
                        };
                    }
                }
            }

            if (validationResult && validationResult.valid === false) {
                if (activeId && hasBrokenFormulaError(validationResult)) {
                    await sendMessage(chatId, "Знайшов технічний збій у формулах, виправляю автоматично...");
                    try {
                        await updateTableViaAppsScript({
                            action: "update_table",
                            spreadsheet_id: activeId,
                            changes: [{ type: "repair_formulas" }]
                        });
                        validationResult = await validateTableViaAppsScript({ spreadsheet_id: activeId });
                    } catch {
                        // If repair fails, we keep generic fallback below.
                    }

                    if (validationResult && validationResult.valid) {
                        await sendMessage(chatId, "Формули виправлено. Завершую налаштування...");
                    }
                }

                if (validationResult && validationResult.valid === false) {
                    await sendMessage(chatId, "Є технічна проблема під час фінальної перевірки. Я вже зберіг таблицю, але потрібно повторити /retry для автоматичної доводки.");
                    return { handled: true, command: commandLabel, engine: "apps_script", result: build.result, validation: validationResult };
                }
            }

            const firstFile = build.result.files?.[0] || {};
            let msg = "Таблиця готова\n\n";
            if (firstFile.name) msg += `${firstFile.name}\n`;
            if (firstFile.url) msg += `${firstFile.url}\n`;

            if (Array.isArray(build.result.forms) && build.result.forms.length > 0) {
                build.result.forms.forEach((form) => {
                    msg += `\nФорма для ${form.name}:\n${form.url}`;
                });
            }

            if (validationResult?.warnings?.length) {
                msg += "\n\nПопередження:";
                validationResult.warnings.forEach((item) => {
                    msg += `\n⚠️ ${item}`;
                });
            }

            msg += "\n\nЯкщо треба щось змінити — просто напиши.";
            await sendMessage(chatId, msg);
            return { handled: true, command: commandLabel, engine: "apps_script", result: build.result };
        }

        const legacyFiles = Array.isArray(build.result.generated_files) ? build.result.generated_files : [];
        const legacyEntries = legacyFiles
            .map((file) => buildTableEntry({
                spreadsheet_id: file.spreadsheet_id || file.id || null,
                name: file.title || file.name,
                url: file.spreadsheet_url || file.url
            }, payload))
            .filter(Boolean);

        const activeId = legacyEntries[0]?.spreadsheet_id || nowDraft.activeTableId || null;
        const activeName = legacyEntries[0]?.name || nowDraft.activeTableName || null;

        const updatedDraft = {
            ...nowDraft,
            stage: STAGES.EDITING,
            payload,
            spreadsheet_id: activeId,
            activeTableId: activeId,
            activeTableName: activeName,
            legacyFallbackUsed: true
        };
        setDraft(chatId, updatedDraft);

        await sendMessage(chatId, formatLegacyResult(build.result, updatedDraft));
        return { handled: true, command: commandLabel, engine: "legacy", result: build.result };
    } catch (error) {
        setDraft(chatId, { ...getDraft(chatId), stage: STAGES.CONFIRMING, lastPayload: payload });
        await sendMessage(chatId, formatBuildError(error));
        return { handled: true, command: commandLabel, error: error.message };
    }
}

async function buildClarificationWithAi(chatId, tz, analysis, defaultQuestions) {
    if (!isLlmEnabled()) {
        return {
            message: buildArchitectureMessage(analysis),
            questions: defaultQuestions
        };
    }

    try {
        const questionBudget = Math.max(0, MAX_TOTAL_QUESTIONS - Number(getDraft(chatId).askedQuestionsCount || 0));
        const ai = await generateClarificationBundle({ tz, analysis, defaultQuestions, questionBudget });
        const limitedQuestions = (ai.questions.length > 0 ? ai.questions : defaultQuestions).slice(0, questionBudget);
        return {
            message: ai.message || buildArchitectureMessage(analysis),
            questions: limitedQuestions
        };
    } catch (error) {
        await sendMessage(chatId, `AI тимчасово недоступний, працюю без нього: ${error.message}`);
        const questionBudget = Math.max(0, MAX_TOTAL_QUESTIONS - Number(getDraft(chatId).askedQuestionsCount || 0));
        return {
            message: buildArchitectureMessage(analysis),
            questions: defaultQuestions.slice(0, questionBudget)
        };
    }
}

async function parseTzFromAnyText(message, chatId) {
    const parsed = parseTzFromTelegramMessage(message);
    if (parsed.detected && parsed.parsed) {
        return parsed.tz;
    }

    if (!isLlmEnabled()) {
        return null;
    }

    try {
        return await generateTzFromFreeText(message.text || "");
    } catch (error) {
        await sendMessage(chatId, `Не зміг розпізнати ТЗ навіть через AI: ${error.message}`);
        return null;
    }
}

async function resolveBusinessName(tz, rawText, message) {
    const fromTz = normalizeText(tz?.business_name);
    if (fromTz) return fromTz;

    if (isLlmEnabled()) {
        try {
            const aiName = normalizeText(await generateBusinessNameFromText(rawText || ""));
            if (aiName) return aiName;
        } catch {
            // fallback to telegram username below
        }
    }

    return normalizeText(message?.from?.username || "Business");
}

function applyCriticalAnswerSideEffects(draft, updatedAnswers) {
    const extracted = { ...(draft.extracted || {}) };
    const answers = updatedAnswers || {};

    const answeredType = normalizeReportType(answers.report_type);
    if (answeredType) {
        extracted.report_type = answeredType;
    }

    if (!hasAtLeastOneArticle(extracted) && normalizeText(answers.articles_seed)) {
        const seed = answers.articles_seed;
        const inflowMatch = String(seed).match(/надходження\s*:\s*([^\n]+)/i);
        const outflowMatch = String(seed).match(/витрати\s*:\s*([^\n]+)/i);
        const parseList = (value) => String(value || "")
            .split(/[;,]/)
            .map((v) => normalizeText(v))
            .filter(Boolean)
            .map((article) => ({ article, responsible: "Owner", ops_per_month: 10, has_sheets_access: true }));

        const inflows = parseList(inflowMatch?.[1]);
        const outflows = parseList(outflowMatch?.[1]);
        if (inflows.length || outflows.length) {
            extracted.inflows = inflows;
            extracted.outflows = outflows;
        }
    }

    return extracted;
}

function ensureBuildMinimum(tz) {
    const next = { ...(tz || {}) };
    if (!normalizeReportType(next.report_type)) {
        next.report_type = "cashflow";
    }

    const inflows = Array.isArray(next.inflows) ? next.inflows : [];
    const outflows = Array.isArray(next.outflows) ? next.outflows : [];
    if (inflows.length + outflows.length === 0) {
        next.outflows = [{ article: "Інші витрати", responsible: "Owner", ops_per_month: 10, has_sheets_access: true }];
    }

    return next;
}

async function handleTzCapture(message, draft) {
    const chatId = message.chat.id;
    const text = normalizeText(message.text);

    const parsedJson = tryParseJson(text);
    if (parsedJson && typeof parsedJson === "object") {
        const payload = { action: String(parsedJson.action || "build_table").toLowerCase(), ...parsedJson };

        if (payload.action === "update_table") {
            const response = await updateTableViaAppsScript(payload);
            await sendMessage(chatId, `Оновлення застосовано: ${JSON.stringify(response)}`);
            return { handled: true, command: "json_update_payload" };
        }

        setDraft(chatId, {
            ...draft,
            stage: STAGES.CONFIRMING,
            payload,
            lastPayload: payload,
            report_type: payload.report_type || draft.report_type
        });
        await sendMessage(chatId, buildConfirmationMessage(payload));
        return { handled: true, command: "json_payload_confirm" };
    }

    const tz = await parseTzFromAnyText(message, chatId);
    if (!tz) {
        await sendMessage(chatId, "Надішли ТЗ у tz code block, JSON або просто текст з описом процесу.");
        return { handled: true, command: "awaiting_tz" };
    }

    const analysis = analyzeArchitecture(tz);
    const businessNameResolved = await resolveBusinessName(tz, text, message);
    const questionsQueue = buildQuestionQueue(tz, analysis, draft.askedQuestionsCount);
    const aiBundle = await buildClarificationWithAi(chatId, tz, analysis, questionsQueue);
    const pendingQuestions = nextQuestionBatch(aiBundle.questions);

    if (aiBundle.questions.length === 0) {
        const readyTz = ensureBuildMinimum(tz);
        const directDraft = {
            ...draft,
            stage: STAGES.CONFIRMING,
            report_type: normalizeReportType(readyTz.report_type) || "cashflow",
            raw_input: text,
            extracted: readyTz,
            analysis: analyzeArchitecture(readyTz),
            businessNameResolved,
            answers: {},
            questionsQueue: [],
            pendingQuestions: []
        };
        const payload = buildPayloadFromTzDraft(directDraft, message);
        setDraft(chatId, { ...directDraft, payload, lastPayload: payload });
        await sendMessage(chatId, "Критичних уточнень немає або ліміт питань вичерпано. Переходимо до побудови.");
        await sendMessage(chatId, buildConfirmationMessage(payload));
        return { handled: true, command: "ready_without_questions" };
    }

    setDraft(chatId, {
        ...draft,
        stage: STAGES.COLLECTING,
        report_type: String(tz.report_type || "cashflow").toLowerCase(),
        raw_input: text,
        extracted: tz,
        analysis,
        businessNameResolved,
        questionsQueue: aiBundle.questions,
        pendingQuestions,
        askedQuestionsCount: Number(draft.askedQuestionsCount || 0) + pendingQuestions.length,
        answers: {},
        payload: null,
        history: [...(draft.history || []), { role: "user", content: text }]
    });

    await sendMessage(chatId, aiBundle.message || buildArchitectureMessage(analysis));
    if (pendingQuestions.length > 0) {
        await sendMessage(chatId, pendingQuestions.map((q, i) => `${i + 1}. ${q.text}`).join("\n"));
    }

    return { handled: true, command: "tz_clarification_started" };
}

async function handleCollectingAnswers(message, draft) {
    const chatId = message.chat.id;
    const updated = applyAnswers(draft, message.text || "");
    const extractedAfterAnswers = applyCriticalAnswerSideEffects(draft, updated.answers);

    const nextDraft = {
        ...draft,
        ...updated,
        stage: updated.pendingQuestions.length > 0 ? STAGES.COLLECTING : STAGES.CONFIRMING,
        extracted: extractedAfterAnswers,
        report_type: normalizeReportType(extractedAfterAnswers.report_type) || draft.report_type,
        askedQuestionsCount: Number(draft.askedQuestionsCount || 0) + updated.pendingQuestions.length,
        clarifications: [...(draft.clarifications || []), normalizeText(message.text)]
    };

    if (updated.pendingQuestions.length > 0) {
        setDraft(chatId, nextDraft);
        await sendMessage(chatId, updated.pendingQuestions.map((q, i) => `${i + 1}. ${q.text}`).join("\n"));
        return { handled: true, command: "clarify_answers" };
    }

    const readyDraft = {
        ...nextDraft,
        extracted: ensureBuildMinimum(nextDraft.extracted)
    };
    const finalPayload = buildPayloadFromTzDraft(readyDraft, message);
    setDraft(chatId, { ...readyDraft, payload: finalPayload, lastPayload: finalPayload });
    await sendMessage(chatId, buildConfirmationMessage(finalPayload));
    return { handled: true, command: "ready_for_confirmation" };
}

async function handleConfirmation(message, draft) {
    const chatId = message.chat.id;
    const text = normalizeText(message.text);

    if (!draft.payload) {
        await sendMessage(chatId, "Немає готового payload. Надішли ТЗ знову.");
        return { handled: true, command: "confirm_without_payload" };
    }

    if (isConfirmBuildText(text)) return runBuildAndReply(message, draft, draft.payload, "confirmed_build");

    if (isRejectBuildText(text)) {
        setDraft(chatId, { ...draft, stage: STAGES.COLLECTING, pendingQuestions: [], questionsQueue: [] });
        await sendMessage(chatId, "Напиши зміни і я оновлю payload перед побудовою.");
        return { handled: true, command: "confirmation_rejected" };
    }

    await sendMessage(chatId, "Відповідай: так / ні / змінити.");
    return { handled: true, command: "confirmation_reask" };
}

function buildUpdatePayloadFromTextFallback(text, draft) {
    const active = resolveActiveTable(draft);
    const spreadsheetId = draft.spreadsheet_id || active?.spreadsheet_id || draft.activeTableId || null;
    if (!spreadsheetId) return null;

    const normalized = normalizeText(text);
    if (!normalized) return null;

    return {
        action: "update_table",
        spreadsheet_id: spreadsheetId,
        changes: [{ type: "add_article", section: "outflows", article: normalized }]
    };
}

async function buildUpdatePayloadWithAi(chatId, text, draft) {
    const active = resolveActiveTable(draft);
    const spreadsheetId = draft.spreadsheet_id || active?.spreadsheet_id || draft.activeTableId || null;

    if (!isLlmEnabled() || !spreadsheetId) {
        return {
            missing: [],
            message_to_user: "",
            update_payload: buildUpdatePayloadFromTextFallback(text, { ...draft, spreadsheet_id: spreadsheetId })
        };
    }

    try {
        return await generateUpdatePayloadFromText({
            spreadsheet_id: spreadsheetId,
            current_payload: draft.payload,
            user_message: text
        });
    } catch (error) {
        await sendMessage(chatId, `AI редагування недоступне, використовую fallback: ${error.message}`);
        return {
            missing: [],
            message_to_user: "",
            update_payload: buildUpdatePayloadFromTextFallback(text, { ...draft, spreadsheet_id: spreadsheetId })
        };
    }
}

async function handleEditingMode(message, draft) {
    const chatId = message.chat.id;
    const text = normalizeText(message.text);
    const json = tryParseJson(text);

    if (json && typeof json === "object") {
        const payload = { action: json.action || "update_table", ...json };
        if (String(payload.action).toLowerCase() !== "update_table") {
            await sendMessage(chatId, "У режимі правок підтримується тільки action=update_table.");
            return { handled: true, command: "editing_wrong_action" };
        }

        const result = await updateTableViaAppsScript(payload);
        await sendMessage(chatId, `Правку застосовано: ${JSON.stringify(result)}`);
        return { handled: true, command: "editing_json_update" };
    }

    const aiResult = await buildUpdatePayloadWithAi(chatId, text, draft);
    if (Array.isArray(aiResult.missing) && aiResult.missing.length > 0) {
        await sendMessage(chatId, aiResult.message_to_user || `Потрібні уточнення: ${aiResult.missing.join(", ")}`);
        return { handled: true, command: "editing_need_more_data" };
    }

    if (!aiResult.update_payload) {
        await sendMessage(chatId, "Надішли JSON update_table або сформулюй правку точніше.");
        return { handled: true, command: "editing_no_payload" };
    }

    const result = await updateTableViaAppsScript(aiResult.update_payload);
    await sendMessage(chatId, `Правку застосовано: ${JSON.stringify(result)}`);
    return { handled: true, command: "editing_auto_update" };
}

async function handleUseCommand(message, draft, argRaw) {
    const chatId = message.chat.id;
    let tablesInfo;

    try {
        tablesInfo = await loadTablesForUser(message);
    } catch (error) {
        await sendMessage(chatId, `Не вдалося отримати список таблиць: ${error.message}`);
        return { handled: true, command: "use_load_error" };
    }

    const selected = selectTableFromArg(tablesInfo.tables, argRaw);

    if (!selected) {
        await sendMessage(chatId, "Не знайшов таблицю. Використай /tables і потім /use <номер або spreadsheet_id>.");
        return { handled: true, command: "use_not_found" };
    }

    setDraft(chatId, {
        ...draft,
        activeTableId: selected.spreadsheet_id,
        spreadsheet_id: selected.spreadsheet_id,
        activeTableName: selected.name || null,
        stage: STAGES.EDITING
    });

    await sendMessage(chatId, `Активна таблиця: ${selected.name}\nID: ${selected.spreadsheet_id}`);
    return { handled: true, command: "use_table" };
}

async function handleNewCommand(message, draft) {
    const chatId = message.chat.id;

    setDraft(chatId, {
        ...draft,
        stage: STAGES.IDLE,
        report_type: null,
        raw_input: "",
        extracted: {},
        clarifications: [],
        payload: null,
        answers: {},
        questionsQueue: [],
        pendingQuestions: [],
        askedQuestionsCount: 0,
        businessNameResolved: null,
        activeTableId: draft.activeTableId || null,
        activeTableName: draft.activeTableName || null,
        spreadsheet_id: draft.activeTableId || null
    });

    await sendMessage(chatId, "Ок, починаємо нову таблицю. Надішли нове ТЗ (можна звичайним текстом).\nАктивну таблицю для правок можна змінити через /use.");
    return { handled: true, command: "new_flow" };
}

async function handleTelegramUpdate(update) {
    const message = extractMessage(update);
    if (!message || !message.chat) return { handled: false, reason: "No message context" };

    const chatId = getChatId(message);
    if (!chatId) return { handled: false, reason: "No chat id" };

    const text = message.text || "";
    const command = extractCommand(text);
    const commandArg = extractCommandArg(text);
    const draft = getDraft(chatId);

    if (command === "/clear") {
        clearDraft(chatId);
        await sendMessage(chatId, "Стан очищено. Надішли новий ТЗ у tz code block або простим текстом.");
        return { handled: true, command };
    }

    if (command === "/status") {
        await sendMessage(chatId, buildStatusMessage(draft));
        return { handled: true, command };
    }

    if (command === "/tables") {
        try {
            const tablesInfo = await loadTablesForUser(message);
            await sendMessage(chatId, formatTablesList(tablesInfo, draft));
        } catch (error) {
            await sendMessage(chatId, `Не вдалося отримати список таблиць: ${error.message}`);
        }
        return { handled: true, command };
    }

    if (command === "/use") {
        return handleUseCommand(message, draft, commandArg);
    }

    if (command === "/new") {
        return handleNewCommand(message, draft);
    }

    if (command === "/retry") {
        if (!draft.lastPayload) {
            await sendMessage(chatId, "Немає payload для повтору. Надішли ТЗ або JSON.");
            return { handled: true, command };
        }

        return runBuildAndReply(message, draft, draft.lastPayload, "retry");
    }

    if (command === "/start") {
        const welcomeMessage = buildWelcomeMessage();
        const brandPhotoUrl = getBrandPhotoUrl();
        if (brandPhotoUrl) {
            await sendPhoto(chatId, brandPhotoUrl, welcomeMessage);
        } else {
            await sendMessage(chatId, welcomeMessage);
        }
        return { handled: true, command };
    }

    if (draft.stage === STAGES.COLLECTING && ((draft.pendingQuestions?.length || 0) > 0 || (draft.questionsQueue?.length || 0) > 0)) {
        return handleCollectingAnswers(message, draft);
    }

    if (draft.stage === STAGES.CONFIRMING) {
        return handleConfirmation(message, draft);
    }

    if (draft.stage === STAGES.EDITING) {
        return handleEditingMode(message, draft);
    }

    if (!command && resolveActiveTable(draft) && /(додай|видали|перейменуй|change|remove|rename|add)/i.test(text)) {
        return handleEditingMode(message, {
            ...draft,
            stage: STAGES.EDITING,
            spreadsheet_id: resolveActiveTable(draft)?.spreadsheet_id || draft.spreadsheet_id
        });
    }

    return handleTzCapture(message, draft);
}

module.exports = {
    handleTelegramUpdate
};
