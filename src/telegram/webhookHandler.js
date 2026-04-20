const { buildReports } = require("../google/reportBuilder");
const { pingAppsScript, buildTableViaAppsScript, updateTableViaAppsScript, listTablesViaAppsScript, validateTableViaAppsScript } = require("../google/appsScriptClient");
const { sendMessage, sendPhoto } = require("./bot");
const { parseTzFromTelegramMessage, analyzeArchitecture } = require("./tzParser");
const {
    isEnabled: isLlmEnabled,
    getConfigSummary,
    generateClarificationBundle,
    generateUpdatePayloadFromText,
    generateTzFromFreeText,
    generateBusinessNameFromText,
    generateCustomTableBlueprint
} = require("../ai/agentBrain");

const DRAFTS = new Map();
const CHAT_QUEUES = new Map();
const PROCESSED_UPDATES = new Map();
const MAX_TOTAL_QUESTIONS = 15;
const UPDATE_DEDUP_TTL_MS = 10 * 60 * 1000;

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
        aiTemporarilyDisabled: false,
        customMode: false,
        customPlanNotes: "",
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
        aiTemporarilyDisabled: false,
        customMode: false,
        customPlanNotes: "",
        updatedAt: new Date().toISOString()
    });
}

function pruneProcessedUpdates(nowTs) {
    for (const [id, ts] of PROCESSED_UPDATES.entries()) {
        if (nowTs - ts > UPDATE_DEDUP_TTL_MS) {
            PROCESSED_UPDATES.delete(id);
        }
    }
}

function isDuplicateUpdate(update) {
    const id = Number(update?.update_id);
    if (!Number.isInteger(id)) return false;

    const nowTs = Date.now();
    pruneProcessedUpdates(nowTs);

    if (PROCESSED_UPDATES.has(id)) {
        return true;
    }

    PROCESSED_UPDATES.set(id, nowTs);
    return false;
}

async function runWithChatQueue(chatId, work) {
    const previous = CHAT_QUEUES.get(chatId) || Promise.resolve();
    const current = previous
        .catch(() => undefined)
        .then(work);

    CHAT_QUEUES.set(chatId, current);

    try {
        return await current;
    } finally {
        if (CHAT_QUEUES.get(chatId) === current) {
            CHAT_QUEUES.delete(chatId);
        }
    }
}

function shouldUseAi(draft) {
    return isLlmEnabled() && !draft?.aiTemporarilyDisabled;
}

function disableAiForChat(chatId, draft, reason) {
    if (!draft || draft.aiTemporarilyDisabled) return;
    setDraft(chatId, { ...draft, aiTemporarilyDisabled: true });
    console.error("AI disabled for chat due to error", { chatId, reason: String(reason || "unknown") });
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

function joinMessageBlocks(blocks = []) {
    return blocks
        .flat()
        .map((item) => normalizeText(item))
        .filter(Boolean)
        .join("\n\n");
}

function sectionTitle(emoji, text) {
    return `${emoji} ${normalizeText(text)}`;
}

function bulletLines(items = [], marker = "•") {
    return items
        .map((item) => normalizeText(item))
        .filter(Boolean)
        .map((item) => `${marker} ${item}`);
}

function numberedLines(items = []) {
    return items
        .map((item) => normalizeText(item))
        .filter(Boolean)
        .map((item, index) => `${index + 1}. ${item}`);
}

function labelValue(label, value) {
    return `${label}: ${normalizeText(value) || "-"}`;
}

function looksLikeCashflowRequest(text) {
    return /(кешфлоу|кеш\s*флоу|кефлоу|cashflow|cash\s*flow)/i.test(String(text || ""));
}

function looksLikeFinanceNarrative(text) {
    const source = String(text || "");
    if (source.length < 120) return false;
    return /(гроші|оплат|надход|витрат|закуп|зарплат|оренд|логіст|реклам|постачаль|клієнт)/i.test(source);
}

function parseArticlesFromLine(line) {
    const source = String(line || "");
    const parts = source.includes(":") ? source.split(":").slice(1).join(":") : source;
    return parts
        .split(/[;,]/)
        .map((item) => normalizeText(item))
        .filter((item) => item.length > 2)
        .map((item) => item.replace(/^[-–]\s*/, ""));
}

function parseCashflowHeuristicFromText(text) {
    const source = String(text || "");
    if (!looksLikeCashflowRequest(source) && !looksLikeFinanceNarrative(source)) return null;

    const lines = source.split(/\r?\n/).map((line) => normalizeText(line)).filter(Boolean);
    const inflowLine = lines.find((line) => /(гроші\s*приход|надход|приходять|дохід|оплат)/i.test(line)) || "";
    const outflowLine = lines.find((line) => /(витрат|закуп|зарплат|оренд|податк|комунал)/i.test(line)) || "";

    let inflows = parseArticlesFromLine(inflowLine);
    let outflows = parseArticlesFromLine(outflowLine);

    if (inflows.length === 0) inflows = ["Оплата від клієнтів"];
    if (outflows.length === 0) outflows = ["Операційні витрати"];

    const toItems = (arr) => arr.map((article) => ({
        article,
        responsible: "Owner",
        ops_per_month: 20,
        has_sheets_access: true
    }));

    return {
        report_type: "cashflow",
        business_name: "Бізнес",
        inflows: toItems(inflows),
        outflows: toItems(outflows)
    };
}

function spreadsheetUrlById(spreadsheetId) {
    const id = normalizeText(spreadsheetId);
    return id ? `https://docs.google.com/spreadsheets/d/${id}/edit` : "";
}

function withNewTableButton() {
    return {
        reply_markup: {
            keyboard: [[{ text: "➕ Створити нову таблицю" }]],
            resize_keyboard: true
        }
    };
}

function isNewTableButtonText(text) {
    return /^\s*➕?\s*створити\s+нову\s+таблицю\s*$/i.test(String(text || ""));
}

function splitAnswers(text) {
    return String(text || "")
        .split(/\r?\n/)
        .map((line) => line.replace(/^\d+[\).]\s*/, "").trim())
        .filter(Boolean);
}

function looksLikeClarificationRequest(text) {
    const source = normalizeText(text).toLowerCase();
    if (!source) return false;

    return /\?|що\s+таке|що\s+значить|що\s+мається\s+на\s+увазі|не\s+зрозуміл|поясни|поясніть|розшифруй/.test(source);
}

function buildClarificationHelp(text, pendingQuestions = []) {
    const source = normalizeText(text).toLowerCase();

    if (/підзвіт|заявк/.test(source)) {
        return joinMessageBlocks([
            sectionTitle("💡", "Коротке пояснення"),
            bulletLines([
                "Підзвіт: людина сама оплачує витрату або їй наперед видають гроші, а потім вона записує цю витрату у таблицю.",
                "Заявка через бухгалтера: людина сама не платить, а просить бухгалтера провести оплату централізовано."
            ]),
            [
                "Можеш відповісти дуже коротко:",
                ...numberedLines(["Через бухгалтера"]),
                "або",
                ...numberedLines(["Підзвіт"])
            ]
        ]);
    }

    if (/google\s*form|форм/.test(source)) {
        return joinMessageBlocks([
            sectionTitle("💡", "Коротке пояснення"),
            bulletLines([
                "Google Form: проста онлайн-форма, куди людина вносить дані через посилання.",
                "Окремий аркуш: окрема вкладка в Google Sheets, де людина вносить дані напряму."
            ]),
            "Можеш відповісти коротко: Google Form або окремий аркуш."
        ]);
    }

    return joinMessageBlocks([
        sectionTitle("💬", "Можу переформулювати питання"),
        "Відповідай коротко по пунктах на ті питання, які вже надіслав бот.",
        pendingQuestions.length > 0 ? "Якщо зручніше, дай відповідь хоча б на перший пункт." : ""
    ]);
}

function getQuestionGlossary(questions = []) {
    const source = questions.map((q) => normalizeText(q?.text || "").toLowerCase()).join(" \n ");
    const notes = [];

    if (/підзвіт/.test(source)) {
        notes.push("підзвіт = людина платить сама або з виданих їй грошей, а потім записує витрату");
    }
    if (/заявк/.test(source)) {
        notes.push("заявка через бухгалтера = людина не платить сама, а бухгалтер оплачує централізовано");
    }
    if (/google\s*form/.test(source)) {
        notes.push("Google Form = проста онлайн-форма за посиланням для внесення даних");
    }
    if (/окремий аркуш/.test(source)) {
        notes.push("окремий аркуш = окрема вкладка в таблиці Google Sheets");
    }
    if (/p&l|\bpl\b/.test(source)) {
        notes.push("P&L = звіт про прибутки і збитки");
    }
    if (/dashboard/.test(source)) {
        notes.push("dashboard = зведений екран з ключовими показниками");
    }

    return notes;
}

function buildQuestionsMessage(title, questions) {
    const glossary = getQuestionGlossary(questions);
    return joinMessageBlocks([
        sectionTitle("❓", title),
        numberedLines(questions.map((q) => q.text)),
        glossary.length > 0
            ? [sectionTitle("🧩", "Пояснення термінів"), ...bulletLines(glossary)]
            : [],
        "Відповідай нумеровано. Можна навіть частково: 1. Через бухгалтера"
    ]);
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

function questionGroupKey(questionKey) {
    return String(questionKey || "").replace(/_\d+$/, "");
}

function shouldBroadcastSingleAnswer(text, batch, answers) {
    if (!Array.isArray(batch) || batch.length <= 1) return false;
    if (!Array.isArray(answers) || answers.length !== 1) return false;

    const source = normalizeText(text).toLowerCase();
    if (!source) return false;

    const hasExplicitNumbering = /^\s*\d+[).]/m.test(String(text || ""));
    if (hasExplicitNumbering) return false;

    const groups = new Set(batch.map((question) => questionGroupKey(question?.key)));
    if (groups.size === 1) return true;

    const samePaymentQuestion = batch.every((question) => /як проходить оплата|підзвіт|заявка через бухгалтера/i.test(String(question?.text || "")));
    if (samePaymentQuestion && /(все|усе|всім|усім|теж|однаково|скрізь|усюди|через бухгалтера|оплачує бухгалтер)/i.test(source)) {
        return true;
    }

    const sameMethodQuestion = batch.every((question) => /google form|окремий аркуш|як зручніше вносити/i.test(String(question?.text || "")));
    if (sameMethodQuestion && /(все|усе|всім|усім|теж|однаково|скрізь|усюди|google form|окремий аркуш)/i.test(source)) {
        return true;
    }

    return false;
}

function applyAnswers(draft, text) {
    const answers = splitAnswers(text);
    const batch = Array.isArray(draft.pendingQuestions) ? draft.pendingQuestions : [];
    const resolved = { ...(draft.answers || {}) };

    if (batch.length === 0) {
        return {
            answers: resolved,
            questionsQueue: Array.isArray(draft.questionsQueue) ? draft.questionsQueue : [],
            pendingQuestions: [],
            answeredCount: 0
        };
    }

    if (batch.length === 1) {
        resolved[batch[0].key] = normalizeAnswerValue(text);
    } else if (shouldBroadcastSingleAnswer(text, batch, answers)) {
        batch.forEach((question) => {
            resolved[question.key] = normalizeAnswerValue(text);
        });
    } else {
        batch.slice(0, answers.length).forEach((question, index) => {
            resolved[question.key] = normalizeAnswerValue(answers[index]);
        });
    }

    const answeredCount = batch.length === 1
        ? 1
        : shouldBroadcastSingleAnswer(text, batch, answers)
            ? batch.length
            : Math.min(batch.length, answers.length);
    const queuedAfterCurrentBatch = Array.isArray(draft.questionsQueue)
        ? draft.questionsQueue.slice(batch.length)
        : [];
    const remainingQueue = batch.slice(answeredCount).concat(queuedAfterCurrentBatch);

    return {
        answers: resolved,
        questionsQueue: remainingQueue,
        pendingQuestions: nextQuestionBatch(remainingQueue),
        answeredCount
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
    return joinMessageBlocks([
        sectionTitle("📋", "Проаналізував запит"),
        [
            labelValue("Загальна кількість операцій", `${analysis.totalOps}/міс`),
            analysis.noAccessPeople.length > 0
                ? labelValue("Без доступу до Sheets", analysis.noAccessPeople.map((i) => `${i.name} (${i.ops})`).join(", "))
                : labelValue("Без доступу до Sheets", "немає")
        ],
        [
            sectionTitle("🏗️", "Попередня архітектура"),
            ...bulletLines([
                `Надходження: архітектура ${analysis.inflowsMode}`,
                `Витрати: архітектура ${analysis.outflowsMode}`,
                "A = одна проста вкладка",
                "B = кілька людей або джерел",
                "C = складний процес з кількома учасниками"
            ])
        ]
    ]);
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

function detectKnownTypeFromText(text) {
    const source = String(text || "").toLowerCase();
    if (/(кешфлоу|кеш\s*флоу|кефлоу|cash\s*flow|cashflow)/i.test(source)) return "cashflow";
    if (/(p&l|\bpl\b|п\s*&\s*л|profit\s*(and|&)\s*loss|прибутк(и|у)?\s*і\s*збитк(и|ів))/i.test(source)) return "pl";
    if (/(баланс|balance)/i.test(source)) return "balance";
    if (/(дашборд|dashboard)/i.test(source)) return "dashboard";
    return "";
}

function detectRoutingMode(text, parsedTz) {
    const fromTz = normalizeReportType(parsedTz?.report_type);
    if (fromTz) return { mode: "known", reportType: fromTz };

    const fromText = detectKnownTypeFromText(text);
    if (fromText) return { mode: "known", reportType: fromText };

    const asksToBuild = /(зроби|побудуй|створи|потрібна|потрібен|таблиц|table)/i.test(String(text || ""));
    if (asksToBuild) return { mode: "custom", reportType: "" };

    return { mode: "unknown", reportType: "" };
}

function isBuildJsonPayload(text) {
    const json = tryParseJson(text);
    if (!json || typeof json !== "object" || Array.isArray(json)) return false;

    const action = normalizeText(json.action || "build_table").toLowerCase();
    return action === "build_table";
}

function shouldStartNewBuildFromMessage(message, draft) {
    const text = normalizeText(message?.text || "");
    if (!text) return false;

    if (isBuildJsonPayload(text)) return true;

    const parsed = parseTzFromTelegramMessage(message);
    if (parsed.detected) return true;

    if (!resolveActiveTable(draft) && !tryParseJson(text)) {
        return true;
    }

    return false;
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
            text: "Вкажи тип таблиці: cashflow (рух грошей), pl / P&L (прибутки і збитки), balance (баланс), dashboard (зведений екран з показниками)"
        });
    }

    if (!hasAtLeastOneArticle(tz)) {
        questions.push({
            key: "articles_seed",
            text: "Дай мінімум 1-2 статті, тобто які саме гроші заходять і на що витрачаються (формат: Надходження: ..., Витрати: ...)"
        });
    }

    const unresolvedNoAccess = findNoAccessItemsWithoutMode(tz);
    unresolvedNoAccess.forEach((item, index) => {
        const person = normalizeText(item.responsible || item.owner || "Співробітник");
        const article = normalizeText(item.article || item.name || "Стаття");
        questions.push({
            key: `money_flow_${index}`,
            text: `${article} — ${person}: як проходить оплата? Підзвіт = людина платить сама і потім записує витрату. Заявка через бухгалтера = бухгалтер оплачує централізовано.`
        });
        questions.push({
            key: `no_access_method_${index}`,
            text: `${article} — якщо це підзвіт, як зручніше вносити дані: Google Form (проста форма за посиланням) чи окремий аркуш (окрема вкладка в таблиці)?`
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
    return joinMessageBlocks([
        sectionTitle("✅", "Усе готово до побудови"),
        [
            labelValue("Файл", `${payload.report_type}_${payload.business_name}_${new Date().getFullYear()}`),
            labelValue("Аркуші", inferSheetsFromPayload(payload).join(", ")),
            labelValue("Надходження", (payload.articles?.inflows || []).join(", ") || "-"),
            labelValue("Витрати", (payload.articles?.outflows || []).join(", ") || "-")
        ],
        "Будуємо далі? Відповідь: так / ні / змінити"
    ]);
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
        return joinMessageBlocks([
            sectionTitle("📁", "У папці клієнта поки немає таблиць"),
            folderLine
        ]);
    }

    const activeId = draft.activeTableId || draft.spreadsheet_id || null;
    const lines = [sectionTitle("📁", "Твої таблиці")];
    if (tablesInfo.clientFolder) lines.push(`Папка клієнта: ${tablesInfo.clientFolder}`);
    if (tablesInfo.folderUrl) lines.push(`Folder URL: ${tablesInfo.folderUrl}`);
    lines.push("");

    tables.forEach((item, index) => {
        const mark = item.spreadsheet_id === activeId ? " [ACTIVE]" : "";
        lines.push(`${index + 1}. ${item.name}${mark}`);
        lines.push(`   id: ${item.spreadsheet_id}`);
        if (item.url) lines.push(`   url: ${item.url}`);
    });

    lines.push("Щоб перемкнутись, напиши: /use <номер або spreadsheet_id>");
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
    const lines = [sectionTitle("✅", "Таблиця готова"), ""];

    if (firstFile.name) lines.push(`📊 ${firstFile.name}`);
    if (firstFile.url) lines.push(`🔗 ${firstFile.url}`);
    if (result.folder_url) lines.push(`📁 Папка: ${result.folder_url}`);

    if (Array.isArray(result.sheets_built) && result.sheets_built.length > 0) {
        lines.push("", sectionTitle("🧾", "Що всередині"));
        result.sheets_built.forEach((name) => lines.push(`- ${name}`));
    }

    if (forms.length > 0) {
        lines.push("");
        lines.push(sectionTitle("📝", "Форми"));
        forms.forEach((form) => lines.push(`- Форма для ${form.name}: ${form.url}`));
    }

    const active = resolveActiveTable(draft);
    if (active) {
        lines.push("", `Активна таблиця: ${active.name}`);
        lines.push(`Active ID: ${active.spreadsheet_id}`);
    }

    lines.push("", sectionTitle("🚀", "Перші кроки"));
    lines.push("1. Відкрий таблицю і перевір аркуш «Інструкція»");
    lines.push("2. Видали жовті тестові рядки перед реальним використанням");
    if (payload.options?.counterparty_tracking) lines.push("3. Перевір дропдаун контрагентів");
    lines.push("", "Якщо треба щось змінити, просто напиши.");

    return lines.join("\n");
}

function formatLegacyResult(result, draft) {
    const lines = [sectionTitle("🛟", "Таблицю побудовано через резервний режим"), ""];
    if (result.folder_url) lines.push(`Папка: ${result.folder_url}`);
    if (Array.isArray(result.generated_files)) {
        result.generated_files.forEach((f) => lines.push(`${f.title}: ${f.spreadsheet_url}`));
    }

    const active = resolveActiveTable(draft);
    if (active) {
        lines.push("", `Активна таблиця: ${active.name}`);
        lines.push(`Active ID: ${active.spreadsheet_id}`);
    }

    lines.push("", "Щоб основним механізмом був Apps Script, налаштуй APPS_SCRIPT_URL.");
    return lines.join("\n");
}

function formatBuildError(error) {
    return joinMessageBlocks([
        sectionTitle("❌", "Не вдалося побудувати таблицю"),
        labelValue("Причина", normalizeText(error?.message || "unknown")),
        bulletLines([
            "Спробуй /retry, і я повторю спробу з тими самими даними.",
            "Або напиши, що змінити, і я перебудую таблицю."
        ])
    ]);
}

function hasBrokenFormulaError(validation) {
    const errors = Array.isArray(validation?.errors) ? validation.errors : [];
    return errors.some((item) => /зламані формули|#ref|#error|#name/i.test(String(item || "")));
}

function buildWelcomeMessage() {
    return joinMessageBlocks([
        sectionTitle("👋", "Привіт! Я допомагаю будувати фінансові таблиці для бізнесу."),
        [
            "Надішли мені опис того, що потрібно, текстом або у форматі tz-блоку.",
            "Я проаналізую запит, задам кілька уточнень за потреби і побудую таблицю на твоєму Google Drive."
        ],
        [
            sectionTitle("🧭", "Команди"),
            ...bulletLines([
                "/status — поточний стан роботи",
                "/tables — список твоїх таблиць",
                "/use — вибрати активну таблицю для правок",
                "/new — почати створення нової таблиці",
                "/retry — повторити останню побудову",
                "/clear — очистити поточний діалог"
            ])
        ],
        "Щоб почати, просто опиши свій бізнес і що треба відстежувати."
    ]);
}

function getBrandPhotoUrl() {
    const appBaseUrl = process.env.APP_BASE_URL;
    if (!appBaseUrl) return "";
    return `${appBaseUrl.replace(/\/$/, "")}/brand-photo`;
}

function buildDebugLogReadyMessage(logSheetUrl) {
    return joinMessageBlocks([
        sectionTitle("🧾", "Файл для технічних логів готовий"),
        "Я створив окрему Google Sheets-таблицю для логів Apps Script, щоб її можна було перевірити ще до першої побудови.",
        logSheetUrl ? `Посилання на лог-файл:\n${logSheetUrl}` : "",
        "Шукай її в Google Drive як DEBUG_APP_SCRIPT_LOGS або відкривай за посиланням вище."
    ]);
}

async function ensureDebugLogFileForChat(message) {
    const identity = getTelegramIdentity(message);
    try {
        const response = await pingAppsScript(identity);
        return {
            ok: true,
            logSheetUrl: normalizeText(response?.log_sheet_url || "")
        };
    } catch (error) {
        console.error("Failed to initialize Apps Script debug log file", {
            telegramId: identity.telegram_id,
            telegramUsername: identity.telegram_username,
            error: String(error?.message || error)
        });
        return {
            ok: false,
            error: String(error?.message || error)
        };
    }
}

function buildStatusMessage(draft) {
    const llm = getConfigSummary();
    const active = resolveActiveTable(draft);

    return joinMessageBlocks([
        sectionTitle("📍", "Поточний стан"),
        [
            labelValue("Етап", draft.stage),
            labelValue("Тип звіту", draft.report_type || "unknown"),
            labelValue("Питань залишилось", Array.isArray(draft.questionsQueue) ? draft.questionsQueue.length : 0),
            labelValue("Питань вже поставлено", `${Number(draft.askedQuestionsCount || 0)} / ${MAX_TOTAL_QUESTIONS}`),
            labelValue("Payload готовий", draft.payload ? "так" : "ні"),
            labelValue("Активна таблиця ID", active?.spreadsheet_id || "-"),
            labelValue("Активна таблиця", draft.activeTableName || "-")
        ],
        [
            sectionTitle("🤖", "AI"),
            labelValue("Fallback використано", draft.legacyFallbackUsed ? "так" : "ні"),
            labelValue("AI увімкнено", llm.enabled ? "так" : "ні"),
            labelValue("AI тимчасово вимкнено", draft.aiTemporarilyDisabled ? "так" : "ні"),
            labelValue("Провайдер", llm.provider),
            labelValue("Модель", llm.model)
        ]
    ]);
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
    await sendMessage(chatId, joinMessageBlocks([
        sectionTitle("⚙️", "Будую таблицю"),
        "Це займе приблизно 30 секунд."
    ]));
    await sendMessage(chatId, joinMessageBlocks([
        sectionTitle("🧱", "Готую структуру"),
        "Планую аркуші, зв'язки та базові налаштування."
    ]));

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

            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("🔍", "Перевіряю файл"),
                "Дивлюсь формули і цілісність таблиці."
            ]));

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
                    await sendMessage(chatId, joinMessageBlocks([
                        sectionTitle("🩺", "Знайшов технічний збій у формулах"),
                        "Пробую виправити автоматично."
                    ]));
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
                        await sendMessage(chatId, joinMessageBlocks([
                            sectionTitle("✅", "Формули виправлено"),
                            "Завершую налаштування."
                        ]));
                    }
                }

                if (validationResult && validationResult.valid === false) {
                    await sendMessage(chatId, joinMessageBlocks([
                        sectionTitle("⚠️", "Є проблема під час фінальної перевірки"),
                        "Таблицю я вже зберіг, але для автоматичної доводки потрібно запустити /retry."
                    ]));
                    return { handled: true, command: commandLabel, engine: "apps_script", result: build.result, validation: validationResult };
                }
            }

            const msg = [
                formatAppsScriptResult(build.result, payload, updatedDraft),
                validationResult?.warnings?.length
                    ? joinMessageBlocks([
                        sectionTitle("⚠️", "Попередження"),
                        bulletLines(validationResult.warnings)
                    ])
                    : "",
                "Щоб створити нову таблицю, натисни кнопку нижче або напиши /new."
            ].filter(Boolean).join("\n\n");
            await sendMessage(chatId, msg, withNewTableButton());
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
    const currentDraft = getDraft(chatId);
    if (!shouldUseAi(currentDraft)) {
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
        disableAiForChat(chatId, currentDraft, error?.message);
        const questionBudget = Math.max(0, MAX_TOTAL_QUESTIONS - Number(getDraft(chatId).askedQuestionsCount || 0));
        return {
            message: buildArchitectureMessage(analysis),
            questions: defaultQuestions.slice(0, questionBudget)
        };
    }
}

async function startCustomArchitectureMode(message, draft) {
    const chatId = message.chat.id;
    const rawText = normalizeText(message.text || "");

    const customDefaultQuestions = [
        { key: "custom_goal", text: "Яка головна мета таблиці і яке рішення має прийматись на її основі простими словами?" },
        { key: "custom_tabs", text: "Які аркуші або блоки потрібні, тобто які окремі вкладки чи розділи мають бути, і що на кожному зберігається?" },
        { key: "custom_fields", text: "Які ключові поля та формули обов'язкові? Поля = що людина заповнює, формули = що таблиця рахує автоматично." },
        { key: "custom_users", text: "Хто буде вносити дані, а хто тільки переглядатиме без редагування?" }
    ];

    let messageText = joinMessageBlocks([
        sectionTitle("🧠", "Вмикаю режим архітектора для кастомної таблиці"),
        "Зберу план і далі побудуємо структуру."
    ]);
    let questions = customDefaultQuestions;

    if (shouldUseAi(draft)) {
        try {
            const ai = await generateClarificationBundle({
                mode: "custom_architect",
                user_request: rawText,
                defaultQuestions: customDefaultQuestions,
                questionBudget: 8
            });
            if (ai?.message) messageText = ai.message;
            if (Array.isArray(ai?.questions) && ai.questions.length > 0) questions = ai.questions.slice(0, 8);
        } catch (error) {
            disableAiForChat(chatId, draft, error?.message);
        }
    }

    const pendingQuestions = nextQuestionBatch(questions);

    setDraft(chatId, {
        ...draft,
        stage: STAGES.COLLECTING,
        customMode: true,
        customPlanNotes: rawText,
        questionsQueue: questions,
        pendingQuestions,
        answers: {},
        askedQuestionsCount: Number(draft.askedQuestionsCount || 0) + pendingQuestions.length
    });

    await sendMessage(chatId, messageText);
    await sendMessage(chatId, joinMessageBlocks([
        sectionTitle("❓", "Кілька питань для проєктування"),
        numberedLines(pendingQuestions.map((q) => q.text)),
        "Відповідай нумеровано, наприклад: 1. Через бухгалтера / 2. Щотижня / 3. Ні"
    ]));

    return { handled: true, command: "custom_architect_started" };
}

async function parseTzFromAnyText(message, chatId, draft) {
    const parsed = parseTzFromTelegramMessage(message);
    if (parsed.detected && parsed.parsed) {
        return parsed.tz;
    }

    const heuristicCashflow = parseCashflowHeuristicFromText(message.text || "");
    if (heuristicCashflow) {
        return heuristicCashflow;
    }

    if (!shouldUseAi(draft)) {
        return null;
    }

    try {
        return await generateTzFromFreeText(message.text || "");
    } catch (error) {
        disableAiForChat(chatId, draft, error?.message);
        return null;
    }
}

async function resolveBusinessName(tz, rawText, message, draft, chatId) {
    const fromTz = normalizeText(tz?.business_name);
    if (fromTz) return fromTz;

    if (shouldUseAi(draft)) {
        try {
            const aiName = normalizeText(await generateBusinessNameFromText(rawText || ""));
            if (aiName) return aiName;
        } catch (error) {
            disableAiForChat(chatId, draft, error?.message);
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
        next.inflows = [{ article: "Оплата від клієнтів", responsible: "Owner", ops_per_month: 10, has_sheets_access: true }];
        next.outflows = [{ article: "Інші витрати", responsible: "Owner", ops_per_month: 10, has_sheets_access: true }];
    }

    return next;
}

function fallbackCustomBlueprint(answers, rawRequest) {
    const values = Object.entries(answers || {}).map(([k, v]) => `${k}: ${String(v || "")}`);
    return {
        title: "Кастомна таблиця",
        goal: normalizeText(rawRequest) || "Кастомний облік",
        sheet_plan: [
            { name: "Ввід", purpose: "Основні записи", editable_by: ["Оператор"] },
            { name: "Зведення", purpose: "Аналітика і підсумки", editable_by: ["Менеджер"] }
        ],
        fields: [],
        formulas: [],
        roles: [
            { role: "Оператор", can_edit: ["Ввід"], can_view: ["Зведення"] },
            { role: "Менеджер", can_edit: ["Зведення"], can_view: ["Ввід", "Зведення"] }
        ],
        automation: [],
        risks: ["Потрібна деталізація полів і формул"],
        open_questions: values.length ? values : ["Потрібно уточнити структуру полів"]
    };
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

    const tz = await parseTzFromAnyText(message, chatId, draft);
    const routing = detectRoutingMode(text, tz);

    if (routing.mode === "custom") {
        return startCustomArchitectureMode(message, draft);
    }

    if (!tz && routing.mode !== "known") {
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("🧩", "Не вистачає кількох деталей"),
            numberedLines([
                "Не визначений тип таблиці: це Cashflow (рух грошей), P&L (прибутки і збитки) чи щось інше?",
                "Опиши коротко, які операції треба відстежувати."
            ]),
            "Відповідай нумеровано або просто допиши опис."
        ]));
        return { handled: true, command: "awaiting_tz" };
    }

    const preparedTz = tz || {
        report_type: routing.reportType || "cashflow",
        business_name: "Business",
        inflows: [{ article: "Оплата від клієнтів", responsible: "Owner", ops_per_month: 20, has_sheets_access: true }],
        outflows: [{ article: "Операційні витрати", responsible: "Owner", ops_per_month: 20, has_sheets_access: true }]
    };

    const analysis = analyzeArchitecture(preparedTz);
    const businessNameResolved = await resolveBusinessName(preparedTz, text, message, draft, chatId);
    const questionsQueue = buildQuestionQueue(preparedTz, analysis, draft.askedQuestionsCount);
    const aiBundle = await buildClarificationWithAi(chatId, preparedTz, analysis, questionsQueue);
    const pendingQuestions = nextQuestionBatch(aiBundle.questions);

    if (aiBundle.questions.length === 0) {
        const readyTz = ensureBuildMinimum(preparedTz);
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
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("👌", "Критичних уточнень немає"),
            "Переходимо до побудови."
        ]));
        await sendMessage(chatId, buildConfirmationMessage(payload));
        return { handled: true, command: "ready_without_questions" };
    }

    setDraft(chatId, {
        ...draft,
        stage: STAGES.COLLECTING,
        report_type: String(preparedTz.report_type || "cashflow").toLowerCase(),
        raw_input: text,
        extracted: preparedTz,
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
        await sendMessage(chatId, buildQuestionsMessage("Є кілька уточнень перед тим як починати:", pendingQuestions));
    }

    return { handled: true, command: "tz_clarification_started" };
}

async function handleCollectingAnswers(message, draft) {
    const chatId = message.chat.id;
    const text = normalizeText(message.text || "");

    if (looksLikeClarificationRequest(text)) {
        await sendMessage(chatId, buildClarificationHelp(text, draft.pendingQuestions || []));
        if ((draft.pendingQuestions || []).length > 0) {
            await sendMessage(chatId, buildQuestionsMessage("Повертаємось до уточнень:", draft.pendingQuestions || []));
        }
        return { handled: true, command: "clarification_help" };
    }

    const updated = applyAnswers(draft, message.text || "");
    const extractedAfterAnswers = applyCriticalAnswerSideEffects(draft, updated.answers);

    const nextDraft = {
        ...draft,
        ...updated,
        stage: updated.pendingQuestions.length > 0 ? STAGES.COLLECTING : STAGES.CONFIRMING,
        extracted: extractedAfterAnswers,
        report_type: normalizeReportType(extractedAfterAnswers.report_type) || draft.report_type,
        askedQuestionsCount: Number(draft.askedQuestionsCount || 0),
        clarifications: [...(draft.clarifications || []), normalizeText(message.text)]
    };

    if (updated.pendingQuestions.length > 0) {
        setDraft(chatId, nextDraft);
        await sendMessage(chatId, buildQuestionsMessage("Кілька уточнень:", updated.pendingQuestions));
        return { handled: true, command: "clarify_answers" };
    }

    if (draft.customMode) {
        let blueprint = null;
        if (shouldUseAi(draft)) {
            try {
                blueprint = await generateCustomTableBlueprint({
                    request: draft.customPlanNotes,
                    answers: nextDraft.answers,
                    history: nextDraft.history || []
                });
            } catch (error) {
                disableAiForChat(chatId, draft, error?.message);
            }
        }
        if (!blueprint) {
            blueprint = fallbackCustomBlueprint(nextDraft.answers, draft.customPlanNotes);
        }

        const summary = joinMessageBlocks([
            sectionTitle("🗺️", "План кастомної таблиці зібрано"),
            labelValue("Назва плану", blueprint.title),
            blueprint.goal ? labelValue("Мета", blueprint.goal) : "",
            labelValue("Аркушів у плані", Array.isArray(blueprint.sheet_plan) ? blueprint.sheet_plan.length : 0),
            [
                sectionTitle("➡️", "Що далі"),
                ...numberedLines([
                    "Можемо звузити задачу до готового типу (cashflow / pl / balance / dashboard) і будувати одразу.",
                    "Або рухатись у full custom builder за цим blueprint."
                ])
            ]
        ]);

        setDraft(chatId, {
            ...nextDraft,
            stage: STAGES.IDLE,
            customMode: false,
            customPlanNotes: JSON.stringify(blueprint, null, 2)
        });
        await sendMessage(chatId, summary);
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("🧾", "Blueprint JSON"),
            JSON.stringify(blueprint, null, 2)
        ]));
        return { handled: true, command: "custom_architect_finished" };
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
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("⚠️", "Немає готового payload"),
            "Надішли ТЗ ще раз."
        ]));
        return { handled: true, command: "confirm_without_payload" };
    }

    if (isConfirmBuildText(text)) return runBuildAndReply(message, draft, draft.payload, "confirmed_build");

    if (isRejectBuildText(text)) {
        setDraft(chatId, { ...draft, stage: STAGES.COLLECTING, pendingQuestions: [], questionsQueue: [] });
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("✏️", "Ок, вносимо зміни"),
            "Напиши, що саме треба змінити, і я оновлю payload перед побудовою."
        ]));
        return { handled: true, command: "confirmation_rejected" };
    }

    await sendMessage(chatId, "Відповідь: так / ні / змінити.");
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

    if (!shouldUseAi(draft) || !spreadsheetId) {
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
        disableAiForChat(chatId, draft, error?.message);
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("⚠️", "Не зміг точно розпізнати правку через AI"),
            "Перемикаюсь на базовий режим."
        ]));
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
            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("⚠️", "Непідтримувана дія"),
                "У режимі правок підтримується тільки action=update_table."
            ]));
            return { handled: true, command: "editing_wrong_action" };
        }

        const result = await updateTableViaAppsScript(payload);
        const tableId = payload.spreadsheet_id || draft.activeTableId || draft.spreadsheet_id || "";
        const tableUrl = spreadsheetUrlById(tableId);
        const messageText = [
            "✅ Готово. Правки внесено.",
            tableUrl ? `Посилання на таблицю:\n${tableUrl}` : "",
            "Таблиця оновлена. Якщо треба ще щось — пиши.",
            "Щоб створити нову таблицю, натисни кнопку нижче або напиши /new."
        ].filter(Boolean).join("\n\n");
        await sendMessage(chatId, messageText, withNewTableButton());
        return { handled: true, command: "editing_json_update" };
    }

    const aiResult = await buildUpdatePayloadWithAi(chatId, text, draft);
    if (Array.isArray(aiResult.missing) && aiResult.missing.length > 0) {
        await sendMessage(chatId, aiResult.message_to_user || joinMessageBlocks([
            sectionTitle("❓", "Потрібні уточнення"),
            aiResult.missing.join(", ")
        ]));
        return { handled: true, command: "editing_need_more_data" };
    }

    if (!aiResult.update_payload) {
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("🛠️", "Не вистачає даних для правки"),
            "Надішли JSON update_table або сформулюй правку точніше."
        ]));
        return { handled: true, command: "editing_no_payload" };
    }

    const result = await updateTableViaAppsScript(aiResult.update_payload);
    const tableId = aiResult.update_payload.spreadsheet_id || draft.activeTableId || draft.spreadsheet_id || "";
    const tableUrl = spreadsheetUrlById(tableId);
    const messageText = [
        "✅ Готово. Правки внесено.",
        tableUrl ? `Посилання на таблицю:\n${tableUrl}` : "",
        "Таблиця оновлена. Якщо треба ще щось — пиши.",
        "Щоб створити нову таблицю, натисни кнопку нижче або напиши /new."
    ].filter(Boolean).join("\n\n");
    await sendMessage(chatId, messageText, withNewTableButton());
    return { handled: true, command: "editing_auto_update" };
}

async function handleUseCommand(message, draft, argRaw) {
    const chatId = message.chat.id;
    let tablesInfo;

    try {
        tablesInfo = await loadTablesForUser(message);
    } catch (error) {
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("❌", "Не вдалося отримати список таблиць"),
            error.message
        ]));
        return { handled: true, command: "use_load_error" };
    }

    const selected = selectTableFromArg(tablesInfo.tables, argRaw);

    if (!selected) {
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("🔎", "Не знайшов таблицю"),
            "Скористайся /tables, а потім /use <номер або spreadsheet_id>."
        ]));
        return { handled: true, command: "use_not_found" };
    }

    setDraft(chatId, {
        ...draft,
        activeTableId: selected.spreadsheet_id,
        spreadsheet_id: selected.spreadsheet_id,
        activeTableName: selected.name || null,
        stage: STAGES.EDITING
    });

    await sendMessage(chatId, joinMessageBlocks([
        sectionTitle("✅", "Активну таблицю вибрано"),
        labelValue("Назва", selected.name),
        labelValue("ID", selected.spreadsheet_id)
    ]));
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
        aiTemporarilyDisabled: false,
        customMode: false,
        customPlanNotes: "",
        activeTableId: draft.activeTableId || null,
        activeTableName: draft.activeTableName || null,
        spreadsheet_id: draft.activeTableId || null
    });

    await sendMessage(chatId, joinMessageBlocks([
        sectionTitle("🆕", "Починаємо нову таблицю"),
        "Надішли нове ТЗ. Можна звичайним текстом.",
        "Активну таблицю для правок можна змінити через /use."
    ]));
    return { handled: true, command: "new_flow" };
}

async function handleTelegramUpdate(update) {
    if (isDuplicateUpdate(update)) {
        return { handled: true, reason: "duplicate_update" };
    }

    const message = extractMessage(update);
    if (!message || !message.chat) return { handled: false, reason: "No message context" };

    const chatId = getChatId(message);
    if (!chatId) return { handled: false, reason: "No chat id" };

    return runWithChatQueue(chatId, async () => {
        const text = message.text || "";
        const command = extractCommand(text);
        const commandArg = extractCommandArg(text);
        const draft = getDraft(chatId);

        if (draft.stage === STAGES.BUILDING && command === "/retry") {
            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("⏳", "Побудова вже триває"),
                "Дочекайся завершення поточної спроби."
            ]));
            return { handled: true, command };
        }

        if (command === "/clear") {
            clearDraft(chatId);
            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("🗑️", "Стан очищено"),
                "Можеш починати з нового ТЗ."
            ]));
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
                await sendMessage(chatId, joinMessageBlocks([
                    sectionTitle("❌", "Не вдалося отримати список таблиць"),
                    error.message
                ]));
            }
            return { handled: true, command };
        }

        if (command === "/use") {
            return handleUseCommand(message, draft, commandArg);
        }

        if (command === "/new") {
            return handleNewCommand(message, draft);
        }

        if (isNewTableButtonText(text)) {
            return handleNewCommand(message, draft);
        }

        if (command === "/retry") {
            if (!draft.lastPayload) {
                await sendMessage(chatId, joinMessageBlocks([
                    sectionTitle("⚠️", "Немає даних для повтору"),
                    "Надішли ТЗ або JSON."
                ]));
                return { handled: true, command };
            }
            await sendMessage(chatId, sectionTitle("🔄", "Повторюю побудову..."));
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

            const debugLogInit = await ensureDebugLogFileForChat(message);
            if (debugLogInit.ok && debugLogInit.logSheetUrl) {
                await sendMessage(chatId, buildDebugLogReadyMessage(debugLogInit.logSheetUrl));
            } else {
                await sendMessage(chatId, joinMessageBlocks([
                    sectionTitle("⚠️", "Не вдалося одразу підготувати лог-файл"),
                    "Це не блокує роботу бота. Лог-таблиця спробує створитися при першому зверненні до Apps Script.",
                    debugLogInit.error ? `Причина: ${debugLogInit.error}` : ""
                ]));
            }
            return { handled: true, command };
        }

        if (draft.stage === STAGES.BUILDING) {
            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("⏳", "Побудова вже триває"),
                "Дочекайся завершення або використай /status."
            ]));
            return { handled: true, command: "build_in_progress" };
        }

        if (draft.stage === STAGES.COLLECTING && ((draft.pendingQuestions?.length || 0) > 0 || (draft.questionsQueue?.length || 0) > 0)) {
            return handleCollectingAnswers(message, draft);
        }

        if (draft.stage === STAGES.CONFIRMING) {
            return handleConfirmation(message, draft);
        }

        if (draft.stage === STAGES.EDITING && shouldStartNewBuildFromMessage(message, draft)) {
            return handleTzCapture(message, {
                ...draft,
                stage: STAGES.IDLE
            });
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
    });
}

module.exports = {
    handleTelegramUpdate
};
