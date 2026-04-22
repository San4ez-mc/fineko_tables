const { buildReports } = require("../google/reportBuilder");
const { pingAppsScript, buildTableViaAppsScript, updateTableViaAppsScript, listTablesViaAppsScript, validateTableViaAppsScript } = require("../google/appsScriptClient");
const { sendMessage, editMessageText, deleteMessage, sendPhoto } = require("./bot");
const { parseTzFromTelegramMessage, analyzeArchitecture } = require("./tzParser");
const { classifyInput } = require("./inputRouter");
const { buildActiveQueue } = require("./questionGraph");
const {
    isEnabled: isLlmEnabled,
    getConfigSummary,
    generateClarificationBundle,
    resolveClarification,
    generateUpdatePayloadFromText,
    planQuestionsFromFreeText,
    parseFreeText,
    generateBusinessNameFromText,
    generateCustomTableBlueprint,
    runPayloadSelfCheck,
    parsePaymentCalendarFixedCosts
} = require("../ai/agentBrain");

const DRAFTS = new Map();
const CHAT_QUEUES = new Map();
const PROCESSED_UPDATES = new Map();
const MAX_TOTAL_QUESTIONS = 15;
const UPDATE_DEDUP_TTL_MS = 10 * 60 * 1000;
const TELEGRAM_API_BASE = "https://api.telegram.org";

const STAGES = {
    IDLE: "idle",
    COLLECTING: "collecting",
    CONFIRMING: "confirming",
    BUILDING: "building",
    EDITING: "editing"
};

function createEmptyDraft(previous = {}) {
    return {
        stage: STAGES.IDLE,
        inputMode: null,
        report_type: null,
        raw_input: "",
        extracted: {},
        clarifications: [],
        payload: null,
        spreadsheet_id: previous.spreadsheet_id || null,
        history: [],
        answers: {},
        resolvedAnswers: {},
        skippedKeys: [],
        questionQueue: [],
        questionsQueue: [],
        pendingQuestions: [],
        lastPayload: previous.lastPayload || null,
        legacyFallbackUsed: false,
        activeTableId: previous.activeTableId || null,
        activeTableName: previous.activeTableName || null,
        askedQuestionsCount: 0,
        questionsCount: 0,
        businessNameResolved: null,
        aiTemporarilyDisabled: false,
        customMode: false,
        customPlanNotes: "",
        buildProgressMessageId: previous.buildProgressMessageId || null,
        updatedAt: new Date().toISOString()
    };
}

function getDraft(chatId) {
    const draft = DRAFTS.get(chatId);
    if (!draft) {
        return createEmptyDraft();
    }

    const resolvedAnswers = draft.resolvedAnswers || draft.answers || {};
    const questionQueue = draft.questionQueue || draft.questionsQueue || [];
    const questionsCount = Number(draft.questionsCount || draft.askedQuestionsCount || 0);

    return {
        ...createEmptyDraft(draft),
        ...draft,
        inputMode: draft.inputMode || null,
        answers: resolvedAnswers,
        resolvedAnswers,
        skippedKeys: Array.isArray(draft.skippedKeys) ? draft.skippedKeys : [],
        questionQueue,
        questionsQueue: questionQueue,
        pendingQuestions: Array.isArray(draft.pendingQuestions) ? draft.pendingQuestions : [],
        askedQuestionsCount: questionsCount,
        questionsCount
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
    DRAFTS.set(chatId, createEmptyDraft(current));
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

function getTelegramBotToken() {
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (!token) {
        throw new Error("Missing TELEGRAM_BOT_TOKEN");
    }
    return token;
}

async function telegramGetJson(pathname, searchParams = {}) {
    const token = getTelegramBotToken();
    const query = new URLSearchParams(searchParams).toString();
    const url = `${TELEGRAM_API_BASE}/bot${token}/${pathname}${query ? `?${query}` : ""}`;
    const response = await fetch(url);
    const data = await response.json();

    if (!response.ok || !data?.ok) {
        throw new Error(`Telegram API error: ${response.status} ${JSON.stringify(data)}`);
    }

    return data.result;
}

async function downloadTelegramFileText(filePath) {
    const token = getTelegramBotToken();
    const response = await fetch(`${TELEGRAM_API_BASE}/file/bot${token}/${filePath}`);
    if (!response.ok) {
        throw new Error(`Telegram file download failed: ${response.status}`);
    }
    return response.text();
}

function isMarkdownDocument(message) {
    const document = message?.document;
    if (!document) return false;
    const fileName = normalizeText(document.file_name).toLowerCase();
    const mimeType = normalizeText(document.mime_type).toLowerCase();
    return fileName.endsWith(".md") || mimeType === "text/markdown" || mimeType === "text/plain";
}

function mergeDocumentText(caption, documentText) {
    return joinMessageBlocks([
        normalizeText(caption),
        normalizeText(documentText)
    ]);
}

async function hydrateMessageTextFromDocument(message) {
    if (!isMarkdownDocument(message)) {
        return message;
    }

    const fileId = normalizeText(message?.document?.file_id);
    if (!fileId) {
        return message;
    }

    const fileMeta = await telegramGetJson("getFile", { file_id: fileId });
    const filePath = normalizeText(fileMeta?.file_path);
    if (!filePath) {
        throw new Error("Telegram getFile did not return file_path");
    }

    const documentText = await downloadTelegramFileText(filePath);
    return {
        ...message,
        text: mergeDocumentText(message.caption || "", documentText),
        caption: message.caption || "",
        document: {
            ...(message.document || {}),
            file_path: filePath
        }
    };
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

function isCentralizedPaymentAnswer(value) {
    const text = normalizeText(value).toLowerCase();
    return /(через бухгал|оплачує бухгалтер|бухгалтер оплачує|централізовано|не сам|не сама)/i.test(text);
}

function isAccountablePaymentAnswer(value) {
    const text = normalizeText(value).toLowerCase();
    return /(^accountable$|підзвіт|сам платить|сама платить|самостійно|сам |сама )/i.test(text);
}

function inferInputModeFromAnswers(flowDecision, methodDecision) {
    const flowText = normalizeText(flowDecision);
    const methodText = normalizeText(methodDecision).toLowerCase();

    if (!flowText) {
        return { input_mode: "", payment: "" };
    }

    if (isCentralizedPaymentAnswer(flowText) || /^centralized$/i.test(flowText)) {
        return { input_mode: "direct", payment: "centralized" };
    }

    if (!isAccountablePaymentAnswer(flowText)) {
        return { input_mode: "", payment: "" };
    }

    if (methodText.includes("form") || methodText.includes("гугл форм")) {
        return { input_mode: "form", payment: "accountable" };
    }

    if (methodText.includes("sheet") || methodText.includes("аркуш")) {
        return { input_mode: "sheet", payment: "accountable" };
    }

    return { input_mode: "", payment: "accountable" };
}

function applyAnswers(draft, text) {
    const queue = Array.isArray(draft.questionQueue) ? draft.questionQueue : [];
    const firstQuestion = queue[0] || null;
    const resolvedAnswers = { ...(draft.resolvedAnswers || draft.answers || {}) };

    if (!firstQuestion) {
        return {
            resolvedAnswers,
            skippedKeys: Array.isArray(draft.skippedKeys) ? draft.skippedKeys : [],
            answeredCount: 0
        };
    }

    resolvedAnswers[firstQuestion.key] = normalizeAnswerValue(text);
    return {
        resolvedAnswers,
        skippedKeys: Array.isArray(draft.skippedKeys) ? draft.skippedKeys : [],
        answeredCount: 1
    };
}

async function resolveClarificationAnswersWithAi(chatId, draft, text) {
    if (!shouldUseAi(draft)) {
        return null;
    }

    try {
        const aiResult = await resolveClarification(
            String(text || ""),
            Array.isArray(draft.questionQueue) ? draft.questionQueue : [],
            draft.resolvedAnswers || draft.answers || {},
            draft.extracted || {}
        );

        return {
            resolvedAnswers: {
                ...(draft.resolvedAnswers || draft.answers || {}),
                ...(aiResult.resolved || {})
            },
            skippedKeys: Array.from(new Set([...(draft.skippedKeys || []), ...(aiResult.skipped || [])])),
            answeredCount: Object.keys(aiResult.resolved || {}).length,
            confidence: aiResult.confidence || 0,
            notes: aiResult.interpretation || ""
        };
    } catch (error) {
        disableAiForChat(chatId, draft, error?.message);
        return null;
    }
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
    const allowed = ["cashflow", "pl", "balance", "cashflow_and_pl", "dashboard"];
    return allowed.includes(text) ? text : "";
}

function detectKnownTypeFromText(text) {
    const source = String(text || "").toLowerCase();
    if ((/(кешфлоу|кеш\s*флоу|cash\s*flow|cashflow)/i.test(source)) && (/(p&l|\bpl\b|profit\s*(and|&)\s*loss|прибутк(и|у)?\s*і\s*збитк(и|ів))/i.test(source))) return "cashflow_and_pl";
    if (/(кешфлоу|кеш\s*флоу|кефлоу|cash\s*flow|cashflow)/i.test(source)) return "cashflow";
    if (/(p&l|\bpl\b|п\s*&\s*л|profit\s*(and|&)\s*loss|прибутк(и|у)?\s*і\s*збитк(и|ів))/i.test(source)) return "pl";
    if (/(баланс|balance)/i.test(source)) return "balance";
    if (/(дашборд|dashboard)/i.test(source)) return "dashboard";
    return "";
}

function isPlLikeReportType(value) {
    return ["pl", "cashflow_and_pl"].includes(normalizeReportType(value));
}

function buildArticleSeedSummary(tz = {}) {
    const inflows = (Array.isArray(tz.inflows) ? tz.inflows : [])
        .map((item) => normalizeText(item?.article || item?.name))
        .filter(Boolean);
    const outflows = (Array.isArray(tz.outflows) ? tz.outflows : [])
        .map((item) => normalizeText(item?.article || item?.name))
        .filter(Boolean);

    const inflowsText = inflows.length > 0 ? inflows.join(", ") : "не знайшов";
    const outflowsText = outflows.length > 0 ? outflows.join(", ") : "не знайшов";
    return `Надходження: ${inflowsText}\nВитрати: ${outflowsText}`;
}

function parseArticlesSeedAnswer(value) {
    const text = normalizeText(value);
    if (!text) return null;

    const inflowMatch = text.match(/надходження\s*:\s*([^\n]+)/i);
    const outflowMatch = text.match(/витрати\s*:\s*([^\n]+)/i);
    if (!inflowMatch && !outflowMatch) return null;

    const parseList = (source) => String(source || "")
        .split(/[;,]/)
        .map((item) => normalizeText(item))
        .filter(Boolean)
        .map((article) => ({ article, responsible: "Owner", ops_per_month: 10, has_sheets_access: true }));

    return {
        inflows: parseList(inflowMatch?.[1]),
        outflows: parseList(outflowMatch?.[1])
    };
}

function normalizeCostTypeAnswer(value) {
    const text = normalizeText(value).toLowerCase();
    if (/прям|cogs|собіварт/.test(text)) return "cogs";
    if (/операц|opex/.test(text)) return "opex";
    if (/подат/.test(text)) return "tax";
    if (/власник|owner/.test(text)) return "owner";
    return "";
}

function normalizeRecognitionMomentAnswer(value) {
    const text = normalizeText(value).toLowerCase();
    if (/оплат/.test(text)) return "payment_date";
    if (/акт|накладн/.test(text)) return "act_date";
    if (/нарахув/.test(text)) return "accrual_date";
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

function isPaymentCalendarRequestedTz(tz) {
    return Boolean(tz?._payment_calendar_requested);
}

function buildQuestionQueue(tz, _analysis, resolvedAnswers = {}, skippedKeys = [], questionsCount = 0) {
    const questions = [];

    if (tz?._requires_article_confirmation && resolvedAnswers.free_text_articles_confirm === undefined) {
        questions.push({
            key: "free_text_articles_confirm",
            text: `Я витягнув такі статті з твого опису. Підтверди або виправ одним повідомленням у форматі "Надходження: ... / Витрати: ...".\n${buildArticleSeedSummary(tz)}`
        });
    }

    if (!normalizeReportType(tz.report_type) && !normalizeText(resolvedAnswers.report_type)) {
        questions.push({
            key: "report_type",
            text: "Вкажи тип таблиці: cashflow (рух грошей), pl / P&L (прибутки і збитки), cashflow_and_pl (обидва звіти в одному файлі), balance (баланс), dashboard (зведений екран з показниками)"
        });
    }

    if (!hasAtLeastOneArticle(tz) && !normalizeText(resolvedAnswers.articles_seed)) {
        questions.push({
            key: "articles_seed",
            text: "Дай мінімум 1-2 статті, тобто які саме гроші заходять і на що витрачаються (формат: Надходження: ..., Витрати: ...)"
        });
    }

    if (isPaymentCalendarRequestedTz(tz)
        && Array.isArray(tz?.outflows)
        && tz.outflows.length > 0
        && resolvedAnswers.payment_calendar_fixed_rules === undefined) {
        questions.push({
            key: "payment_calendar_fixed_rules",
            text: "По яких статтях витрат є фіксована дата платежу щомісяця? Наприклад: зарплата — 10-го, оренда — 1-го, податки — 20-го. Напиши у форматі: назва статті — число місяця. Або напиши \"пропустити\" якщо поки невідомо."
        });
    }

    questions.push(...buildActiveQueue(tz, resolvedAnswers, skippedKeys));

    if (isPlLikeReportType(tz.report_type) && resolvedAnswers.pl_project_tracking === undefined) {
        questions.push({
            key: "pl_project_tracking",
            text: "Для P&L потрібен облік по проєктах чи достатньо загальної картини без проєктів?"
        });
    }

    if (isPlLikeReportType(tz.report_type) && /^(так|yes|y|true|по\s*проєктах|з\s*проєктами)$/i.test(normalizeText(resolvedAnswers.pl_project_tracking))
        && !normalizeText(resolvedAnswers.pl_project_list)) {
        questions.push({
            key: "pl_project_list",
            text: "Переліч проєкти через кому, наприклад: Project A, Project B"
        });
    }

    const budgetLeft = Math.max(0, MAX_TOTAL_QUESTIONS - Number(questionsCount || 0));
    return questions.slice(0, budgetLeft);
}

function normalizeCalendarRules(rules = []) {
    return (Array.isArray(rules) ? rules : [])
        .map((item) => ({
            name: normalizeText(item?.name || item?.article),
            typical_day: Number(item?.typical_day)
        }))
        .filter((item) => item.name && Number.isInteger(item.typical_day) && item.typical_day >= 1 && item.typical_day <= 31);
}

async function resolvePaymentCalendarRules(chatId, draft, answerText) {
    const outflows = Array.isArray(draft?.extracted?.outflows) ? draft.extracted.outflows : [];

    try {
        return normalizeCalendarRules(await parsePaymentCalendarFixedCosts(answerText, outflows));
    } catch (error) {
        disableAiForChat(chatId, draft, error?.message);
        return [];
    }
}

function buildResponsibleMap(tz, answers = {}) {
    const result = {};

    const addItem = (item, outflowIndex = null) => {
        const article = normalizeText(item.article || item.name);
        if (!article) return;

        const person = normalizeText(item.responsible || item.owner || "Owner");
        const hasAccess = item.has_sheets_access !== false;
        let inputMode = hasAccess ? "direct" : "sheet";
        let payment = hasAccess ? "centralized" : "accountable";

        if (!hasAccess) {
            const flowDecision = outflowIndex === null ? "" : answers[`money_flow_${outflowIndex}`];
            const methodDecision = outflowIndex === null ? "" : answers[`no_access_method_${outflowIndex}`];
            const resolvedInput = inferInputModeFromAnswers(flowDecision, methodDecision);
            inputMode = resolvedInput.input_mode;
            payment = resolvedInput.payment;
        }

        result[article] = {
            name: person,
            access: hasAccess,
            input_mode: inputMode,
            payment
        };
    };

    (Array.isArray(tz.inflows) ? tz.inflows : []).forEach((item) => addItem(item, null));
    (Array.isArray(tz.outflows) ? tz.outflows : []).forEach((item, index) => addItem(item, index));

    return result;
}

function buildPayloadFromTzDraft(draft, message) {
    const identity = getTelegramIdentity(message);
    const tz = draft.extracted || {};
    const answers = draft.resolvedAnswers || draft.answers || {};

    const inflows = (Array.isArray(tz.inflows) ? tz.inflows : [])
        .map((item) => normalizeText(item.article || item.name))
        .filter(Boolean);
    const outflows = (Array.isArray(tz.outflows) ? tz.outflows : [])
        .map((item) => normalizeText(item.article || item.name))
        .filter(Boolean);
    const articleDetails = {
        inflows: (Array.isArray(tz.inflows) ? tz.inflows : []).map((item) => ({
            article: normalizeText(item.article || item.name),
            name: normalizeText(item.article || item.name),
            responsible: normalizeText(item.responsible || item.owner || "Owner"),
            ops_per_month: Number(item.ops_per_month) || 0,
            has_sheets_access: item.has_sheets_access !== false,
            recognition_moment: normalizeRecognitionMomentAnswer(item.recognition_moment) || normalizeText(item.recognition_moment) || null
        })).filter((item) => item.article),
        outflows: (Array.isArray(tz.outflows) ? tz.outflows : []).map((item) => ({
            article: normalizeText(item.article || item.name),
            name: normalizeText(item.article || item.name),
            responsible: normalizeText(item.responsible || item.owner || "Owner"),
            ops_per_month: Number(item.ops_per_month) || 0,
            has_sheets_access: item.has_sheets_access !== false,
            cost_type: normalizeCostTypeAnswer(item.cost_type) || normalizeText(item.cost_type) || null,
            recognition_moment: normalizeRecognitionMomentAnswer(item.recognition_moment) || normalizeText(item.recognition_moment) || null,
            is_fixed: item.is_fixed === true,
            typical_day: Number.isInteger(Number(item.typical_day)) ? Number(item.typical_day) : null
        })).filter((item) => item.article)
    };
    const projectTracking = /^(так|yes|y|true|по\s*проєктах|з\s*проєктами)$/i.test(normalizeText(answers.pl_project_tracking));
    const projects = normalizeText(answers.pl_project_list)
        .split(/[;,]/)
        .map((item) => normalizeText(item))
        .filter(Boolean);
    const paymentCalendarRequested = isPaymentCalendarRequestedTz(tz) || draft.inputClassification === "payment_calendar_cashflow_tz";
    const minimalBuildRequested = draft.inputClassification === "minimal_cashflow_tz" || draft.inputClassification === "payment_calendar_cashflow_tz";

    const payload = {
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
        inflows: articleDetails.inflows.map((item) => ({ name: item.name })),
        outflows: articleDetails.outflows.map((item) => ({
            name: item.name,
            is_fixed: item.is_fixed === true,
            typical_day: item.typical_day
        })),
        article_details: articleDetails,
        responsible: buildResponsibleMap(tz, answers),
        pl_settings: {
            project_tracking: projectTracking,
            projects
        },
        payment_calendar: paymentCalendarRequested,
        options: {
            payment_calendar: paymentCalendarRequested,
            multi_account: false,
            counterparty_tracking: false,
            formatting: true
        }
    };

    if (minimalBuildRequested) {
        payload.build_mode = "minimal";
    }

    return payload;
}

function inferSheetsFromPayload(payload) {
    const sheets = [];
    if (payload.report_type === "cashflow") {
        if (payload.build_mode === "minimal") {
            sheets.push("Cashflow", "Надходження", "Витрати", "Довідники", "Інструкція");
        } else {
            sheets.push("Cashflow", "Надходження", "Витрати", "Довідники", "Налаштування", "References", "Інструкція");
        }
    } else if (payload.report_type === "cashflow_and_pl") {
        sheets.push("Cashflow", "Надходження", "Витрати", "P&L", "Доходи", "Прямі витрати", "Операційні витрати", "Довідники", "Налаштування", "References", "Інструкція");
    } else if (payload.report_type === "pl") {
        sheets.push("P&L", "Доходи", "Прямі витрати", "Операційні витрати", "Довідники", "Налаштування", "References", "Інструкція");
    } else if (payload.report_type === "balance") {
        sheets.push("Баланс", "Довідники", "Налаштування", "References", "Інструкція");
    } else {
        sheets.push("Dashboard", "References", "Інструкція");
    }
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
    const title = payload.options?.payment_calendar
        ? sectionTitle("📅", "Платіжний календар готовий")
        : sectionTitle("✅", "Таблиця готова");
    const lines = [title, ""];

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
    }

    lines.push("", sectionTitle("🚀", "Перші кроки"));
    if (payload.options?.payment_calendar) {
        lines.push("1. Відкрий аркуш «📅 Платіжний календар»");
        lines.push("2. Введи поточний залишок на рахунку");
        lines.push("3. По кожному тижню внеси очікувані надходження і платежі");
        lines.push("4. Видали жовті тестові значення перед початком роботи");
        lines.push("", "Якщо залишок стає червоним, у тебе є час заздалегідь виправити касовий розрив.");
    } else {
        lines.push("1. Відкрий таблицю і перевір аркуш «Інструкція»");
        lines.push("2. Видали жовті тестові рядки перед реальним використанням");
        if (payload.options?.counterparty_tracking) lines.push("3. Перевір дропдаун контрагентів");
    }
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
            labelValue("Режим вводу", draft.inputMode || "-"),
            labelValue("Тип звіту", draft.report_type || "unknown"),
            labelValue("Питань залишилось", Array.isArray(draft.questionQueue) ? draft.questionQueue.length : 0),
            labelValue("Питань вже поставлено", `${Number(draft.questionsCount || draft.askedQuestionsCount || 0)} / ${MAX_TOTAL_QUESTIONS}`),
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

async function updateBuildProgressMessage(chatId, draft, title, description) {
    const text = joinMessageBlocks([
        sectionTitle(title.emoji, title.text),
        description
    ]);

    const currentMessageId = draft.buildProgressMessageId || null;
    if (currentMessageId) {
        try {
            await editMessageText(chatId, currentMessageId, text);
            return { ...draft, buildProgressMessageId: currentMessageId };
        } catch (error) {
            const message = String(error?.message || "");
            if (!/message is not modified/i.test(message)) {
                console.warn("Failed to edit build progress message", {
                    chatId,
                    messageId: currentMessageId,
                    error: message
                });
            }
        }
    }

    const sent = await sendMessage(chatId, text);
    return { ...draft, buildProgressMessageId: sent?.message_id || null };
}

async function clearBuildProgressMessage(chatId, draft) {
    const messageId = draft.buildProgressMessageId || null;
    if (messageId) {
        try {
            await deleteMessage(chatId, messageId);
        } catch (error) {
            console.warn("Failed to delete build progress message", {
                chatId,
                messageId,
                error: String(error?.message || error)
            });
        }
    }

    const nextDraft = { ...draft, buildProgressMessageId: null };
    setDraft(chatId, nextDraft);
    return nextDraft;
}

async function runBuildAndReply(message, draft, payload, commandLabel) {
    const chatId = message.chat.id;
    let workingDraft = { ...draft, stage: STAGES.BUILDING, lastPayload: payload };
    setDraft(chatId, workingDraft);
    workingDraft = await updateBuildProgressMessage(chatId, workingDraft, {
        emoji: "⚙️",
        text: "Будую таблицю"
    }, "Це займе приблизно 30 секунд.");
    setDraft(chatId, workingDraft);
    workingDraft = await updateBuildProgressMessage(chatId, workingDraft, {
        emoji: "🧱",
        text: "Готую структуру"
    }, "Планую аркуші, зв'язки та базові налаштування.");
    setDraft(chatId, workingDraft);

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

            let buildUiDraft = await updateBuildProgressMessage(chatId, updatedDraft, {
                emoji: "🔍",
                text: "Перевіряю файл"
            }, "Дивлюсь формули і цілісність таблиці.");
            setDraft(chatId, buildUiDraft);

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
                    buildUiDraft = await updateBuildProgressMessage(chatId, getDraft(chatId), {
                        emoji: "🩺",
                        text: "Знайшов технічний збій у формулах"
                    }, "Пробую виправити автоматично.");
                    setDraft(chatId, buildUiDraft);
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
                        buildUiDraft = await updateBuildProgressMessage(chatId, getDraft(chatId), {
                            emoji: "✅",
                            text: "Формули виправлено"
                        }, "Завершую налаштування.");
                        setDraft(chatId, buildUiDraft);
                    }
                }

                if (validationResult && validationResult.valid === false) {
                    await clearBuildProgressMessage(chatId, getDraft(chatId));
                    await sendMessage(chatId, joinMessageBlocks([
                        sectionTitle("⚠️", "Є проблема під час фінальної перевірки"),
                        "Таблицю я вже зберіг, але для автоматичної доводки потрібно запустити /retry.",
                        Array.isArray(validationResult.errors) && validationResult.errors.length
                            ? bulletLines(validationResult.errors.slice(0, 2).map((item) => `Помилка: ${item}`))
                            : ""
                    ]));
                    return { handled: true, command: commandLabel, engine: "apps_script", result: build.result, validation: validationResult };
                }
            }

            await clearBuildProgressMessage(chatId, getDraft(chatId));
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

        await clearBuildProgressMessage(chatId, updatedDraft);
        await sendMessage(chatId, formatLegacyResult(build.result, updatedDraft));
        return { handled: true, command: commandLabel, engine: "legacy", result: build.result };
    } catch (error) {
        const failedDraft = { ...getDraft(chatId), stage: STAGES.CONFIRMING, lastPayload: payload };
        setDraft(chatId, failedDraft);
        await clearBuildProgressMessage(chatId, failedDraft);
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
        const questionBudget = Math.max(0, MAX_TOTAL_QUESTIONS - Number(getDraft(chatId).questionsCount || getDraft(chatId).askedQuestionsCount || 0));
        const ai = await planQuestionsFromFreeText(tz, { analysis, defaultQuestions, questionBudget });
        const limitedQuestions = (ai.questions.length > 0 ? ai.questions : defaultQuestions).slice(0, questionBudget);
        return {
            message: ai.message || buildArchitectureMessage(analysis),
            questions: limitedQuestions
        };
    } catch (error) {
        disableAiForChat(chatId, currentDraft, error?.message);
        const questionBudget = Math.max(0, MAX_TOTAL_QUESTIONS - Number(getDraft(chatId).questionsCount || getDraft(chatId).askedQuestionsCount || 0));
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
        inputMode: "free_text",
        customMode: true,
        customPlanNotes: rawText,
        questionQueue: questions,
        questionsQueue: questions,
        pendingQuestions,
        answers: {},
        resolvedAnswers: {},
        skippedKeys: [],
        askedQuestionsCount: Number(draft.questionsCount || draft.askedQuestionsCount || 0) + pendingQuestions.length,
        questionsCount: Number(draft.questionsCount || draft.askedQuestionsCount || 0) + pendingQuestions.length
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
        return {
            tz: parsed.tz,
            inputMode: "tz",
            parser: parsed.language || "structured"
        };
    }

    const heuristicCashflow = parseCashflowHeuristicFromText(message.text || "");
    if (heuristicCashflow) {
        return {
            tz: heuristicCashflow,
            inputMode: "free_text",
            parser: "heuristic"
        };
    }

    if (!shouldUseAi(draft)) {
        return null;
    }

    try {
        return {
            tz: await parseFreeText(message.text || ""),
            inputMode: "free_text",
            parser: "ai"
        };
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

function applyCriticalAnswerSideEffects(draft, updatedAnswers, paymentCalendarRules = []) {
    const extracted = { ...(draft.extracted || {}) };
    const answers = updatedAnswers || {};

    const answeredType = normalizeReportType(answers.report_type);
    if (answeredType) {
        extracted.report_type = answeredType;
    }

    const freeTextConfirmation = normalizeText(answers.free_text_articles_confirm);
    if (freeTextConfirmation) {
        if (asYesNo(freeTextConfirmation)) {
            extracted._articles_confirmed = true;
        } else {
            const correctedArticles = parseArticlesSeedAnswer(freeTextConfirmation);
            if (correctedArticles && (correctedArticles.inflows.length || correctedArticles.outflows.length)) {
                extracted.inflows = correctedArticles.inflows;
                extracted.outflows = correctedArticles.outflows;
                extracted._articles_confirmed = true;
            }
        }
    }

    if (!hasAtLeastOneArticle(extracted) && normalizeText(answers.articles_seed)) {
        const correctedArticles = parseArticlesSeedAnswer(answers.articles_seed);
        if (correctedArticles && (correctedArticles.inflows.length || correctedArticles.outflows.length)) {
            extracted.inflows = correctedArticles.inflows;
            extracted.outflows = correctedArticles.outflows;
        }
    }

    (Array.isArray(extracted.outflows) ? extracted.outflows : []).forEach((item, index) => {
        const costType = normalizeCostTypeAnswer(answers[`cost_type_${index}`]);
        if (costType) {
            item.cost_type = costType;
        }

        const recognitionMoment = normalizeRecognitionMomentAnswer(answers[`recognition_moment_${index}`]);
        if (recognitionMoment) {
            item.recognition_moment = recognitionMoment;
        }
    });

    if (isPaymentCalendarRequestedTz(extracted)) {
        const ruleMap = new Map(normalizeCalendarRules(paymentCalendarRules).map((item) => [item.name.toLowerCase(), item]));
        extracted.outflows = (Array.isArray(extracted.outflows) ? extracted.outflows : []).map((item) => {
            const articleName = normalizeText(item.article || item.name);
            const matchedRule = ruleMap.get(articleName.toLowerCase());
            return {
                ...item,
                is_fixed: Boolean(matchedRule),
                typical_day: matchedRule ? matchedRule.typical_day : null
            };
        });
    }

    return extracted;
}

function ensureBuildMinimum(tz) {
    const next = { ...(tz || {}) };
    if (!normalizeReportType(next.report_type)) {
        next.report_type = "cashflow";
    }
    if (isPaymentCalendarRequestedTz(next)) {
        next.report_type = "cashflow";
    }
    if (next._requires_article_confirmation && next._articles_confirmed !== true) {
        next._articles_confirmed = false;
    }

    const inflows = Array.isArray(next.inflows) ? next.inflows : [];
    const outflows = Array.isArray(next.outflows) ? next.outflows : [];
    if (inflows.length + outflows.length === 0) {
        next.inflows = [{ article: "Оплата від клієнтів", responsible: "Owner", ops_per_month: 10, has_sheets_access: true }];
        next.outflows = [{ article: "Інші витрати", responsible: "Owner", ops_per_month: 10, has_sheets_access: true }];
    }

    if (isPaymentCalendarRequestedTz(next)) {
        next.outflows = (Array.isArray(next.outflows) ? next.outflows : []).map((item) => ({
            ...item,
            is_fixed: item.is_fixed === true,
            typical_day: Number.isInteger(Number(item.typical_day)) ? Number(item.typical_day) : null
        }));
    }

    return next;
}

function deterministicPayloadSelfCheck(payload) {
    const missing = [];
    if (!normalizeReportType(payload?.report_type)) missing.push("report_type");
    const inflows = Array.isArray(payload?.articles?.inflows) ? payload.articles.inflows : [];
    const outflows = Array.isArray(payload?.articles?.outflows) ? payload.articles.outflows : [];
    if (inflows.length + outflows.length === 0) missing.push("articles");
    if (!normalizeText(payload?.business_name)) missing.push("business_name");

    Object.entries(payload?.responsible || {}).forEach(([article, item]) => {
        if (item?.access === false && !normalizeText(item?.input_mode)) {
            missing.push(`input_mode:${article}`);
        }
    });

    if (payload?.report_type === "cashflow_and_pl" && payload?.pl_settings?.project_tracking === true) {
        const projects = Array.isArray(payload?.pl_settings?.projects) ? payload.pl_settings.projects : [];
        if (projects.length === 0) {
            missing.push("pl_projects");
        }
    }

    return {
        ready: missing.length === 0,
        missing
    };
}

async function selfCheckPayload(chatId, draft, payload) {
    if (!shouldUseAi(draft)) {
        return deterministicPayloadSelfCheck(payload);
    }

    try {
        const aiCheck = await runPayloadSelfCheck(payload);
        if (Array.isArray(aiCheck.missing) && aiCheck.missing.length > 0) {
            return {
                ready: false,
                missing: aiCheck.missing
            };
        }
        return {
            ready: Boolean(aiCheck.ready),
            missing: []
        };
    } catch (error) {
        disableAiForChat(chatId, draft, error?.message);
        return deterministicPayloadSelfCheck(payload);
    }
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

    const parsedInput = await parseTzFromAnyText(message, chatId, draft);
    const tz = parsedInput?.tz || null;
    const inputMode = parsedInput?.inputMode || "free_text";
    const routing = detectRoutingMode(text, tz);

    if (inputMode !== "tz" && routing.mode === "custom") {
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
    if (draft.inputClassification === "payment_calendar_cashflow_tz") {
        preparedTz.report_type = "cashflow";
        preparedTz._payment_calendar_requested = true;
    }
    if (inputMode === "free_text") {
        preparedTz._requires_article_confirmation = true;
        preparedTz._articles_confirmed = false;
    }

    const analysis = analyzeArchitecture(preparedTz);
    const businessNameResolved = await resolveBusinessName(preparedTz, text, message, draft, chatId);
    const resolvedAnswers = {};
    const skippedKeys = [];
    const questionsQueue = buildQuestionQueue(preparedTz, analysis, resolvedAnswers, skippedKeys, draft.questionsCount || draft.askedQuestionsCount);
    const aiBundle = inputMode === "tz"
        ? {
            message: buildArchitectureMessage(analysis),
            questions: questionsQueue
        }
        : await buildClarificationWithAi(chatId, preparedTz, analysis, questionsQueue);
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
            inputMode,
            inputClassification: draft.inputClassification || null,
            resolvedAnswers,
            skippedKeys,
            answers: {},
            questionQueue: [],
            questionsQueue: [],
            pendingQuestions: []
        };
        const payload = buildPayloadFromTzDraft(directDraft, message);
        const selfCheck = await selfCheckPayload(chatId, directDraft, payload);
        if (!selfCheck.ready) {
            await sendMessage(chatId, joinMessageBlocks([
                sectionTitle("🧩", "Потрібно ще трохи даних"),
                bulletLines(selfCheck.missing.map((item) => `Не вистачає: ${item}`))
            ]));
            return { handled: true, command: "payload_not_ready" };
        }
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
        inputMode,
        inputClassification: draft.inputClassification || null,
        resolvedAnswers,
        skippedKeys,
        questionQueue: aiBundle.questions,
        questionsQueue: aiBundle.questions,
        pendingQuestions,
        askedQuestionsCount: Number(draft.questionsCount || draft.askedQuestionsCount || 0) + pendingQuestions.length,
        questionsCount: Number(draft.questionsCount || draft.askedQuestionsCount || 0) + pendingQuestions.length,
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
        const activeBatch = (draft.pendingQuestions || []).length > 0 ? draft.pendingQuestions : nextQuestionBatch(draft.questionQueue || []);
        await sendMessage(chatId, buildClarificationHelp(text, activeBatch));
        if (activeBatch.length > 0) {
            await sendMessage(chatId, buildQuestionsMessage("Повертаємось до уточнень:", activeBatch));
        }
        return { handled: true, command: "clarification_help" };
    }

    const firstPendingQuestion = (Array.isArray(draft.pendingQuestions) && draft.pendingQuestions.length > 0
        ? draft.pendingQuestions[0]
        : (Array.isArray(draft.questionQueue) ? draft.questionQueue[0] : null)) || null;
    const updated = firstPendingQuestion?.key === "payment_calendar_fixed_rules"
        ? applyAnswers(draft, message.text || "")
        : ((await resolveClarificationAnswersWithAi(chatId, draft, message.text || ""))
            || applyAnswers(draft, message.text || ""));
    const paymentCalendarRules = updated.resolvedAnswers.payment_calendar_fixed_rules !== undefined
        ? await resolvePaymentCalendarRules(chatId, draft, updated.resolvedAnswers.payment_calendar_fixed_rules)
        : [];
    const extractedAfterAnswers = applyCriticalAnswerSideEffects(draft, updated.resolvedAnswers, paymentCalendarRules);
    const rebuiltQueue = buildQuestionQueue(
        extractedAfterAnswers,
        draft.analysis,
        updated.resolvedAnswers,
        updated.skippedKeys,
        draft.questionsCount || draft.askedQuestionsCount
    );
    const pendingQuestions = nextQuestionBatch(rebuiltQueue);
    const nextQuestionsCount = pendingQuestions.length > 0
        ? Number(draft.questionsCount || draft.askedQuestionsCount || 0) + pendingQuestions.length
        : Number(draft.questionsCount || draft.askedQuestionsCount || 0);

    const nextDraft = {
        ...draft,
        answers: updated.resolvedAnswers,
        resolvedAnswers: updated.resolvedAnswers,
        skippedKeys: updated.skippedKeys,
        questionQueue: rebuiltQueue,
        questionsQueue: rebuiltQueue,
        pendingQuestions,
        stage: pendingQuestions.length > 0 ? STAGES.COLLECTING : STAGES.CONFIRMING,
        extracted: extractedAfterAnswers,
        report_type: normalizeReportType(extractedAfterAnswers.report_type) || draft.report_type,
        askedQuestionsCount: nextQuestionsCount,
        questionsCount: nextQuestionsCount,
        clarifications: [...(draft.clarifications || []), normalizeText(message.text)]
    };

    if (pendingQuestions.length > 0) {
        setDraft(chatId, nextDraft);
        await sendMessage(chatId, buildQuestionsMessage("Кілька уточнень:", pendingQuestions));
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
    const selfCheck = await selfCheckPayload(chatId, readyDraft, finalPayload);
    if (!selfCheck.ready) {
        setDraft(chatId, {
            ...readyDraft,
            stage: STAGES.COLLECTING,
            questionQueue: [],
            questionsQueue: [],
            pendingQuestions: []
        });
        await sendMessage(chatId, joinMessageBlocks([
            sectionTitle("🧩", "Payload ще не готовий"),
            bulletLines(selfCheck.missing.map((item) => `Не вистачає: ${item}`)),
            "Напиши відсутні дані одним повідомленням."
        ]));
        return { handled: true, command: "payload_self_check_failed" };
    }
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
        setDraft(chatId, {
            ...draft,
            stage: STAGES.COLLECTING,
            questionQueue: [],
            questionsQueue: [],
            pendingQuestions: []
        });
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

    const nextDraft = createEmptyDraft(draft);
    nextDraft.activeTableId = draft.activeTableId || null;
    nextDraft.activeTableName = draft.activeTableName || null;
    nextDraft.spreadsheet_id = draft.activeTableId || null;
    setDraft(chatId, nextDraft);

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

    const rawMessage = extractMessage(update);
    const message = rawMessage ? await hydrateMessageTextFromDocument(rawMessage) : rawMessage;
    if (!message || !message.chat) return { handled: false, reason: "No message context" };

    const chatId = getChatId(message);
    if (!chatId) return { handled: false, reason: "No chat id" };

    return runWithChatQueue(chatId, async () => {
        const text = message.text || "";
        const command = extractCommand(text);
        const commandArg = extractCommandArg(text);
        const draft = getDraft(chatId);
        const inputKind = classifyInput(text, {
            stage: draft.stage,
            questionQueue: draft.questionQueue || draft.questionsQueue || [],
            reportType: draft.report_type,
            fileName: message.document?.file_name || ""
        });

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

        if (inputKind === "clarification_answer") {
            return handleCollectingAnswers(message, draft);
        }

        if (draft.stage === STAGES.CONFIRMING) {
            return handleConfirmation(message, draft);
        }

        if (draft.stage === STAGES.EDITING && shouldStartNewBuildFromMessage(message, draft)) {
            return handleTzCapture(message, {
                ...draft,
                stage: STAGES.IDLE,
                inputClassification: inputKind
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

        return handleTzCapture(message, {
            ...draft,
            inputClassification: inputKind
        });
    });
}

module.exports = {
    handleTelegramUpdate
};
