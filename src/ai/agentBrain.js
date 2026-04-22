const DEFAULT_PROVIDER = process.env.AGENT_LLM_PROVIDER || "anthropic";
const DEFAULT_TEMPERATURE = Number(process.env.AGENT_LLM_TEMPERATURE || 0.2);

function getProvider() {
    if (DEFAULT_PROVIDER === "none") {
        return "none";
    }

    if (DEFAULT_PROVIDER === "anthropic" && process.env.ANTHROPIC_API_KEY) {
        return "anthropic";
    }

    if (DEFAULT_PROVIDER === "openrouter" && process.env.OPENROUTER_API_KEY) {
        return "openrouter";
    }

    if (process.env.ANTHROPIC_API_KEY) {
        return "anthropic";
    }

    if (process.env.OPENROUTER_API_KEY) {
        return "openrouter";
    }

    return "none";
}

function getModel(provider) {
    if (provider === "anthropic") {
        return process.env.ANTHROPIC_MODEL || "claude-3-haiku-20240307";
    }

    if (provider === "openrouter") {
        return process.env.OPENROUTER_MODEL || "google/gemini-2.0-flash-lite-001";
    }

    return "none";
}

function isEnabled() {
    return getProvider() !== "none";
}

function getConfigSummary() {
    const provider = getProvider();
    return {
        enabled: provider !== "none",
        provider,
        model: getModel(provider)
    };
}

function getAnthropicFallbackModels(primaryModel) {
    const candidates = [
        "claude-3-5-haiku-20241022",
        "claude-3-5-sonnet-20241022",
        "claude-3-haiku-20240307"
    ];

    return candidates.filter((model) => model !== primaryModel);
}

async function callAnthropicOnce({ apiKey, model, systemPrompt, userPrompt }) {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
            "content-type": "application/json",
            "x-api-key": apiKey,
            "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify({
            model,
            max_tokens: 1200,
            temperature: DEFAULT_TEMPERATURE,
            system: systemPrompt,
            messages: [
                {
                    role: "user",
                    content: userPrompt
                }
            ]
        })
    });

    const data = await response.json();
    return { response, data };
}

async function callAnthropicJson({ systemPrompt, userPrompt }) {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
        throw new Error("ANTHROPIC_API_KEY is missing");
    }

    const primaryModel = getModel("anthropic");
    const fallbackModels = getAnthropicFallbackModels(primaryModel);
    const modelsToTry = [primaryModel].concat(fallbackModels);

    let lastError = null;
    for (const model of modelsToTry) {
        const { response, data } = await callAnthropicOnce({ apiKey, model, systemPrompt, userPrompt });
        if (response.ok) {
            const text = Array.isArray(data.content)
                ? data.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")
                : "";

            return parseJsonFromText(text);
        }

        const message = String(data?.error?.message || "");
        const type = String(data?.error?.type || "");
        const isModelNotFound = response.status === 404 && (type === "not_found_error" || /model\s*:/i.test(message));
        lastError = new Error(`Anthropic error ${response.status}: ${JSON.stringify(data)}`);

        if (!isModelNotFound) {
            throw lastError;
        }

        console.warn("Anthropic model fallback", { failedModel: model, nextModel: fallbackModels[0] || null, reason: message });
    }

    throw lastError || new Error("Anthropic model fallback failed");
}

async function callOpenRouterJson({ systemPrompt, userPrompt }) {
    const apiKey = process.env.OPENROUTER_API_KEY;
    if (!apiKey) {
        throw new Error("OPENROUTER_API_KEY is missing");
    }

    const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
        method: "POST",
        headers: {
            "content-type": "application/json",
            authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
            model: getModel("openrouter"),
            temperature: DEFAULT_TEMPERATURE,
            messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: userPrompt }
            ]
        })
    });

    const data = await response.json();
    if (!response.ok) {
        throw new Error(`OpenRouter error ${response.status}: ${JSON.stringify(data)}`);
    }

    const text = data?.choices?.[0]?.message?.content || "";
    return parseJsonFromText(text);
}

function parseJsonFromText(text) {
    const raw = String(text || "").trim();
    if (!raw) {
        throw new Error("LLM returned empty response");
    }

    const directTry = tryParseJson(raw);
    if (directTry) {
        return directTry;
    }

    const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced) {
        const inside = tryParseJson(fenced[1]);
        if (inside) {
            return inside;
        }
    }

    const fromFirstBrace = raw.slice(raw.indexOf("{"));
    const fromBrace = tryParseJson(fromFirstBrace);
    if (fromBrace) {
        return fromBrace;
    }

    throw new Error("LLM response is not valid JSON");
}

function isAnthropicModelNotFoundError(error) {
    const message = String(error?.message || "");
    return /Anthropic error 404/i.test(message) && (/not_found_error/i.test(message) || /model\s*:/i.test(message));
}

function tryParseJson(text) {
    try {
        return JSON.parse(String(text || "").trim());
    } catch {
        return null;
    }
}

async function callJsonTask({ systemPrompt, userPrompt }) {
    const provider = getProvider();
    if (provider === "anthropic") {
        try {
            return await callAnthropicJson({ systemPrompt, userPrompt });
        } catch (error) {
            if (isAnthropicModelNotFoundError(error) && process.env.OPENROUTER_API_KEY) {
                console.warn("Anthropic unavailable for current model, falling back to OpenRouter for this request");
                return callOpenRouterJson({ systemPrompt, userPrompt });
            }

            throw error;
        }
    }

    if (provider === "openrouter") {
        return callOpenRouterJson({ systemPrompt, userPrompt });
    }

    throw new Error("LLM provider is not configured");
}

async function generateClarificationBundle(context) {
    const systemPrompt = [
        "You are a finance table assistant for Telegram.",
        "Return only JSON.",
        "Use concise Ukrainian text without markdown.",
        "Use simple user-facing language.",
        "If you must use a specific finance or spreadsheet term, immediately explain it in plain words in the same sentence.",
        "Ask only critical missing questions for table build.",
        "Group related topics into one question.",
        "Each message must include at most 3-4 grouped questions.",
        "Include an example answer format in message.",
        "Respect total question budget from context.questionBudget."
    ].join(" ");

    const userPrompt = `Context JSON:\n${JSON.stringify(context, null, 2)}\n\nReturn JSON:\n{\n  \"message\": \"short user-facing summary\",\n  \"questions\": [{\"key\":\"...\",\"text\":\"...\"}]\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        message: String(data.message || "").trim(),
        questions: Array.isArray(data.questions)
            ? data.questions
                .map((item) => ({
                    key: String(item.key || "").trim(),
                    text: String(item.text || "").trim()
                }))
                .filter((item) => item.key && item.text)
            : []
    };
}

async function resolveClarification(answer, queue, resolved, extracted) {
    const systemPrompt = [
        "You resolve a user's free-text answer to clarification questions for a Telegram financial table builder.",
        "Return only JSON.",
        "Use the provided question keys exactly.",
        "You may apply one general answer to multiple related questions if the user clearly means all of them.",
        "If centralized payment through accountant is chosen, related input-method questions can be skipped.",
        "Do not invent new keys.",
        "If the user did not answer a question, leave it unresolved.",
        "skip_keys should contain only questions that became irrelevant because of the resolved answers."
    ].join(" ");

    const userPrompt = `Context:\n${JSON.stringify({
        user_answer: answer,
        question_queue: Array.isArray(queue) ? queue : [],
        resolved_answers: resolved || {},
        extracted_tz: extracted || {}
    }, null, 2)}\n\nReturn JSON:\n{\n  "resolved": {"question_key":"normalized value"},\n  "skipped": ["question_key"],\n  "confidence": 0.0,\n  "interpretation": "short explanation"\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        resolved: data.resolved && typeof data.resolved === "object" && !Array.isArray(data.resolved)
            ? Object.fromEntries(
                Object.entries(data.resolved)
                    .map(([key, value]) => [String(key || "").trim(), String(value || "").trim()])
                    .filter(([key, value]) => key && value)
            )
            : {},
        skipped: Array.isArray(data.skipped)
            ? data.skipped.map((item) => String(item || "").trim()).filter(Boolean)
            : [],
        confidence: Number.isFinite(Number(data.confidence)) ? Number(data.confidence) : 0,
        interpretation: String(data.interpretation || "").trim()
    };
}

async function generateClarificationAnswerResolution(input) {
    const result = await resolveClarification(
        input?.user_answer,
        input?.all_questions || input?.question_queue || input?.pending_questions || [],
        input?.current_answers || input?.resolved_answers || {},
        input?.extracted_tz || input?.extracted || {}
    );

    return {
        resolved_answers: result.resolved,
        skip_keys: result.skipped,
        confidence: result.confidence,
        notes: result.interpretation
    };
}

async function generateBusinessNameFromText(input) {
    const systemPrompt = [
        "You generate a short business name from user description.",
        "Return only JSON.",
        "Name must be Ukrainian and 3-4 words max.",
        "No quotes, no punctuation at edges."
    ].join(" ");

    const userPrompt = `Business description:\n${String(input || "")}\n\nReturn JSON:\n{\n  "business_name": "..."\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    const name = String(data.business_name || "").trim();
    return name;
}

async function generateCustomTableBlueprint(input) {
    const systemPrompt = [
        "You are a product architect for spreadsheet systems.",
        "Return only JSON.",
        "Design a practical table blueprint from user requirements.",
        "Keep concise but complete.",
        "Use Ukrainian text in labels."
    ].join(" ");

    const userPrompt = `Input:\n${JSON.stringify(input || {}, null, 2)}\n\nReturn JSON:\n{\n  "title": "...",\n  "goal": "...",\n  "sheet_plan": [{"name":"...","purpose":"...","editable_by":["..."]}],\n  "fields": [{"sheet":"...","name":"...","type":"text|number|date|select|formula","required":true}],\n  "formulas": [{"sheet":"...","cell":"A1","formula":"=...","description":"..."}],\n  "roles": [{"role":"...","can_edit":["..."],"can_view":["..."]}],\n  "automation": [{"trigger":"...","action":"..."}],\n  "risks": ["..."],\n  "open_questions": ["..."]\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        title: String(data.title || "Кастомна таблиця").trim(),
        goal: String(data.goal || "").trim(),
        sheet_plan: Array.isArray(data.sheet_plan) ? data.sheet_plan : [],
        fields: Array.isArray(data.fields) ? data.fields : [],
        formulas: Array.isArray(data.formulas) ? data.formulas : [],
        roles: Array.isArray(data.roles) ? data.roles : [],
        automation: Array.isArray(data.automation) ? data.automation : [],
        risks: Array.isArray(data.risks) ? data.risks.map((v) => String(v)) : [],
        open_questions: Array.isArray(data.open_questions) ? data.open_questions.map((v) => String(v)) : []
    };
}

async function generateUpdatePayloadFromText(input) {
    const systemPrompt = [
        "You convert user edit requests to update_table payload.",
        "Return only JSON.",
        "If data is missing, return missing list and update_payload null."
    ].join(" ");

    const userPrompt = `Context:\n${JSON.stringify(input, null, 2)}\n\nReturn JSON:\n{\n  \"missing\": [\"...\"],\n  \"message_to_user\": \"...\",\n  \"update_payload\": {\"action\":\"update_table\",\"spreadsheet_id\":\"...\",\"changes\":[...]}\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        missing: Array.isArray(data.missing) ? data.missing.map((v) => String(v)) : [],
        message_to_user: String(data.message_to_user || "").trim(),
        update_payload: data.update_payload && typeof data.update_payload === "object" ? data.update_payload : null
    };
}

function normalizeParsedItem(item = {}) {
    const article = String(item.article || "").trim();
    const responsible = item.responsible == null ? null : String(item.responsible || "").trim() || null;
    const opsRaw = item.ops_per_month;
    const opsPerMonth = Number.isFinite(Number(opsRaw)) ? Number(opsRaw) : null;
    const hasSheetsAccess = typeof item.has_sheets_access === "boolean" ? item.has_sheets_access : null;
    const recognitionMoment = item.recognition_moment == null ? null : String(item.recognition_moment || "").trim() || null;
    const costType = item.cost_type == null ? null : String(item.cost_type || "").trim() || null;

    return {
        article: article || null,
        responsible,
        ops_per_month: opsPerMonth,
        has_sheets_access: hasSheetsAccess,
        recognition_moment: recognitionMoment,
        cost_type: costType
    };
}

function normalizeParsedReportType(value) {
    const text = String(value || "").trim().toLowerCase();
    return ["cashflow", "pl", "balance", "cashflow_and_pl", "dashboard"].includes(text)
        ? text
        : null;
}

function normalizeParsedExtraction(data = {}) {
    return {
        report_type: normalizeParsedReportType(data.report_type) || "cashflow",
        business_name: String(data.business_name || "").trim() || null,
        inflows: Array.isArray(data.inflows) ? data.inflows.map(normalizeParsedItem).filter((item) => item.article) : [],
        outflows: Array.isArray(data.outflows) ? data.outflows.map(normalizeParsedItem).filter((item) => item.article) : [],
        confidence_notes: Array.isArray(data.confidence_notes)
            ? data.confidence_notes.map((item) => String(item || "").trim()).filter(Boolean)
            : []
    };
}

async function selfValidateFreeText(inputText, extracted) {
    const systemPrompt = [
        "You validate parsed financial table data extracted from Ukrainian free text.",
        "Return only JSON.",
        "If an article is a sentence fragment or copied text chunk, replace it with a short operation title or null.",
        "If a responsible person is a sentence or description, replace it with a short role or null.",
        "Do not invent details you cannot justify from the original text."
    ].join(" ");

    const userPrompt = `Original text:\n${String(inputText || "")}\n\nParsed result:\n${JSON.stringify(extracted || {}, null, 2)}\n\nReturn JSON:\n{\n  "valid": true,\n  "fixed": {\n    "report_type": "cashflow|pl|balance|cashflow_and_pl|dashboard|null",\n    "business_name": "...|null",\n    "inflows": [{"article":"...|null","responsible":"...|null","ops_per_month":0,"has_sheets_access":true,"recognition_moment":"...|null"}],\n    "outflows": [{"article":"...|null","responsible":"...|null","ops_per_month":0,"has_sheets_access":true,"cost_type":"...|null","recognition_moment":"...|null"}],\n    "confidence_notes": ["..."]\n  },\n  "issues_found": ["..."]\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        valid: Boolean(data.valid),
        fixed: data.fixed && typeof data.fixed === "object" ? normalizeParsedExtraction(data.fixed) : null,
        issues_found: Array.isArray(data.issues_found)
            ? data.issues_found.map((item) => String(item || "").trim()).filter(Boolean)
            : []
    };
}

async function parseFreeText(input) {
    const systemPrompt = [
        "You extract structured financial-table data from Ukrainian free text.",
        "Return only JSON.",
        "Prefer report_type=cashflow unless the user clearly asks for P&L, balance, or a combined cashflow_and_pl report.",
        "Article must be a short financial operation name, usually 2-5 words, not a copied sentence.",
        "Responsible person must be a short name or role, not a sentence.",
        "If you are not sure, return null instead of guessing.",
        "If data is missing keep arrays empty but preserve shape."
    ].join(" ");

    const userPrompt = `User text:\n${String(input || "")}\n\nReturn JSON:\n{\n  "report_type": "cashflow|pl|balance|cashflow_and_pl|dashboard|null",\n  "business_name": "...|null",\n  "inflows": [{"article":"...|null","responsible":"...|null","ops_per_month":0,"has_sheets_access":true,"recognition_moment":"payment_date|act_date|accrual_date|null"}],\n  "outflows": [{"article":"...|null","responsible":"...|null","ops_per_month":0,"has_sheets_access":true,"cost_type":"cogs|opex|owner|tax|null","recognition_moment":"payment_date|act_date|accrual_date|null"}],\n  "confidence_notes": ["..."]\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });
    const extracted = normalizeParsedExtraction(data);
    const validation = await selfValidateFreeText(input, extracted);

    if (!validation.valid && validation.fixed) {
        return {
            ...validation.fixed,
            _was_corrected: true,
            _validation_issues: validation.issues_found
        };
    }

    return {
        ...extracted,
        _was_corrected: false,
        _validation_issues: validation.issues_found
    };
}

async function generateTzFromFreeText(input) {
    return parseFreeText(input);
}

async function planQuestionsFromFreeText(extracted, context = {}) {
    return generateClarificationBundle({
        ...context,
        tz: extracted,
        mode: "free_text"
    });
}

async function runPayloadSelfCheck(payload) {
    const systemPrompt = [
        "You verify whether a financial table payload is complete enough for build.",
        "Return only JSON.",
        "Be strict but concise."
    ].join(" ");

    const userPrompt = `Payload:\n${JSON.stringify(payload || {}, null, 2)}\n\nReturn JSON:\n{\n  "ready": true,\n  "missing": ["..."]\n}`;
    const data = await callJsonTask({ systemPrompt, userPrompt });
    return {
        ready: Boolean(data.ready),
        missing: Array.isArray(data.missing) ? data.missing.map((item) => String(item || "").trim()).filter(Boolean) : []
    };
}

function normalizeCalendarRule(rule = {}) {
    const name = String(rule.name || rule.article || "").trim();
    const typicalDay = Number(rule.typical_day);
    return {
        name,
        typical_day: Number.isInteger(typicalDay) && typicalDay >= 1 && typicalDay <= 31 ? typicalDay : null
    };
}

function deterministicParsePaymentCalendarFixedCosts(answer, outflows = []) {
    const source = String(answer || "").trim();
    if (!source || /^(пропустити|skip|unknown|не знаю)$/i.test(source)) {
        return [];
    }

    const knownOutflows = (Array.isArray(outflows) ? outflows : [])
        .map((item) => String(item?.article || item?.name || item || "").trim())
        .filter(Boolean);

    const chunks = source
        .split(/[\n;,]+/)
        .map((item) => item.trim())
        .filter(Boolean);

    const rules = [];
    chunks.forEach((chunk) => {
        const match = chunk.match(/(\d{1,2})\s*(?:-?го)?\s*$/i);
        if (!match) return;

        const rawName = String(chunk.slice(0, match.index || 0) || "")
            .replace(/[\s—:.-]+$/g, "")
            .trim()
            .toLowerCase();
        const typicalDay = Number(match[1]);
        if (!Number.isInteger(typicalDay) || typicalDay < 1 || typicalDay > 31) return;
        if (!rawName) return;

        const resolvedName = knownOutflows.find((name) => {
            const normalizedName = name.toLowerCase();
            return normalizedName.includes(rawName) || rawName.includes(normalizedName);
        }) || String(chunk.slice(0, match.index || 0) || "").replace(/[\s—:.-]+$/g, "").trim();

        rules.push({ name: resolvedName, typical_day: typicalDay });
    });

    return rules.map(normalizeCalendarRule).filter((item) => item.name && item.typical_day);
}

async function parsePaymentCalendarFixedCosts(answer, outflows = []) {
    const fallback = deterministicParsePaymentCalendarFixedCosts(answer, outflows);
    if (!isEnabled()) {
        return fallback;
    }

    const systemPrompt = [
        "You map a user's Ukrainian answer about fixed monthly expense dates to known outflow article names for a payment calendar.",
        "Return only JSON.",
        "Use only outflow names from the provided list.",
        "If the user says to skip or does not know, return an empty array.",
        "typical_day must be an integer from 1 to 31."
    ].join(" ");

    const userPrompt = `Known outflows:\n${JSON.stringify((Array.isArray(outflows) ? outflows : []).map((item) => String(item?.article || item?.name || item || "").trim()).filter(Boolean), null, 2)}\n\nUser answer:\n${String(answer || "")}\n\nReturn JSON:\n{\n  "fixed_costs": [{"name":"...","typical_day":10}]\n}`;

    try {
        const data = await callJsonTask({ systemPrompt, userPrompt });
        return Array.isArray(data.fixed_costs)
            ? data.fixed_costs.map(normalizeCalendarRule).filter((item) => item.name && item.typical_day)
            : fallback;
    } catch {
        return fallback;
    }
}

module.exports = {
    isEnabled,
    getConfigSummary,
    generateClarificationBundle,
    resolveClarification,
    generateClarificationAnswerResolution,
    planQuestionsFromFreeText,
    generateUpdatePayloadFromText,
    parseFreeText,
    generateTzFromFreeText,
    generateBusinessNameFromText,
    generateCustomTableBlueprint,
    runPayloadSelfCheck,
    parsePaymentCalendarFixedCosts
};
