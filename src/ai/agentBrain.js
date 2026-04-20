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

async function generateTzFromFreeText(input) {
    const systemPrompt = [
        "You convert unstructured user text into TZ JSON for financial table builder.",
        "Return only JSON.",
        "Prefer report_type=cashflow unless clearly stated otherwise.",
        "If data is missing keep arrays empty but preserve shape."
    ].join(" ");

    const userPrompt = `User text:\n${String(input || "")}\n\nReturn JSON:\n{\n  "report_type": "cashflow|pl|balance|dashboard",\n  "business_name": "...",\n  "inflows": [{"article":"...","responsible":"...","ops_per_month":0,"has_sheets_access":true}],\n  "outflows": [{"article":"...","responsible":"...","ops_per_month":0,"has_sheets_access":true}]\n}`;

    const data = await callJsonTask({ systemPrompt, userPrompt });

    return {
        report_type: String(data.report_type || "cashflow").toLowerCase(),
        business_name: String(data.business_name || "Business").trim(),
        inflows: Array.isArray(data.inflows) ? data.inflows : [],
        outflows: Array.isArray(data.outflows) ? data.outflows : []
    };
}

module.exports = {
    isEnabled,
    getConfigSummary,
    generateClarificationBundle,
    generateUpdatePayloadFromText,
    generateTzFromFreeText,
    generateBusinessNameFromText,
    generateCustomTableBlueprint
};
