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

async function callAnthropicJson({ systemPrompt, userPrompt }) {
    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) {
        throw new Error("ANTHROPIC_API_KEY is missing");
    }

    const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
            "content-type": "application/json",
            "x-api-key": apiKey,
            "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify({
            model: getModel("anthropic"),
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
    if (!response.ok) {
        throw new Error(`Anthropic error ${response.status}: ${JSON.stringify(data)}`);
    }

    const text = Array.isArray(data.content)
        ? data.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")
        : "";

    return parseJsonFromText(text);
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
        return callAnthropicJson({ systemPrompt, userPrompt });
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
        "Ask only critical missing questions for table build."
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

module.exports = {
    isEnabled,
    getConfigSummary,
    generateClarificationBundle,
    generateUpdatePayloadFromText
};
