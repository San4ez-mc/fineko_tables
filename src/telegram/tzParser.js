function extractTzCodeBlock(text) {
    const source = String(text || "");
    const exactMatch = source.match(/```\s*tz\s*\n([\s\S]*?)```/i);
    if (exactMatch) {
        return {
            hasCodeBlock: true,
            language: "tz",
            content: exactMatch[1].trim()
        };
    }

    const genericMatch = source.match(/```\s*([a-zA-Z0-9_-]+)?\s*\n([\s\S]*?)```/);
    if (genericMatch) {
        return {
            hasCodeBlock: true,
            language: (genericMatch[1] || "").toLowerCase(),
            content: String(genericMatch[2] || "").trim()
        };
    }

    return {
        hasCodeBlock: false,
        language: "",
        content: ""
    };
}

function parseScalar(value) {
    const text = String(value || "").trim();
    if (text === "") {
        return "";
    }

    if (/^(true|false)$/i.test(text)) {
        return text.toLowerCase() === "true";
    }

    if (/^-?\d+(\.\d+)?$/.test(text)) {
        return Number(text);
    }

    return text.replace(/^['"]|['"]$/g, "");
}

function parseTzBlock(content) {
    const lines = String(content || "")
        .split(/\r?\n/)
        .map((line) => line.replace(/\t/g, "  "));

    const data = {};
    let currentSection = "";
    let currentListItem = null;

    for (const rawLine of lines) {
        if (!rawLine.trim() || rawLine.trim().startsWith("#")) {
            continue;
        }

        const indent = rawLine.match(/^\s*/)[0].length;
        const line = rawLine.trim();

        const topLevelMatch = indent === 0 ? line.match(/^([^:]+):\s*(.*)$/) : null;
        if (topLevelMatch) {
            const key = topLevelMatch[1].trim();
            const value = topLevelMatch[2];

            if (value === "") {
                currentSection = key;
                currentListItem = null;
                if (!Array.isArray(data[currentSection])) {
                    data[currentSection] = [];
                }
            } else {
                data[key] = parseScalar(value);
                currentSection = "";
                currentListItem = null;
            }
            continue;
        }

        if (line.startsWith("- ") && currentSection) {
            const itemText = line.slice(2).trim();
            if (itemText.includes(":")) {
                const index = itemText.indexOf(":");
                const itemKey = itemText.slice(0, index).trim();
                const itemValue = itemText.slice(index + 1).trim();
                currentListItem = { [itemKey]: parseScalar(itemValue) };
                data[currentSection].push(currentListItem);
            } else {
                currentListItem = parseScalar(itemText);
                data[currentSection].push(currentListItem);
            }
            continue;
        }

        const nestedMatch = indent > 0 ? line.match(/^([^:]+):\s*(.*)$/) : null;
        if (nestedMatch && currentSection && currentListItem && typeof currentListItem === "object") {
            const nestedKey = nestedMatch[1].trim();
            const nestedValue = nestedMatch[2].trim();
            currentListItem[nestedKey] = parseScalar(nestedValue);
            continue;
        }
    }

    if (!Object.keys(data).length) {
        throw new Error("TZ block is empty or invalid");
    }

    return data;
}

function parseTzMessage(text) {
    const codeBlock = extractTzCodeBlock(text);
    if (!codeBlock.hasCodeBlock) {
        return {
            detected: false,
            parsed: false,
            reason: "no_code_block"
        };
    }

    try {
        const tz = parseTzBlock(codeBlock.content);
        return {
            detected: true,
            parsed: true,
            tz,
            language: codeBlock.language || "tz"
        };
    } catch (error) {
        return {
            detected: true,
            parsed: false,
            reason: error.message,
            language: codeBlock.language || ""
        };
    }
}

function toNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
}

function pickName(item = {}) {
    return item.responsible || item.owner || "Невідомо";
}

function classifyMode(totalOps, peopleWithoutAccessCount) {
    if (totalOps > 200 || peopleWithoutAccessCount >= 3) {
        return "C";
    }

    if ((totalOps >= 50 && totalOps <= 200) || peopleWithoutAccessCount > 0) {
        return "B";
    }

    return "A";
}

function analyzeArchitecture(tz) {
    const inflows = Array.isArray(tz.inflows) ? tz.inflows : [];
    const outflows = Array.isArray(tz.outflows) ? tz.outflows : [];
    const allItems = [...inflows, ...outflows];

    const totalOps = allItems.reduce((acc, item) => acc + toNumber(item.ops_per_month), 0);
    const byResponsible = new Map();

    allItems.forEach((item) => {
        const name = pickName(item);
        const ops = toNumber(item.ops_per_month);
        const hasAccess = item.has_sheets_access !== false;

        if (!hasAccess) {
            byResponsible.set(name, (byResponsible.get(name) || 0) + ops);
        }
    });

    const noAccessPeople = Array.from(byResponsible.entries()).map(([name, ops]) => ({ name, ops }));
    const inflowsNoAccessCount = inflows.filter((item) => item.has_sheets_access === false)
        .map((item) => pickName(item)).filter((value, index, arr) => arr.indexOf(value) === index).length;
    const outflowsNoAccessCount = outflows.filter((item) => item.has_sheets_access === false)
        .map((item) => pickName(item)).filter((value, index, arr) => arr.indexOf(value) === index).length;

    const inflowsOps = inflows.reduce((acc, item) => acc + toNumber(item.ops_per_month), 0);
    const outflowsOps = outflows.reduce((acc, item) => acc + toNumber(item.ops_per_month), 0);

    const inflowsMode = classifyMode(inflowsOps, inflowsNoAccessCount);
    const outflowsMode = classifyMode(outflowsOps, outflowsNoAccessCount);

    return {
        totalOps,
        noAccessPeople,
        inflowsMode,
        outflowsMode,
        highOpsItems: allItems
            .filter((item) => toNumber(item.ops_per_month) > 15)
            .map((item) => ({
                article: item.article || item.name || "Стаття",
                responsible: pickName(item),
                ops: toNumber(item.ops_per_month)
            }))
    };
}

module.exports = {
    parseTzMessage,
    analyzeArchitecture
};
