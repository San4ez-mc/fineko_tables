function normalizeText(value) {
    return String(value || "").trim();
}

function articlePerson(item) {
    return normalizeText(item?.responsible || item?.owner || "Співробітник");
}

function articleName(item) {
    return normalizeText(item?.article || item?.name || "Стаття");
}

function hasNoSheetsAccess(item) {
    return item && item.has_sheets_access === false;
}

function reportTypeOf(extracted) {
    return normalizeText(extracted?.report_type).toLowerCase();
}

function moneyFlowCondition(resolvedAnswers, extracted, index) {
    const item = extracted?.outflows?.[index];
    if (!hasNoSheetsAccess(item)) return false;
    return resolvedAnswers[`money_flow_${index}`] === undefined;
}

function methodCondition(resolvedAnswers, extracted, index) {
    const item = extracted?.outflows?.[index];
    if (!hasNoSheetsAccess(item)) return false;
    return resolvedAnswers[`money_flow_${index}`] === "accountable"
        && resolvedAnswers[`no_access_method_${index}`] === undefined;
}

function plCostTypeCondition(resolvedAnswers, extracted, index) {
    const reportType = reportTypeOf(extracted);
    if (!["pl", "cashflow_and_pl"].includes(reportType)) return false;
    return resolvedAnswers[`cost_type_${index}`] === undefined;
}

function plRecognitionCondition(resolvedAnswers, extracted, index) {
    const reportType = reportTypeOf(extracted);
    if (!["pl", "cashflow_and_pl"].includes(reportType)) return false;
    return resolvedAnswers[`recognition_moment_${index}`] === undefined;
}

const questionGraph = {
    "money_flow_{i}": {
        depends: null,
        condition: moneyFlowCondition,
        generate: (context, index) => {
            const item = context?.outflows?.[index] || {};
            return {
                text: `${articleName(item)} — ${articlePerson(item)} сам платить і фіксує, чи подає заявку і бухгалтер проводить оплату?`,
                options: ["Сам платить (підзвітні)", "Через бухгалтера"]
            };
        }
    },
    "no_access_method_{i}": {
        depends: "money_flow_{i}",
        condition: methodCondition,
        generate: (context, index) => {
            const item = context?.outflows?.[index] || {};
            return {
                text: `${articleName(item)} — ${articlePerson(item)} розраховується сам. Як зручніше фіксувати витрати?`,
                options: ["Google Form", "Окремий аркуш"]
            };
        }
    },
    "cost_type_{i}": {
        depends: null,
        condition: plCostTypeCondition,
        generate: (context, index) => {
            const item = context?.outflows?.[index] || {};
            return {
                text: `${articleName(item)} — це прямі витрати на продукт/послугу, операційні витрати, податки чи витрати власника?`,
                options: ["Прямі витрати", "Операційні витрати", "Податки", "Власник"]
            };
        }
    },
    "recognition_moment_{i}": {
        depends: null,
        condition: plRecognitionCondition,
        generate: (context, index) => {
            const item = context?.outflows?.[index] || {};
            return {
                text: `${articleName(item)} — коли визнавати цю статтю в P&L: по оплаті, по акту/накладній чи по нарахуванню?`,
                options: ["По оплаті", "По акту / накладній", "По нарахуванню"]
            };
        }
    }
};

function buildActiveQueue(extracted = {}, resolvedAnswers = {}, skippedKeys = []) {
    const queue = [];
    const skipSet = new Set(Array.isArray(skippedKeys) ? skippedKeys : []);
    const outflows = Array.isArray(extracted?.outflows) ? extracted.outflows : [];

    outflows.forEach((_, index) => {
        Object.entries(questionGraph).forEach(([template, def]) => {
            const key = template.replace("{i}", String(index));
            if (resolvedAnswers[key] !== undefined) return;
            if (skipSet.has(key)) return;
            if (!def.condition(resolvedAnswers, extracted, index)) return;
            queue.push({ key, ...def.generate(extracted, index) });
        });
    });

    return queue;
}

module.exports = {
    questionGraph,
    buildActiveQueue
};
