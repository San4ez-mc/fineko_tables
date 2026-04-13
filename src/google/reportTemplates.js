const CASHFLOW_SHEETS = ["Інструкція", "План", "Факт", "Звіт"];
const PL_SHEETS = ["Інструкція", "P&L", "Довідник статей"];

const CASHFLOW_PLAN_HEADERS = [
    "Період",
    "Стаття",
    "Група",
    "Тип руху",
    "Частота",
    "Регулярна",
    "План сума",
    "Коментар"
];

const CASHFLOW_GROUP_MAPPING = {
    income: "Доходи",
    cogs: "Прямі витрати",
    team: "Команда",
    operations: "Операційні витрати",
    taxes: "Податки"
};

const PL_SECTIONS = [
    "Revenue",
    "Cogs",
    "Gross Profit",
    "Opex",
    "Operating Profit",
    "Owner Payout",
    "Pre-Tax Profit",
    "Taxes",
    "Net Profit"
];

module.exports = {
    CASHFLOW_SHEETS,
    PL_SHEETS,
    CASHFLOW_PLAN_HEADERS,
    CASHFLOW_GROUP_MAPPING,
    PL_SECTIONS
};
