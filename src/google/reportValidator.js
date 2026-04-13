const { CASHFLOW_SHEETS, PL_SHEETS, PL_SECTIONS } = require("./reportTemplates");

function toTitleSet(spreadsheet) {
    return new Set(
        (spreadsheet.sheets || []).map((sheet) => sheet.properties?.title).filter(Boolean)
    );
}

function normalizeFirstRow(values = []) {
    return (values[0] || []).map((value) => String(value || "").trim());
}

async function validateCashflowV23Spreadsheet(sheets, spreadsheetId, tzPayload = {}) {
    const errors = [];
    const checks = ["cashflow_v23_sheets_exist", "cashflow_v23_headers_valid", "cashflow_v23_articles_present"];
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
    const titles = toTitleSet(spreadsheet.data);

    ["📊 Cashflow", "📋 Довідники", "⚙️ Налаштування"].forEach((sheetTitle) => {
        if (!titles.has(sheetTitle)) {
            errors.push(`Missing Cashflow v2.3 sheet: ${sheetTitle}`);
        }
    });

    const headerResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: "📊 Cashflow!A1:D1"
    });

    const headerRow = normalizeFirstRow(headerResponse.data.values || []);
    const expectedHeaders = ["Стаття", "Надходження", "Витрати", "Чистий рух"];
    const hasHeaders = expectedHeaders.every((header, index) => headerRow[index] === header);
    if (!hasHeaders) {
        errors.push("Cashflow header row does not match expected structure");
    }

    const formulasResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: "📊 Cashflow!B2:C200"
    });
    const formulasValues = formulasResponse.data.values || [];
    if (formulasValues.length > 0) {
        const hasSumif = formulasValues.some((row) => row.some((cell) => String(cell || "").toUpperCase().includes("SUMIF")));
        if (!hasSumif) {
            errors.push("Cashflow formulas do not reference SUMIF");
        }
    }

    const expectedArticles = [...(tzPayload.inflows || []), ...(tzPayload.outflows || [])]
        .map((item) => String(item.article || "").trim())
        .filter(Boolean);

    if (expectedArticles.length > 0) {
        const directoryResponse = await sheets.spreadsheets.values.get({
            spreadsheetId,
            range: "📋 Довідники!B2:B500"
        });
        const directoryArticles = new Set((directoryResponse.data.values || []).map((row) => String(row[0] || "").trim()).filter(Boolean));

        expectedArticles.forEach((article) => {
            if (!directoryArticles.has(article)) {
                errors.push(`Missing article in Довідники: ${article}`);
            }
        });
    }

    return {
        valid: errors.length === 0,
        checks,
        errors
    };
}

async function validateCashflowSpreadsheet(sheets, spreadsheetId) {
    const errors = [];
    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
    const titles = toTitleSet(spreadsheet.data);

    for (const title of CASHFLOW_SHEETS) {
        if (!titles.has(title)) {
            errors.push(`Missing Cashflow sheet: ${title}`);
        }
    }

    return {
        valid: errors.length === 0,
        checks: ["cashflow_sheets_exist"],
        errors
    };
}

async function validatePLSpreadsheet(sheets, spreadsheetId) {
    const errors = [];
    const checks = ["pl_sections_exist"];

    const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId });
    const titles = toTitleSet(spreadsheet.data);

    for (const title of PL_SHEETS) {
        if (!titles.has(title)) {
            errors.push(`Missing P&L sheet: ${title}`);
        }
    }

    const valuesResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: "P&L!A:A"
    });

    const firstColumn = (valuesResponse.data.values || []).map((row) => row[0]).filter(Boolean);

    for (const section of PL_SECTIONS) {
        if (!firstColumn.includes(section)) {
            errors.push(`Missing P&L section row: ${section}`);
        }
    }

    return {
        valid: errors.length === 0,
        checks,
        errors
    };
}

async function validateReports({ drive, sheets, folderId, cashflowId, plId, mode, tzPayload }) {
    const errors = [];
    const checks = [];

    if (folderId) {
        await drive.files.get({
            fileId: folderId,
            fields: "id",
            supportsAllDrives: true
        });
        checks.push("folder_exists");
    }

    if (mode === "cashflow_v23") {
        const cashflowValidation = await validateCashflowV23Spreadsheet(sheets, cashflowId, tzPayload);
        checks.push(...cashflowValidation.checks);
        errors.push(...cashflowValidation.errors);

        return {
            valid: errors.length === 0,
            folder_created: Boolean(folderId),
            cashflow_created: Boolean(cashflowId),
            pl_created: false,
            checks,
            errors
        };
    }

    const cashflowValidation = await validateCashflowSpreadsheet(sheets, cashflowId);
    const plValidation = await validatePLSpreadsheet(sheets, plId);

    checks.push(...cashflowValidation.checks, ...plValidation.checks);
    errors.push(...cashflowValidation.errors, ...plValidation.errors);

    return {
        valid: errors.length === 0,
        folder_created: Boolean(folderId),
        cashflow_created: Boolean(cashflowId),
        pl_created: Boolean(plId),
        checks,
        errors
    };
}

module.exports = {
    validateCashflowSpreadsheet,
    validatePLSpreadsheet,
    validateCashflowV23Spreadsheet,
    validateReports
};
