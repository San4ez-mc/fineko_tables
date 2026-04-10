const { getGoogleClients } = require("./auth");
const {
    getOrCreateUserReportsFolder,
    setSharingToAnyoneWithLink,
    buildFolderUrl,
    buildSpreadsheetUrl
} = require("./drive");
const {
    createSpreadsheetInFolder,
    addSheetIfMissing,
    writeHeaders,
    writeValues
} = require("./sheets");
const {
    CASHFLOW_SHEETS,
    PL_SHEETS,
    CASHFLOW_PLAN_HEADERS,
    CASHFLOW_GROUP_MAPPING,
    PL_SECTIONS
} = require("./reportTemplates");
const { validateReports } = require("./reportValidator");

const REPORT_SPEC_ALIASES = [
    "report_spec",
    "report_tz",
    "tz",
    "spec",
    "table_spec",
    "tables_spec"
];

const SPREADSHEETS_ALIASES = [
    "spreadsheets",
    "tables",
    "files",
    "reports"
];

const SHEETS_ALIASES = [
    "sheets",
    "tabs",
    "worksheets",
    "аркуші",
    "вкладки",
    "листы"
];

const TITLE_ALIASES = [
    "title",
    "name",
    "table_name",
    "sheet_name",
    "назва",
    "название"
];

const HEADERS_ALIASES = [
    "headers",
    "columns",
    "fields",
    "колонки",
    "стовпці"
];

const ROWS_ALIASES = [
    "rows",
    "data",
    "items",
    "records",
    "entries",
    "рядки",
    "дані",
    "данные"
];

const FORMULAS_ALIASES = [
    "formulas",
    "formula",
    "формули",
    "формулы"
];

const REQUIRED_FIELDS_ALIASES = [
    "required_fields",
    "required_columns",
    "required",
    "обовязкові_поля",
    "обязательные_поля"
];

const FOLDER_NAME_ALIASES = [
    "folder_name",
    "folder",
    "reports_folder_name",
    "назва_папки",
    "название_папки"
];

function firstDefined(source, aliases) {
    for (const key of aliases) {
        if (source && source[key] !== undefined && source[key] !== null) {
            return source[key];
        }
    }

    return undefined;
}

function slugify(value) {
    return String(value || "")
        .toLowerCase()
        .replace(/[^a-z0-9а-яіїєґ]+/gi, "_")
        .replace(/^_+|_+$/g, "");
}

function hasUniversalSpec(payload = {}) {
    return Boolean(firstDefined(payload, REPORT_SPEC_ALIASES));
}

function normalizeFormulas(raw) {
    if (!raw) {
        return [];
    }

    if (Array.isArray(raw)) {
        return raw
            .map((item) => ({
                range: item.range || item.cell,
                formula: item.formula || item.value
            }))
            .filter((item) => item.range && item.formula);
    }

    if (typeof raw === "object") {
        return Object.entries(raw)
            .map(([range, formula]) => ({ range, formula }))
            .filter((item) => item.range && item.formula);
    }

    return [];
}

function pickValueByHeader(rowObject, header) {
    if (!rowObject || typeof rowObject !== "object") {
        return "";
    }

    if (rowObject[header] !== undefined) {
        return rowObject[header];
    }

    const headerSlug = slugify(header);
    const matchKey = Object.keys(rowObject).find((key) => slugify(key) === headerSlug);
    return matchKey ? rowObject[matchKey] : "";
}

function normalizeRows(rawRows, headers) {
    if (!Array.isArray(rawRows) || rawRows.length === 0) {
        return [];
    }

    if (Array.isArray(rawRows[0])) {
        return rawRows;
    }

    if (!headers || headers.length === 0) {
        return rawRows.map((row) => Object.values(row || {}));
    }

    return rawRows.map((row) => headers.map((header) => pickValueByHeader(row, header)));
}

function validateRequiredFields(rawRows, requiredFields) {
    if (!Array.isArray(rawRows) || rawRows.length === 0 || !Array.isArray(requiredFields) || requiredFields.length === 0) {
        return;
    }

    const missing = [];

    rawRows.forEach((row, index) => {
        if (!row || typeof row !== "object" || Array.isArray(row)) {
            return;
        }

        requiredFields.forEach((field) => {
            const value = pickValueByHeader(row, field);
            if (value === "" || value === null || value === undefined) {
                missing.push(`row ${index + 1}: ${field}`);
            }
        });
    });

    if (missing.length > 0) {
        throw new Error(`Required fields are empty (${missing.slice(0, 12).join(", ")})`);
    }
}

function normalizeSheetSpec(rawSheet = {}, index = 0) {
    const title = firstDefined(rawSheet, TITLE_ALIASES) || `Sheet ${index + 1}`;
    const headers = firstDefined(rawSheet, HEADERS_ALIASES) || [];
    const rawRows = firstDefined(rawSheet, ROWS_ALIASES) || [];
    const requiredFields = firstDefined(rawSheet, REQUIRED_FIELDS_ALIASES) || [];
    const formulas = normalizeFormulas(firstDefined(rawSheet, FORMULAS_ALIASES));

    validateRequiredFields(rawRows, requiredFields);

    return {
        title,
        headers: Array.isArray(headers) ? headers : [],
        rows: normalizeRows(rawRows, Array.isArray(headers) ? headers : []),
        formulas,
        requiredFields: Array.isArray(requiredFields) ? requiredFields : []
    };
}

function normalizeSpreadsheetSpec(rawSpreadsheet = {}, index = 0) {
    const title = firstDefined(rawSpreadsheet, TITLE_ALIASES) || `Report ${index + 1}`;
    const rawSheets = firstDefined(rawSpreadsheet, SHEETS_ALIASES) || [];

    return {
        title,
        sheets: (Array.isArray(rawSheets) ? rawSheets : []).map((sheet, i) => normalizeSheetSpec(sheet, i))
    };
}

function normalizeReportSpec(payload) {
    const rawSpec = firstDefined(payload, REPORT_SPEC_ALIASES) || {};
    const folderName = firstDefined(rawSpec, FOLDER_NAME_ALIASES);
    const rawSpreadsheets = firstDefined(rawSpec, SPREADSHEETS_ALIASES) || [];

    return {
        folderName,
        spreadsheets: (Array.isArray(rawSpreadsheets) ? rawSpreadsheets : []).map((spreadsheet, index) =>
            normalizeSpreadsheetSpec(spreadsheet, index)
        )
    };
}

function flattenCashflowItems(cashflowItems = {}) {
    const rows = [];

    for (const [groupKey, items] of Object.entries(cashflowItems)) {
        const groupName = CASHFLOW_GROUP_MAPPING[groupKey] || groupKey;
        for (const item of items || []) {
            rows.push([
                "",
                item.name || item.title || "",
                groupName,
                item.flow_type || "",
                item.frequency || "",
                item.is_regular ? "так" : "ні",
                item.amount || "",
                item.comment || ""
            ]);
        }
    }

    return rows;
}

function buildPLRows(plStructure = {}) {
    const sectionMap = {
        Revenue: plStructure.revenue || [],
        Cogs: plStructure.cogs || [],
        Opex: plStructure.opex || [],
        "Owner Payout": plStructure.owner_payout || [],
        Taxes: plStructure.taxes || []
    };

    const rows = [];

    for (const section of PL_SECTIONS) {
        rows.push([section]);

        const items = sectionMap[section] || [];
        for (const item of items) {
            rows.push([`  - ${item.name || item.title || ""}`]);
        }
    }

    return rows;
}

async function ensureSheets(sheets, spreadsheetId, sheetTitles) {
    for (const title of sheetTitles) {
        await addSheetIfMissing(sheets, spreadsheetId, title);
    }
}

async function applySharingSafely(drive, fileId, role, shareErrors, label) {
    try {
        await setSharingToAnyoneWithLink(drive, fileId, role);
    } catch (error) {
        shareErrors.push(`${label}: ${error.message}`);
    }
}

async function writeSheetBySpec(sheets, spreadsheetId, sheetSpec) {
    await addSheetIfMissing(sheets, spreadsheetId, sheetSpec.title);

    if (sheetSpec.headers.length > 0) {
        await writeHeaders(sheets, spreadsheetId, sheetSpec.title, sheetSpec.headers);
    }

    if (sheetSpec.rows.length > 0) {
        await writeValues(sheets, spreadsheetId, `${sheetSpec.title}!A2`, sheetSpec.rows);
    }

    for (const formula of sheetSpec.formulas) {
        await writeValues(sheets, spreadsheetId, `${sheetSpec.title}!${formula.range}`, [[formula.formula]]);
    }
}

async function buildFromUniversalSpec(payload, options, clients) {
    const { drive, sheets } = clients;
    const shareErrors = [];
    const spec = normalizeReportSpec(payload);

    if (!spec.spreadsheets || spec.spreadsheets.length === 0) {
        throw new Error("Universal report spec is empty. Add report_spec.spreadsheets with sheets, columns, and rows.");
    }

    const folder = await getOrCreateUserReportsFolder(drive, payload, {
        parentFolderId: options.parentFolderId,
        folderNameOverride: spec.folderName
    });

    const generatedFiles = [];

    for (const spreadsheetSpec of spec.spreadsheets) {
        const spreadsheet = await createSpreadsheetInFolder(
            sheets,
            drive,
            folder.id,
            spreadsheetSpec.title
        );

        for (const sheetSpec of spreadsheetSpec.sheets) {
            await writeSheetBySpec(sheets, spreadsheet.spreadsheetId, sheetSpec);
        }

        if ((options.shareMode || process.env.GOOGLE_REPORTS_SHARE_MODE) === "anyone_with_link") {
            await applySharingSafely(
                drive,
                spreadsheet.spreadsheetId,
                options.shareRole || "writer",
                shareErrors,
                `${spreadsheetSpec.title} sharing`
            );
        }

        generatedFiles.push({
            title: spreadsheetSpec.title,
            spreadsheet_id: spreadsheet.spreadsheetId,
            spreadsheet_url: spreadsheet.spreadsheetUrl || buildSpreadsheetUrl(spreadsheet.spreadsheetId)
        });
    }

    if ((options.shareMode || process.env.GOOGLE_REPORTS_SHARE_MODE) === "anyone_with_link") {
        await applySharingSafely(drive, folder.id, options.shareRole || "writer", shareErrors, "folder sharing");
    }

    const result = {
        folder_id: folder.id,
        folder_url: buildFolderUrl(folder.id),
        generated_files: generatedFiles,
        last_build_status: shareErrors.length > 0 ? "partial_success" : "success",
        last_build_error: shareErrors.join("; "),
        last_validated_at: new Date().toISOString(),
        validation: {
            valid: true,
            checks: ["universal_spec_applied"],
            errors: []
        },
        share_warnings: shareErrors
    };

    if (typeof options.saveGoogleReports === "function") {
        await options.saveGoogleReports(payload.telegram_id, result);
    }

    return result;
}

async function buildLegacyFinancialReports(payload, options, clients) {
    const { drive, sheets } = clients;
    const shareErrors = [];

    const folder = await getOrCreateUserReportsFolder(drive, payload, {
        parentFolderId: options.parentFolderId
    });

    const cashflowSpreadsheet = await createSpreadsheetInFolder(
        sheets,
        drive,
        folder.id,
        "Cashflow"
    );
    await ensureSheets(sheets, cashflowSpreadsheet.spreadsheetId, CASHFLOW_SHEETS);

    await writeHeaders(sheets, cashflowSpreadsheet.spreadsheetId, "План", CASHFLOW_PLAN_HEADERS);
    const cashflowRows = flattenCashflowItems(payload.financial_reports_model?.cashflow_items || {});
    if (cashflowRows.length > 0) {
        await writeValues(sheets, cashflowSpreadsheet.spreadsheetId, "План!A2", cashflowRows);
    }

    const plSpreadsheet = await createSpreadsheetInFolder(sheets, drive, folder.id, "P&L");
    await ensureSheets(sheets, plSpreadsheet.spreadsheetId, PL_SHEETS);

    const plRows = buildPLRows(payload.financial_reports_model?.pl_structure || {});
    if (plRows.length > 0) {
        await writeValues(sheets, plSpreadsheet.spreadsheetId, "P&L!A1", plRows);
    }

    if ((options.shareMode || process.env.GOOGLE_REPORTS_SHARE_MODE) === "anyone_with_link") {
        await applySharingSafely(drive, folder.id, options.shareRole || "writer", shareErrors, "folder sharing");
        await applySharingSafely(drive, cashflowSpreadsheet.spreadsheetId, options.shareRole || "writer", shareErrors, "cashflow sharing");
        await applySharingSafely(drive, plSpreadsheet.spreadsheetId, options.shareRole || "writer", shareErrors, "p&l sharing");
    }

    const validation = await validateReports({
        drive,
        sheets,
        folderId: folder.id,
        cashflowId: cashflowSpreadsheet.spreadsheetId,
        plId: plSpreadsheet.spreadsheetId
    });

    const result = {
        folder_id: folder.id,
        folder_url: buildFolderUrl(folder.id),
        cashflow_sheet_id: cashflowSpreadsheet.spreadsheetId,
        cashflow_url: cashflowSpreadsheet.spreadsheetUrl || buildSpreadsheetUrl(cashflowSpreadsheet.spreadsheetId),
        pl_sheet_id: plSpreadsheet.spreadsheetId,
        pl_url: plSpreadsheet.spreadsheetUrl || buildSpreadsheetUrl(plSpreadsheet.spreadsheetId),
        generated_files: [
            {
                title: "Cashflow",
                spreadsheet_id: cashflowSpreadsheet.spreadsheetId,
                spreadsheet_url: cashflowSpreadsheet.spreadsheetUrl || buildSpreadsheetUrl(cashflowSpreadsheet.spreadsheetId)
            },
            {
                title: "P&L",
                spreadsheet_id: plSpreadsheet.spreadsheetId,
                spreadsheet_url: plSpreadsheet.spreadsheetUrl || buildSpreadsheetUrl(plSpreadsheet.spreadsheetId)
            }
        ],
        last_build_status: validation.valid ? (shareErrors.length > 0 ? "partial_success" : "success") : "failed",
        last_build_error: [...validation.errors, ...shareErrors].join("; "),
        last_validated_at: new Date().toISOString(),
        validation,
        share_warnings: shareErrors
    };

    if (typeof options.saveGoogleReports === "function") {
        await options.saveGoogleReports(payload.telegram_id, result);
    }

    return result;
}

async function buildReports(payload, options = {}) {
    const clients = getGoogleClients(options.credentials);

    if (hasUniversalSpec(payload)) {
        return buildFromUniversalSpec(payload, options, clients);
    }

    return buildLegacyFinancialReports(payload, options, clients);
}

module.exports = {
    buildReports,
    flattenCashflowItems,
    buildPLRows,
    normalizeReportSpec
};
