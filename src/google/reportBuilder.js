const { getGoogleClients } = require("./auth");
const {
    getOrCreateUserReportsFolder,
    getOrCreateFolderByName,
    findSpreadsheetByName,
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

const TEXT_TZ_ALIASES = [
    "tz_text",
    "report_tz_text",
    "technical_task_text",
    "prompt",
    "description",
    "instructions_text"
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
    return Boolean(firstDefined(payload, REPORT_SPEC_ALIASES) || firstDefined(payload, TEXT_TZ_ALIASES));
}

function hasTzCashflowSpec(payload = {}) {
    if (!payload || typeof payload !== "object") {
        return false;
    }

    if (payload.report_type && String(payload.report_type).toLowerCase() === "cashflow" && payload.tz_struct) {
        return true;
    }

    return Array.isArray(payload.tz_struct?.inflows) || Array.isArray(payload.tz_struct?.outflows);
}

function parseList(value) {
    return String(value || "")
        .split(/[,;|]/)
        .map((item) => item.trim())
        .filter(Boolean);
}

function ensureSpreadsheet(spec, fallbackTitle = "Report") {
    if (!spec.spreadsheets || spec.spreadsheets.length === 0) {
        spec.spreadsheets = [{ title: fallbackTitle, sheets: [] }];
    }

    return spec.spreadsheets[spec.spreadsheets.length - 1];
}

function ensureSheet(spreadsheet, fallbackTitle = "Sheet 1") {
    if (!spreadsheet.sheets || spreadsheet.sheets.length === 0) {
        spreadsheet.sheets = [{ title: fallbackTitle, headers: [], rows: [], formulas: [], requiredFields: [] }];
    }

    return spreadsheet.sheets[spreadsheet.sheets.length - 1];
}

function parseRowExpression(text) {
    const trimmed = String(text || "").trim();
    if (!trimmed) {
        return null;
    }

    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        try {
            return JSON.parse(trimmed);
        } catch {
            return null;
        }
    }

    const pairTokens = trimmed.split(/[;|]/).map((part) => part.trim()).filter(Boolean);
    const rowObject = {};

    for (const token of pairTokens) {
        const separatorIndex = token.indexOf("=") >= 0 ? token.indexOf("=") : token.indexOf(":");
        if (separatorIndex === -1) {
            continue;
        }

        const key = token.slice(0, separatorIndex).trim();
        const value = token.slice(separatorIndex + 1).trim();
        if (key) {
            rowObject[key] = value;
        }
    }

    return Object.keys(rowObject).length > 0 ? rowObject : null;
}

function parseTextTzToReportSpec(text, payload) {
    const normalized = {
        folderName: payload?.telegram_username ? `@${payload.telegram_username} - Financial Reports` : undefined,
        spreadsheets: []
    };

    const lines = String(text || "")
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

    if (lines.length === 0) {
        return normalized;
    }

    for (const line of lines) {
        let match = line.match(/^(folder|папка|назва\s*папки|название\s*папки)\s*[:\-]\s*(.+)$/i);
        if (match) {
            normalized.folderName = match[2].trim();
            continue;
        }

        match = line.match(/^(table|spreadsheet|таблиця|таблица|файл)\s*[:\-]\s*(.+)$/i);
        if (match) {
            normalized.spreadsheets.push({ title: match[2].trim(), sheets: [] });
            continue;
        }

        match = line.match(/^(sheet|tab|вкладка|лист|аркуш)\s*[:\-]\s*(.+)$/i);
        if (match) {
            const spreadsheet = ensureSpreadsheet(normalized);
            spreadsheet.sheets.push({ title: match[2].trim(), headers: [], rows: [], formulas: [], requiredFields: [] });
            continue;
        }

        match = line.match(/^(columns|headers|колонки|стовпці|поля)\s*[:\-]\s*(.+)$/i);
        if (match) {
            const spreadsheet = ensureSpreadsheet(normalized);
            const sheet = ensureSheet(spreadsheet);
            sheet.headers = parseList(match[2]);
            continue;
        }

        match = line.match(/^(required|required_fields|required_columns|обовязкові|обязательные)\s*[:\-]\s*(.+)$/i);
        if (match) {
            const spreadsheet = ensureSpreadsheet(normalized);
            const sheet = ensureSheet(spreadsheet);
            sheet.requiredFields = parseList(match[2]);
            continue;
        }

        match = line.match(/^(row|рядок|строка|data|дані|данные)\s*[:\-]\s*(.+)$/i);
        if (match) {
            const parsedRow = parseRowExpression(match[2]);
            if (parsedRow) {
                const spreadsheet = ensureSpreadsheet(normalized);
                const sheet = ensureSheet(spreadsheet);
                sheet.rows.push(parsedRow);
            }
            continue;
        }

        match = line.match(/^(formula|формула)\s*[:\-]\s*([A-Za-z]+\d+)\s*[=:]\s*(.+)$/i);
        if (match) {
            const spreadsheet = ensureSpreadsheet(normalized);
            const sheet = ensureSheet(spreadsheet);
            sheet.formulas.push({ range: match[2], formula: match[3] });
            continue;
        }
    }

    return normalized;
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
    const rawTextTz = firstDefined(payload, TEXT_TZ_ALIASES);
    const parsedTextSpec = rawTextTz ? parseTextTzToReportSpec(rawTextTz, payload) : null;
    const rawSpec = firstDefined(payload, REPORT_SPEC_ALIASES) || {};
    const folderName = firstDefined(rawSpec, FOLDER_NAME_ALIASES) || parsedTextSpec?.folderName;
    const rawSpreadsheets = firstDefined(rawSpec, SPREADSHEETS_ALIASES) || parsedTextSpec?.spreadsheets || [];

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

function sanitizeTitlePart(value, fallback = "Business") {
    return String(value || fallback)
        .trim()
        .replace(/[\\/:*?"<>|]/g, "_")
        .replace(/\s+/g, " ")
        .slice(0, 90);
}

function buildCashflowFileName(companyName, date = new Date()) {
    const year = date.getFullYear();
    return `Cashflow_${sanitizeTitlePart(companyName)}_${year}`;
}

function buildCashflowRowsFromTz(tz = {}) {
    const inflows = Array.isArray(tz.inflows) ? tz.inflows : [];
    const outflows = Array.isArray(tz.outflows) ? tz.outflows : [];
    const allArticles = [...inflows, ...outflows]
        .map((item) => item.article || item.name || "")
        .filter(Boolean)
        .filter((value, index, arr) => arr.indexOf(value) === index);

    return allArticles.map((article, index) => {
        const row = index + 2;
        return [
            article,
            "=IFERROR(SUMIF('⬇️ Надходження'!C:C,A" + row + ",'⬇️ Надходження'!D:D),0)",
            "=IFERROR(SUMIF('⬆️ Витрати'!C:C,A" + row + ",'⬆️ Витрати'!D:D),0)",
            "=B" + row + "-C" + row
        ];
    });
}

function buildDirectoryRows(tz = {}) {
    const inflows = Array.isArray(tz.inflows) ? tz.inflows : [];
    const outflows = Array.isArray(tz.outflows) ? tz.outflows : [];

    const rows = [["Тип", "Стаття", "Відповідальний", "Опер./міс", "Має доступ до Sheets"]];

    for (const item of inflows) {
        rows.push([
            "Надходження",
            item.article || "",
            item.responsible || "",
            item.ops_per_month || "",
            item.has_sheets_access === false ? "ні" : "так"
        ]);
    }

    for (const item of outflows) {
        rows.push([
            "Витрати",
            item.article || "",
            item.responsible || "",
            item.ops_per_month || "",
            item.has_sheets_access === false ? "ні" : "так"
        ]);
    }

    return rows;
}

function buildSourceRowsByType(items = []) {
    return items.map((item) => [
        "",
        "",
        item.article || "",
        "",
        item.comment || item.fixation_moment || ""
    ]);
}

async function createCashflowSpreadsheetInFolder({ drive, sheets, folderId, title, conflictStrategy }) {
    const existing = await findSpreadsheetByName(drive, title, folderId);
    if (existing && conflictStrategy === "overwrite") {
        return {
            spreadsheetId: existing.id,
            spreadsheetUrl: existing.webViewLink || buildSpreadsheetUrl(existing.id),
            reusedExisting: true
        };
    }

    const finalTitle = existing && conflictStrategy !== "overwrite"
        ? `${title}_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`
        : title;

    return createSpreadsheetInFolder(sheets, drive, folderId, finalTitle);
}

async function buildCashflowV23(payload, options, clients) {
    const { drive, sheets } = clients;
    const shareErrors = [];
    const answers = payload.answers || {};
    const tz = payload.tz_struct || {};
    const companyName = sanitizeTitlePart(payload.business_name || tz.business_name || "Business");
    const topFolderName = sanitizeTitlePart(`Фінансова система - ${companyName}`, "Фінансова система - Business");

    const systemFolder = await getOrCreateFolderByName(drive, topFolderName, {
        parentFolderId: options.parentFolderId
    });
    const cashflowFolder = await getOrCreateFolderByName(drive, "Cashflow", {
        parentFolderId: systemFolder.id
    });

    const subFolders = ["P&L", "Баланс", "Дашборд"];
    for (const subFolder of subFolders) {
        await getOrCreateFolderByName(drive, subFolder, {
            parentFolderId: systemFolder.id
        });
    }

    const cashflowTitle = buildCashflowFileName(companyName);
    const spreadsheet = await createCashflowSpreadsheetInFolder({
        drive,
        sheets,
        folderId: cashflowFolder.id,
        title: cashflowTitle,
        conflictStrategy: payload.file_conflict_strategy || "new_dated"
    });

    const incomingItems = Array.isArray(tz.inflows) ? tz.inflows : [];
    const outgoingItems = Array.isArray(tz.outflows) ? tz.outflows : [];
    const hasNoAccess = [...incomingItems, ...outgoingItems].some((item) => item.has_sheets_access === false);
    const includeLog = payload.architecture?.inflowsMode === "C" || payload.architecture?.outflowsMode === "C";

    const requiredSheets = ["📊 Cashflow", "📋 Довідники", "⚙️ Налаштування", "⬇️ Надходження"];
    if (payload.architecture?.outflowsMode === "B" || payload.architecture?.outflowsMode === "C") {
        requiredSheets.push("⬆️ Витрати");
    }
    if (payload.include_payment_calendar) {
        requiredSheets.push("📅 Платіжний календар");
    }
    if (includeLog) {
        requiredSheets.push("📝 Лог");
    }

    const noAccessOutflowPeople = outgoingItems
        .filter((item) => item.has_sheets_access === false)
        .map((item) => item.responsible || "Учасник")
        .filter((value, index, arr) => arr.indexOf(value) === index);

    for (const person of noAccessOutflowPeople) {
        requiredSheets.push(`⬆️ Витрати - ${person}`);
    }

    await ensureSheets(sheets, spreadsheet.spreadsheetId, requiredSheets);

    await writeHeaders(sheets, spreadsheet.spreadsheetId, "📊 Cashflow", ["Стаття", "Надходження", "Витрати", "Чистий рух"]);
    const cashflowRows = buildCashflowRowsFromTz(tz);
    if (cashflowRows.length > 0) {
        await writeValues(sheets, spreadsheet.spreadsheetId, "📊 Cashflow!A2", cashflowRows);
    }

    await writeValues(sheets, spreadsheet.spreadsheetId, "📋 Довідники!A1", buildDirectoryRows(tz));

    await writeValues(sheets, spreadsheet.spreadsheetId, "⚙️ Налаштування!A1", [
        ["Компанія", companyName],
        ["Мова", payload.language || answers.language || "українська"],
        ["Валюта", "UAH"],
        ["Поріг від'ємного залишку", 0]
    ]);

    await writeHeaders(sheets, spreadsheet.spreadsheetId, "⬇️ Надходження", ["Дата", "Контрагент", "Стаття", "Сума", "Коментар"]);
    const inflowRows = buildSourceRowsByType(incomingItems);
    if (inflowRows.length > 0) {
        await writeValues(sheets, spreadsheet.spreadsheetId, "⬇️ Надходження!A2", inflowRows);
    }

    if (requiredSheets.includes("⬆️ Витрати")) {
        await writeHeaders(sheets, spreadsheet.spreadsheetId, "⬆️ Витрати", ["Дата", "Контрагент", "Стаття", "Сума", "Коментар"]);
        const outflowRows = buildSourceRowsByType(outgoingItems);
        if (outflowRows.length > 0) {
            await writeValues(sheets, spreadsheet.spreadsheetId, "⬆️ Витрати!A2", outflowRows);
        }
    }

    for (const person of noAccessOutflowPeople) {
        const sheetTitle = `⬆️ Витрати - ${person}`;
        await writeHeaders(sheets, spreadsheet.spreadsheetId, sheetTitle, ["Дата", "Стаття", "Сума", "Коментар"]);
    }

    if (payload.include_payment_calendar) {
        await writeHeaders(sheets, spreadsheet.spreadsheetId, "📅 Платіжний календар", ["Дата", "Початковий залишок", "Надходження", "Витрати", "Кінцевий залишок"]);
    }

    if ((options.shareMode || process.env.GOOGLE_REPORTS_SHARE_MODE) === "anyone_with_link") {
        await applySharingSafely(drive, systemFolder.id, options.shareRole || "writer", shareErrors, "system folder sharing");
        await applySharingSafely(drive, cashflowFolder.id, options.shareRole || "writer", shareErrors, "cashflow folder sharing");
        await applySharingSafely(drive, spreadsheet.spreadsheetId, options.shareRole || "writer", shareErrors, "cashflow file sharing");
    }

    const validation = await validateReports({
        drive,
        sheets,
        folderId: systemFolder.id,
        cashflowId: spreadsheet.spreadsheetId,
        tzPayload: tz,
        mode: "cashflow_v23"
    });

    return {
        mode: "cashflow_v23",
        folder_id: systemFolder.id,
        folder_url: buildFolderUrl(systemFolder.id),
        system_folder_name: topFolderName,
        generated_files: [
            {
                type: "cashflow",
                title: cashflowTitle,
                spreadsheet_id: spreadsheet.spreadsheetId,
                spreadsheet_url: spreadsheet.spreadsheetUrl || buildSpreadsheetUrl(spreadsheet.spreadsheetId)
            }
        ],
        built_summary: [
            "Аркуш «Cashflow» - зведений звіт з формулами",
            `Аркуш «Надходження» - ${incomingItems.length} статей`,
            requiredSheets.includes("⬆️ Витрати") ? "Аркуш «Витрати» - підготовлений для внесення" : "Витрати враховуються через окремі джерела",
            noAccessOutflowPeople.length > 0 ? `Окремі аркуші без доступу: ${noAccessOutflowPeople.join(", ")}` : "Окремі аркуші без доступу не потрібні",
            hasNoAccess ? "Google Form не створювалась автоматично, додай вручну за потреби" : "Google Form не потрібна"
        ],
        last_build_status: validation.valid ? (shareErrors.length > 0 ? "partial_success" : "success") : "failed",
        last_build_error: [...validation.errors, ...shareErrors].join("; "),
        last_validated_at: new Date().toISOString(),
        validation,
        share_warnings: shareErrors
    };
}

async function buildFromUniversalSpec(payload, options, clients) {
    const { drive, sheets } = clients;
    const shareErrors = [];
    const spec = normalizeReportSpec(payload);

    if (!spec.spreadsheets || spec.spreadsheets.length === 0) {
        throw new Error("Universal report spec is empty. Add report_spec.spreadsheets or provide tz_text with table/sheet/columns/rows.");
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

    if (hasTzCashflowSpec(payload)) {
        return buildCashflowV23(payload, options, clients);
    }

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
