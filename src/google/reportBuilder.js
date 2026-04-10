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

async function buildReports(payload, options = {}) {
  const { drive, sheets } = getGoogleClients(options.credentials);

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
    await setSharingToAnyoneWithLink(drive, folder.id, options.shareRole || "writer");
    await setSharingToAnyoneWithLink(drive, cashflowSpreadsheet.spreadsheetId, options.shareRole || "writer");
    await setSharingToAnyoneWithLink(drive, plSpreadsheet.spreadsheetId, options.shareRole || "writer");
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
    last_build_status: validation.valid ? "success" : "failed",
    last_build_error: validation.errors.join("; "),
    last_validated_at: new Date().toISOString(),
    validation
  };

  if (typeof options.saveGoogleReports === "function") {
    await options.saveGoogleReports(payload.telegram_id, result);
  }

  return result;
}

module.exports = {
  buildReports,
  flattenCashflowItems,
  buildPLRows
};
