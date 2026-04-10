const { CASHFLOW_SHEETS, PL_SHEETS, PL_SECTIONS } = require("./reportTemplates");

function toTitleSet(spreadsheet) {
  return new Set(
    (spreadsheet.sheets || []).map((sheet) => sheet.properties?.title).filter(Boolean)
  );
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

async function validateReports({ drive, sheets, folderId, cashflowId, plId }) {
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
  validateReports
};
