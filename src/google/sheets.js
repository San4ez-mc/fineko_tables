const SPREADSHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet";

function buildSpreadsheetUrl(spreadsheetId) {
    return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`;
}

async function createSpreadsheetInFolder(_sheets, drive, folderId, title) {
    const requestBody = {
        name: title,
        mimeType: SPREADSHEET_MIME_TYPE
    };

    if (folderId) {
        requestBody.parents = [folderId];
    }

    const created = await drive.files.create({
        requestBody,
        fields: "id,name,webViewLink",
        supportsAllDrives: true
    });

    return {
        spreadsheetId: created.data.id,
        spreadsheetUrl: created.data.webViewLink || buildSpreadsheetUrl(created.data.id)
    };
}

async function getSpreadsheet(sheets, spreadsheetId) {
    const response = await sheets.spreadsheets.get({
        spreadsheetId
    });

    return response.data;
}

function getSheetByTitle(spreadsheet, title) {
    return spreadsheet.sheets?.find((sheet) => sheet.properties?.title === title) || null;
}

async function addSheetIfMissing(sheets, spreadsheetId, title) {
    const spreadsheet = await getSpreadsheet(sheets, spreadsheetId);
    const existing = getSheetByTitle(spreadsheet, title);

    if (existing) {
        return existing.properties;
    }

    const response = await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
            requests: [
                {
                    addSheet: {
                        properties: { title }
                    }
                }
            ]
        }
    });

    return response.data.replies?.[0]?.addSheet?.properties;
}

async function writeValues(sheets, spreadsheetId, range, values) {
    await sheets.spreadsheets.values.update({
        spreadsheetId,
        range,
        valueInputOption: "USER_ENTERED",
        requestBody: { values }
    });
}

async function writeHeaders(sheets, spreadsheetId, sheetTitle, headers) {
    await writeValues(sheets, spreadsheetId, `${sheetTitle}!A1`, [headers]);
}

async function applySheetFormatting(sheets, spreadsheetId, requests) {
    if (!requests || requests.length === 0) {
        return;
    }

    await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
            requests
        }
    });
}

module.exports = {
    createSpreadsheetInFolder,
    addSheetIfMissing,
    writeValues,
    writeHeaders,
    applySheetFormatting,
    getSpreadsheet
};
