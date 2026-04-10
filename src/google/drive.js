const FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";

function sanitizeFolderName(value) {
  if (!value) {
    return "financial-reports";
  }

  return String(value)
    .trim()
    .replace(/[\\/:*?"<>|]/g, "_")
    .replace(/\s+/g, " ");
}

function buildUserFolderName({ telegram_id, telegram_username }) {
  const base = telegram_username
    ? `@${telegram_username} - Financial Reports`
    : `tg_${telegram_id} - Financial Reports`;

  return sanitizeFolderName(base);
}

async function findFolderByName(drive, folderName, parentFolderId) {
  const parentFilter = parentFolderId ? ` and '${parentFolderId}' in parents` : "";
  const query = [
    `mimeType='${FOLDER_MIME_TYPE}'`,
    "trashed=false",
    `name='${folderName.replace(/'/g, "\\'")}'`
  ].join(" and ") + parentFilter;

  const response = await drive.files.list({
    q: query,
    fields: "files(id,name,webViewLink)",
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  });

  return response.data.files?.[0] || null;
}

async function createFolder(drive, folderName, parentFolderId) {
  const fileMetadata = {
    name: folderName,
    mimeType: FOLDER_MIME_TYPE
  };

  if (parentFolderId) {
    fileMetadata.parents = [parentFolderId];
  }

  const response = await drive.files.create({
    requestBody: fileMetadata,
    fields: "id,name,webViewLink",
    supportsAllDrives: true
  });

  return response.data;
}

async function getOrCreateUserReportsFolder(drive, userData, options = {}) {
  const folderName = buildUserFolderName(userData);
  const parentFolderId = options.parentFolderId || process.env.GOOGLE_DRIVE_PARENT_FOLDER_ID;

  const existing = await findFolderByName(drive, folderName, parentFolderId);
  if (existing) {
    return { ...existing, created: false };
  }

  const created = await createFolder(drive, folderName, parentFolderId);
  return { ...created, created: true };
}

async function setSharingToAnyoneWithLink(drive, fileId, role = "writer") {
  await drive.permissions.create({
    fileId,
    requestBody: {
      type: "anyone",
      role
    },
    supportsAllDrives: true
  });
}

function buildFolderUrl(folderId) {
  return `https://drive.google.com/drive/folders/${folderId}`;
}

function buildSpreadsheetUrl(spreadsheetId) {
  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`;
}

module.exports = {
  buildUserFolderName,
  getOrCreateUserReportsFolder,
  setSharingToAnyoneWithLink,
  buildFolderUrl,
  buildSpreadsheetUrl
};
