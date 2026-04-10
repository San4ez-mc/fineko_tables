const { google } = require("googleapis");

function normalizePrivateKey(key) {
  if (!key) {
    return "";
  }

  // Common deployment pattern stores new lines as escaped \n.
  return key.replace(/\\n/g, "\n");
}

function getGoogleClients(options = {}) {
  const serviceAccountEmail =
    options.serviceAccountEmail || process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const serviceAccountPrivateKey = normalizePrivateKey(
    options.serviceAccountPrivateKey || process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY
  );

  if (!serviceAccountEmail || !serviceAccountPrivateKey) {
    throw new Error("Missing Google service account credentials in environment variables");
  }

  const auth = new google.auth.JWT({
    email: serviceAccountEmail,
    key: serviceAccountPrivateKey,
    scopes: [
      "https://www.googleapis.com/auth/drive",
      "https://www.googleapis.com/auth/spreadsheets"
    ]
  });

  return {
    auth,
    drive: google.drive({ version: "v3", auth }),
    sheets: google.sheets({ version: "v4", auth })
  };
}

module.exports = {
  getGoogleClients
};
