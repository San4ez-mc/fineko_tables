const { google } = require("googleapis");

const GOOGLE_API_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
];

function normalizePrivateKey(key) {
    if (!key) {
        return "";
    }

    // Common deployment pattern stores new lines as escaped \n.
    return key.replace(/\\n/g, "\n");
}

function hasOAuthCredentials(options = {}) {
    const clientId = options.oauthClientId || process.env.GOOGLE_OAUTH_CLIENT_ID;
    const clientSecret = options.oauthClientSecret || process.env.GOOGLE_OAUTH_CLIENT_SECRET;
    const refreshToken = options.oauthRefreshToken || process.env.GOOGLE_OAUTH_REFRESH_TOKEN;

    return Boolean(clientId && clientSecret && refreshToken);
}

function createOAuthAuth(options = {}) {
    const clientId = options.oauthClientId || process.env.GOOGLE_OAUTH_CLIENT_ID;
    const clientSecret = options.oauthClientSecret || process.env.GOOGLE_OAUTH_CLIENT_SECRET;
    const refreshToken = options.oauthRefreshToken || process.env.GOOGLE_OAUTH_REFRESH_TOKEN;

    const auth = new google.auth.OAuth2(clientId, clientSecret);

    auth.setCredentials({
        refresh_token: refreshToken
    });

    return auth;
}

function createServiceAccountAuth(options = {}) {
    const serviceAccountEmail =
        options.serviceAccountEmail || process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
    const serviceAccountPrivateKey = normalizePrivateKey(
        options.serviceAccountPrivateKey || process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY
    );

    if (!serviceAccountEmail || !serviceAccountPrivateKey) {
        throw new Error(
            "Missing Google credentials. Provide OAuth (GOOGLE_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN) or Service Account credentials."
        );
    }

    return new google.auth.JWT({
        email: serviceAccountEmail,
        key: serviceAccountPrivateKey,
        scopes: GOOGLE_API_SCOPES
    });
}

function getGoogleClients(options = {}) {
    const auth = hasOAuthCredentials(options)
        ? createOAuthAuth(options)
        : createServiceAccountAuth(options);

    return {
        auth,
        drive: google.drive({ version: "v3", auth }),
        sheets: google.sheets({ version: "v4", auth })
    };
}

module.exports = {
    getGoogleClients
};
