const { buildReports } = require("../google/reportBuilder");
const { sendMessage, sendPhoto } = require("./bot");

function extractMessage(update) {
  if (update.message) {
    return update.message;
  }

  if (update.callback_query && update.callback_query.message) {
    return update.callback_query.message;
  }

  return null;
}

function extractCommand(text) {
  if (!text || !text.startsWith("/")) {
    return "";
  }

  return text.trim().split(/\s+/)[0].toLowerCase();
}

function buildDefaultPayload(message) {
  const telegramId = message.from?.id;
  const telegramUsername = message.from?.username || null;

  return {
    telegram_id: telegramId,
    telegram_username: telegramUsername,
    business_type: "unknown",
    process_model: {},
    financial_reports_model: {
      business_type: "unknown",
      cashflow_items: {
        income: [],
        cogs: [],
        team: [],
        operations: [],
        taxes: []
      },
      pl_structure: {
        revenue: [],
        cogs: [],
        gross_profit: "revenue - cogs",
        opex: [],
        operating_profit: "gross_profit - opex",
        owner_payout: [],
        pre_tax_profit: "operating_profit - owner_payout",
        taxes: [],
        net_profit: "pre_tax_profit - taxes"
      },
      items_count: 0,
      status: "complete"
    }
  };
}

async function fetchPayloadFromSource(telegramId) {
  const sourceUrl = process.env.REPORTS_SOURCE_API_URL;
  if (!sourceUrl) {
    return null;
  }

  const url = new URL(sourceUrl);
  url.searchParams.set("telegram_id", String(telegramId));

  const headers = {};
  if (process.env.REPORTS_SOURCE_API_KEY) {
    headers.Authorization = `Bearer ${process.env.REPORTS_SOURCE_API_KEY}`;
  }

  const response = await fetch(url, { headers });
  if (!response.ok) {
    throw new Error(`Failed to load user payload from source API: ${response.status}`);
  }

  return response.json();
}

function formatSuccessMessage(result) {
  return [
    "Готово! Таблиці створені.",
    `Папка: ${result.folder_url}`,
    `Cashflow: ${result.cashflow_url}`,
    `P&L: ${result.pl_url}`,
    `Validation: ${result.validation.valid ? "OK" : "FAILED"}`
  ].join("\n");
}

function formatErrorMessage(error) {
  return [
    "Не вдалося згенерувати таблиці.",
    "Спробуй ще раз через 1-2 хвилини.",
    `Технічна причина: ${error.message}`
  ].join("\n");
}

function buildWelcomeMessage() {
  return [
    "Привіт! Це бот Олександра Мацука для автоматичного створення фінансових таблиць.",
    "Бот допомагає згенерувати Cashflow і P&L у Google Sheets, створює папку на Google Drive, відкриває доступ і надсилає готові посилання.",
    "Надішли /build_reports, щоб запустити генерацію таблиць."
  ].join("\n\n");
}

function getBrandPhotoUrl() {
  const appBaseUrl = process.env.APP_BASE_URL;
  if (!appBaseUrl) {
    return "";
  }

  return `${appBaseUrl.replace(/\/$/, "")}/brand-photo`;
}

async function handleBuildReports(message) {
  const telegramId = message.from?.id;
  if (!telegramId) {
    throw new Error("Missing Telegram user id");
  }

  const payload =
    (await fetchPayloadFromSource(telegramId)) ||
    buildDefaultPayload(message);

  return buildReports(payload);
}

async function handleTelegramUpdate(update) {
  const message = extractMessage(update);
  if (!message || !message.chat || !message.text) {
    return { handled: false, reason: "No command message" };
  }

  const command = extractCommand(message.text);
  if (!["/start", "/build_reports", "/tables"].includes(command)) {
    return { handled: false, reason: "Unknown command" };
  }

  if (command === "/start") {
    const welcomeMessage = buildWelcomeMessage();
    const brandPhotoUrl = getBrandPhotoUrl();

    if (brandPhotoUrl) {
      await sendPhoto(message.chat.id, brandPhotoUrl, welcomeMessage);
    } else {
      await sendMessage(message.chat.id, welcomeMessage);
    }

    return { handled: true, command };
  }

  await sendMessage(message.chat.id, "Починаю побудову таблиць. Це може зайняти до хвилини...");

  try {
    const result = await handleBuildReports(message);
    await sendMessage(message.chat.id, formatSuccessMessage(result));
    return { handled: true, command, result };
  } catch (error) {
    await sendMessage(message.chat.id, formatErrorMessage(error));
    return { handled: true, command, error: error.message };
  }
}

module.exports = {
  handleTelegramUpdate
};
