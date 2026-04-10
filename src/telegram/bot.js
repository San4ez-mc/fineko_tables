const TELEGRAM_API_BASE = "https://api.telegram.org";

function getBotToken() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) {
    throw new Error("Missing TELEGRAM_BOT_TOKEN");
  }

  return token;
}

async function telegramRequest(method, body) {
  const token = getBotToken();
  const response = await fetch(`${TELEGRAM_API_BASE}/bot${token}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await response.json();
  if (!response.ok || !data.ok) {
    throw new Error(`Telegram API error: ${response.status} ${JSON.stringify(data)}`);
  }

  return data.result;
}

async function sendMessage(chatId, text) {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text,
    disable_web_page_preview: true
  });
}

async function setWebhook(webhookUrl, secretToken) {
  const body = {
    url: webhookUrl,
    allowed_updates: ["message", "callback_query"]
  };

  if (secretToken) {
    body.secret_token = secretToken;
  }

  return telegramRequest("setWebhook", body);
}

module.exports = {
  sendMessage,
  setWebhook
};
