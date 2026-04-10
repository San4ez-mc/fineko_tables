const express = require("express");
const { handleTelegramUpdate } = require("./telegram/webhookHandler");
const { setWebhook } = require("./telegram/bot");

const app = express();
app.use(express.json({ limit: "1mb" }));

function verifyWebhookSecret(req) {
  const expectedSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
  if (!expectedSecret) {
    return true;
  }

  const providedSecret = req.get("x-telegram-bot-api-secret-token");
  return providedSecret === expectedSecret;
}

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true, service: "google-reports-telegram-webhook" });
});

app.post("/telegram/webhook", async (req, res) => {
  if (!verifyWebhookSecret(req)) {
    return res.status(401).json({ ok: false, error: "Invalid webhook secret" });
  }

  try {
    const result = await handleTelegramUpdate(req.body || {});
    return res.status(200).json({ ok: true, result });
  } catch (error) {
    console.error("Webhook handling failed", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

async function maybeSetWebhook() {
  if (process.env.TELEGRAM_AUTO_SET_WEBHOOK !== "true") {
    return;
  }

  const appBaseUrl = process.env.APP_BASE_URL;
  if (!appBaseUrl) {
    throw new Error("APP_BASE_URL is required when TELEGRAM_AUTO_SET_WEBHOOK=true");
  }

  const webhookUrl = `${appBaseUrl.replace(/\/$/, "")}/telegram/webhook`;
  await setWebhook(webhookUrl, process.env.TELEGRAM_WEBHOOK_SECRET || undefined);
  console.log(`Telegram webhook is set: ${webhookUrl}`);
}

async function startServer() {
  const port = Number(process.env.PORT || 3000);
  app.listen(port, async () => {
    console.log(`Server listening on port ${port}`);

    try {
      await maybeSetWebhook();
    } catch (error) {
      console.error("Failed to auto-set Telegram webhook", error);
    }
  });
}

startServer();
