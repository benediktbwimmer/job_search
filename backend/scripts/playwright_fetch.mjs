#!/usr/bin/env node

import process from "node:process";

function parseArgs(argv) {
  const out = {
    url: "",
    timeoutMs: 30000,
    waitUntil: "domcontentloaded",
    userAgent: "",
    headless: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "");
    if (token === "--url") out.url = String(argv[++i] || "");
    else if (token === "--timeout-ms") out.timeoutMs = Number(argv[++i] || "30000");
    else if (token === "--wait-until") out.waitUntil = String(argv[++i] || "domcontentloaded");
    else if (token === "--user-agent") out.userAgent = String(argv[++i] || "");
    else if (token === "--headless") out.headless = String(argv[++i] || "true") !== "false";
  }
  return out;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.url) {
    throw new Error("missing --url");
  }

  let playwright;
  try {
    playwright = await import("playwright");
  } catch (error) {
    throw new Error(
      "Playwright dependency missing. Run `cd backend && npm install` to enable playwright_cli backend."
    );
  }

  const { chromium } = playwright;
  let browser;
  try {
    try {
      browser = await chromium.launch({ headless: args.headless, channel: "chrome" });
    } catch {
      browser = await chromium.launch({ headless: args.headless });
    }

    const context = await browser.newContext(
      args.userAgent ? { userAgent: args.userAgent } : {}
    );
    const page = await context.newPage();
    const response = await page.goto(args.url, {
      waitUntil: args.waitUntil,
      timeout: Math.max(1000, Number(args.timeoutMs) || 30000),
    });
    await page.waitForTimeout(1200);
    const html = await page.content();
    const title = await page.title();
    const payload = {
      ok: true,
      url: page.url(),
      status: response ? response.status() : 200,
      title,
      html,
    };
    process.stdout.write(JSON.stringify(payload));
  } finally {
    if (browser) {
      await browser.close().catch(() => undefined);
    }
  }
}

run().catch((error) => {
  const payload = {
    ok: false,
    error: String(error && error.message ? error.message : error || "unknown error"),
  };
  process.stdout.write(JSON.stringify(payload));
  process.exit(1);
});

