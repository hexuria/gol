import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import { sendTurn } from "./turn.mjs";

function listen(handler) {
  const server = http.createServer(handler);
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({ server, url: `http://127.0.0.1:${port}` });
    });
  });
}

function readJson(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      resolve(raw ? JSON.parse(raw) : {});
    });
    request.on("error", reject);
  });
}

test("subscription records on the server before it calls the proxy", async () => {
  const calls = [];
  const gol = await listen(async (request, response) => {
    calls.push(`server ${request.url}`);
    const body = await readJson(request);
    if (request.url === "/v1/coworker/turns") {
      assert.equal(body.input, "hello");
      assert.equal(body.placement, "Local");
      assert.equal(body.work_model.credential.BringYourOwn.secret_ref, "desktop-subscription");
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ run_id: "run-1", completion: null, computer: { started_by: "desktop" } }));
      return;
    }
    assert.equal(body.text, "fixture assistant text");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ run_id: "run-1", completion: body.text }));
  });
  const proxy = await listen(async (request, response) => {
    calls.push(`proxy ${request.url}`);
    assert.equal(request.headers["x-api-key"], "gol-desktop-fixture");
    assert.equal(request.headers["anthropic-version"], "2023-06-01");
    assert.equal(request.headers["x-claude-code-session-id"], "gol-desktop");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ content: [{ type: "text", text: "fixture assistant text" }] }));
  });

  const result = await sendTurn({
    serverUrl: gol.url,
    proxyUrl: proxy.url,
    text: "hello",
    computer: "local",
    credential: "subscription",
    agentId: "11111111-1111-4111-8111-111111111111",
  });
  assert.equal(result.assistant, "fixture assistant text");
  assert.equal(result.proxyCalledByDesktop, true);
  assert.deepEqual(calls, [
    "server /v1/coworker/turns",
    "proxy /v1/messages?beta=true",
    "server /v1/coworker/turns/run-1/completion",
  ]);
  gol.server.close();
  proxy.server.close();
});

test("gateway sends the user message to the server and does not call the proxy", async () => {
  const calls = [];
  const gol = await listen(async (request, response) => {
    calls.push(`server ${request.url}`);
    const body = await readJson(request);
    assert.equal(body.placement, "Box");
    assert.equal(body.work_model.credential, "PlatformGateway");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        run_id: "run-2",
        completion: "fixture gateway completion",
        computer: { started_by: "server", image: "gol-agent:production" },
      }),
    );
  });
  const proxy = await listen(async (request, response) => {
    calls.push(`proxy ${request.url}`);
    response.writeHead(500);
    response.end("desktop called the proxy");
  });

  const result = await sendTurn({
    serverUrl: gol.url,
    proxyUrl: proxy.url,
    text: "hello",
    computer: "box",
    credential: "gateway",
    agentId: "22222222-2222-4222-8222-222222222222",
  });
  assert.equal(result.assistant, "fixture gateway completion");
  assert.equal(result.proxyCalledByDesktop, false);
  assert.deepEqual(calls, ["server /v1/coworker/turns"]);
  gol.server.close();
  proxy.server.close();
});

test("the desktop refuses a vendor host for the subscription proxy", async () => {
  await assert.rejects(
    () =>
      sendTurn({
        serverUrl: "http://127.0.0.1:9",
        proxyUrl: "https://api.anthropic.com",
        text: "hello",
        computer: "local",
        credential: "subscription",
        fetchImpl: async (url) => {
          if (String(url).includes("coworker/turns") && !String(url).includes("completion")) {
            return {
              ok: true,
              async json() {
                return { run_id: "run-3", completion: null, computer: {} };
              },
              async text() {
                return "";
              },
            };
          }
          throw new Error(`unexpected fetch ${url}`);
        },
      }),
    /anthropic\.com/,
  );
});
