// Subscription calls the local fixture proxy after the server has the user
// message. Gateway mode stops at the server. The desktop does not call the proxy.

const VENDOR_HOSTS = ["anthropic.com", "openai.com", "chatgpt.com", "x.ai", "grok.com"];

export const FIXTURE_KEY = "gol-desktop-fixture";

export function localImageCommand() {
  return "docker run --rm -d --name gol-agent-local -v gol-workspace:/workspace gol-agent:local";
}

export function productionImageCommand() {
  return "docker create --name gol-box-$RUN_ID -v gol-workspace:/workspace --entrypoint /bin/sh gol-agent:production -c true && docker start -a gol-box-$RUN_ID && docker rm -f gol-box-$RUN_ID";
}

export function assertFixtureProxy(url) {
  const lower = String(url).toLowerCase();
  for (const host of VENDOR_HOSTS) {
    if (lower.includes(host)) {
      throw new Error(`refusing to call ${host}; the fixture proxy is local`);
    }
  }
}

export async function sendTurn({
  serverUrl,
  proxyUrl,
  text,
  computer,
  credential,
  fetchImpl = fetch,
  agentId = crypto.randomUUID(),
}) {
  if (credential !== "subscription" && credential !== "gateway") {
    throw new Error(`unknown credential ${credential}`);
  }
  if (computer !== "local" && computer !== "box") {
    throw new Error(`unknown computer ${computer}`);
  }
  const message = String(text ?? "").trim();
  if (!message) {
    throw new Error("message is empty");
  }

  const placement = computer === "box" ? "Box" : "Local";
  const credentialBody =
    credential === "gateway"
      ? "PlatformGateway"
      : { BringYourOwn: { secret_ref: "desktop-subscription" } };

  const recorded = await postJson(fetchImpl, `${serverUrl}/v1/coworker/turns`, {
    agent_id: agentId,
    agent_version: "1",
    input: message,
    placement,
    work_model: {
      provider: "Anthropic",
      model_name: "claude-fixture",
      credential: credentialBody,
    },
    capabilities: ["model.call"],
    limits: { max_steps: 8, max_model_calls: 4 },
  });

  if (credential === "gateway") {
    const assistant = recorded.completion;
    if (typeof assistant !== "string" || assistant.length === 0) {
      throw new Error("server gateway did not return a completion");
    }
    return {
      runId: recorded.run_id,
      assistant,
      proxyCalledByDesktop: false,
      computer: recorded.computer,
    };
  }

  assertFixtureProxy(proxyUrl);
  const proxy = await postJson(
    fetchImpl,
    `${proxyUrl}/v1/messages?beta=true`,
    {
      model: "claude-fixture",
      max_tokens: 64,
      stream: false,
      messages: [{ role: "user", content: message }],
    },
    {
      "x-api-key": FIXTURE_KEY,
      "anthropic-version": "2023-06-01",
      "anthropic-beta": "claude-code-20250219,oauth-2025-04-20",
      "x-claude-code-session-id": "gol-desktop",
    },
  );
  const assistant = proxy?.content?.[0]?.text;
  if (typeof assistant !== "string" || assistant.length === 0) {
    throw new Error("proxy fixture missing assistant text");
  }
  const accepted = await postJson(
    fetchImpl,
    `${serverUrl}/v1/coworker/turns/${recorded.run_id}/completion`,
    { text: assistant },
  );
  return {
    runId: recorded.run_id,
    assistant: accepted.completion ?? assistant,
    proxyCalledByDesktop: true,
    computer: recorded.computer,
  };
}

async function postJson(fetchImpl, url, body, headers = {}) {
  const response = await fetchImpl(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`${response.status} ${url} ${detail}`);
  }
  return response.json();
}
