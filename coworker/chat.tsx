/**
 * gol coworker.
 *
 * Started from gpuix examples/chat.tsx at 9fcd628863e354e9c58019fc3bf38981a1e64158.
 * That demo's Build / Plan picker slot is now two pickers: Local or Box, and
 * subscription or gateway. Replies are not fixtures inside this window.
 * The desktop posts the user message to the gol server before any model call.
 * Subscription then calls the local fixture proxy. Gateway does not.
 *
 * Run: bun chat.tsx
 * GOL_SERVER_URL defaults to http://127.0.0.1:43123
 * GOL_PROXY_URL defaults to http://127.0.0.1:43124
 */

import React, { useState } from "react";
import { render } from "@gpuix/react";

import { localImageCommand, productionImageCommand, sendTurn } from "./turn.mjs";

const C = {
  canvas: "#1A1A1A",
  sidebar: "#181818",
  composer: "#212121",
  overlayStrong: "#E6EAF217",
  border: "#E6EAF212",
  sidebarBorder: "#292929",
  text: "#E2E2E2",
  secondary: "#A3A3A3",
  tertiary: "#7D7D7D",
  ghost: "#575757",
  accent: "#E2795B",
  inverse: "#E7E9EC",
  onInverse: "#17181C",
};

const FONT_SANS = typeof window === "undefined" ? "Helvetica" : "IBM Plex Sans";

type Computer = "local" | "box";
type Credential = "subscription" | "gateway";
type Turn =
  | { kind: "user"; text: string }
  | { kind: "assistant"; text: string }
  | { kind: "status"; text: string };

const SERVER_URL = process.env.GOL_SERVER_URL ?? "http://127.0.0.1:43123";
const PROXY_URL = process.env.GOL_PROXY_URL ?? "http://127.0.0.1:43124";

function modeCopy(computer: Computer, credential: Credential) {
  const caller =
    credential === "subscription"
      ? "Model call: this window, after the server records the message."
      : "Model call: the gol server. This window does not call the proxy.";
  const box =
    computer === "local"
      ? `Computer: Local. Start the local image with Docker.\n${localImageCommand()}`
      : `Computer: Box. The server starts the production image.\n${productionImageCommand()}`;
  return `${caller}\n${box}`;
}

function Segmented({
  testId,
  value,
  options,
  onChange,
}: {
  testId: string;
  value: string;
  options: { id: string; label: string }[];
  onChange: (id: string) => void;
}) {
  return (
    <div testId={testId} style={{ display: "flex", flexDirection: "row", gap: 2 }}>
      {options.map((option) => {
        const selected = option.id === value;
        return (
          <div
            key={option.id}
            testId={`${testId}-${option.id}`}
            onClick={() => onChange(option.id)}
            style={{
              height: 26,
              paddingLeft: 8,
              paddingRight: 8,
              borderRadius: 6,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              backgroundColor: selected ? C.overlayStrong : "#00000000",
            }}
          >
            <text style={{ fontSize: 13, color: selected ? C.accent : C.secondary }}>{option.label}</text>
          </div>
        );
      })}
    </div>
  );
}

export function ChatApp() {
  const [computer, setComputer] = useState<Computer>("box");
  const [credential, setCredential] = useState<Credential>("subscription");
  const [draft, setDraft] = useState("");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [busy, setBusy] = useState(false);

  const send = (raw: string) => {
    const text = raw.trim();
    if (!text || busy) return;
    setDraft("");
    setBusy(true);
    setTurns((current) => [
      ...current,
      { kind: "user", text },
      {
        kind: "status",
        text:
          credential === "gateway"
            ? "Recording the message. The server will call the gateway."
            : "Recording the message, then calling the local proxy.",
      },
    ]);
    void sendTurn({
      serverUrl: SERVER_URL,
      proxyUrl: PROXY_URL,
      text,
      computer,
      credential,
    })
      .then((result) => {
        setTurns((current) => [...current, { kind: "assistant", text: result.assistant }]);
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : "send failed";
        setTurns((current) => [...current, { kind: "status", text: message }]);
      })
      .finally(() => setBusy(false));
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "row",
        width: "100%",
        height: "100%",
        backgroundColor: C.canvas,
        fontFamily: FONT_SANS,
        color: C.text,
      }}
    >
      <div
        style={{
          width: 280,
          height: "100%",
          flexShrink: 0,
          backgroundColor: C.sidebar,
          borderRightWidth: 1,
          borderColor: C.sidebarBorder,
          padding: 16,
          gap: 12,
          display: "flex",
          flexDirection: "column",
        }}
      >
        <text style={{ fontSize: 16, fontWeight: 600, color: C.text }}>gol</text>
        <text style={{ fontSize: 13, lineHeight: 18, color: C.secondary }}>{modeCopy(computer, credential)}</text>
      </div>
      <div style={{ display: "flex", flexDirection: "column", flexGrow: 1, minWidth: 0, height: "100%" }}>
        <div style={{ height: 48, paddingLeft: 20, display: "flex", alignItems: "center" }}>
          <text style={{ fontSize: 14, color: C.text }}>Coworker</text>
        </div>
        <div style={{ flexGrow: 1, overflowY: "scroll", paddingLeft: 20, paddingRight: 20, gap: 12, display: "flex", flexDirection: "column" }}>
          {turns.length === 0 && (
            <text style={{ fontSize: 14, color: C.tertiary }}>
              The server keeps the message. Subscription calls the local proxy. Gateway leaves that call on the server.
            </text>
          )}
          {turns.map((turn, index) => (
            <text
              key={`${turn.kind}-${index}`}
              style={{
                fontSize: 14,
                lineHeight: 20,
                color: turn.kind === "status" ? C.tertiary : C.text,
              }}
            >
              {turn.kind === "user" ? `You: ${turn.text}` : turn.kind === "assistant" ? turn.text : turn.text}
            </text>
          ))}
        </div>
        <div style={{ padding: 20, paddingTop: 8 }}>
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              backgroundColor: C.composer,
              borderRadius: 13,
              borderWidth: 1,
              borderColor: C.border,
              paddingTop: 10,
              paddingBottom: 10,
            }}
          >
            <textarea
              testId="composer"
              value={draft}
              placeholder="Message the coworker"
              minRows={1}
              maxRows={3}
              style={{
                width: "100%",
                fontSize: 14,
                lineHeight: 20,
                color: C.text,
                backgroundColor: "#00000000",
                borderWidth: 0,
                paddingLeft: 10,
                paddingRight: 10,
              }}
              onChange={(event: { value?: string }) => setDraft(event.value ?? "")}
              onSubmit={(event: { value?: string }) => send(event.value ?? draft)}
            />
            <div
              style={{
                display: "flex",
                flexDirection: "row",
                alignItems: "center",
                gap: 8,
                marginTop: 8,
                paddingLeft: 10,
                paddingRight: 10,
              }}
            >
              <Segmented
                testId="computer"
                value={computer}
                options={[
                  { id: "local", label: "Local" },
                  { id: "box", label: "Box" },
                ]}
                onChange={(id) => setComputer(id as Computer)}
              />
              <Segmented
                testId="credential"
                value={credential}
                options={[
                  { id: "subscription", label: "Subscription" },
                  { id: "gateway", label: "Gateway" },
                ]}
                onChange={(id) => setCredential(id as Credential)}
              />
              <div style={{ flexGrow: 1 }} />
              <div
                testId="send"
                onClick={() => send(draft)}
                style={{
                  height: 26,
                  paddingLeft: 10,
                  paddingRight: 10,
                  borderRadius: 13,
                  display: "flex",
                  alignItems: "center",
                  cursor: draft.trim() && !busy ? "pointer" : undefined,
                  backgroundColor: draft.trim() && !busy ? C.inverse : C.overlayStrong,
                }}
              >
                <text style={{ fontSize: 13, color: draft.trim() && !busy ? C.onInverse : C.ghost }}>Send</text>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const isEntryPoint =
  typeof Bun !== "undefined"
    ? Bun.isStandaloneExecutable || Bun.main === import.meta.path
    : typeof process !== "undefined" && process.argv[1]?.endsWith("chat.tsx");

if (isEntryPoint) {
  render(<ChatApp />, {
    title: "gol coworker",
    width: 1100,
    height: 760,
    titlebarTransparent: true,
    windowBackground: "blurred",
  });
}
