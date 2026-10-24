// Public OpenCode plugin/SDK shape, checked against v1.19.4 generated types.
// No terminal scraping or private history files. Install only after enrollment.
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";

const BUDGET = 4 * 1024 * 1024;

function completedMessages(rows, sessionID) {
  if (!Array.isArray(rows) || rows.length > 2000) throw new Error("Snapshot budget exceeded");
  const output = [];
  for (const row of rows) {
    const info = row?.info;
    if (!info || info.sessionID !== sessionID || typeof info.id !== "string") throw new Error("Invalid message binding");
    if (info.role !== "user" && info.role !== "assistant") continue;
    // An incomplete assistant is a stable-prefix boundary, even if later rows exist.
    if (info.role === "assistant" && !Number.isFinite(info.time?.completed)) break;
    if (info.role === "assistant" && info.error) continue;
    if (!Array.isArray(row.parts)) throw new Error("Invalid message parts");
    const text = row.parts.filter(part => part.type === "text" && !part.synthetic && !part.ignored)
      .map(part => {
        if (part.sessionID !== sessionID || part.messageID !== info.id || typeof part.text !== "string") throw new Error("Invalid text binding");
        return part.text;
      }).join("\n");
    if (!text) continue;
    if (Buffer.byteLength(text) > 256 * 1024) throw new Error("Message budget exceeded");
    const message = { id: info.id, role: info.role, text };
    if (info.role === "assistant" && typeof info.providerID === "string" && typeof info.modelID === "string") {
      message.model = `${info.providerID}/${info.modelID}`;
    }
    output.push(message);
  }
  return output;
}

function enqueue(executable, args, payload) {
  return new Promise((resolve, reject) => {
    const input = JSON.stringify(payload);
    if (Buffer.byteLength(input) > BUDGET) return reject(new Error("Snapshot budget exceeded"));
    const child = spawn(executable, args, { shell: false, stdio: ["pipe", "ignore", "ignore"] });
    const timer = setTimeout(() => { child.kill("SIGTERM"); reject(new Error("Local adapter timed out")); }, 5000);
    child.on("error", () => { clearTimeout(timer); reject(new Error("Local adapter unavailable")); });
    child.on("exit", code => { clearTimeout(timer); code === 0 ? resolve() : reject(new Error("Local adapter rejected snapshot")); });
    child.stdin.on("error", () => {});
    child.stdin.end(input);
  });
}

export const DeltaHistoryPlugin = async ({ client, directory }) => {
  const executable = process.env.DELTA_HISTORY_EXECUTABLE;
  const instance = process.env.DELTA_HISTORY_INSTANCE;
  if (!executable || !isAbsolute(executable) || !instance || !/^[a-z0-9][a-z0-9._-]{0,63}$/.test(instance)) {
    throw new Error("Set an absolute DELTA_HISTORY_EXECUTABLE and enrolled DELTA_HISTORY_INSTANCE");
  }
  const args = [];
  for (const [variable, flag] of [["DELTA_HISTORY_CONFIG", "--config"], ["DELTA_HISTORY_STATE_DIR", "--state-dir"]]) {
    if (process.env[variable]) {
      if (!isAbsolute(process.env[variable])) throw new Error(`${variable} must be absolute`);
      args.push(flag, process.env[variable]);
    }
  }
  args.push("hook", "opencode", "--instance", instance);
  // Only sessions created while this plugin instance is active. No old-session backfill.
  const eligible = new Set();
  let queue = Promise.resolve();
  return {
    event: async ({ event }) => {
      if (event.type === "session.created") {
        const session = event.properties?.info;
        if (session?.directory === directory && !session.parentID) eligible.add(session.id);
        return;
      }
      if (event.type === "session.deleted") {
        eligible.delete(event.properties?.info?.id);
        return;
      }
      const id = event.properties?.sessionID;
      if (event.type !== "session.idle" || !eligible.has(id)) return;
      // Serialize snapshots; failed capture never changes the agent's completion decision.
      queue = queue.catch(() => {}).then(async () => {
        const response = await client.session.messages({ path: { id }, query: { directory } });
        if (response.error) throw new Error("OpenCode message read failed");
        const messages = completedMessages(response.data, id);
        await enqueue(executable, args, { session_id: id, project: directory, messages });
      });
      try { await queue; } catch {
        console.error("Delta history: snapshot not recorded; check local recorder status.");
      }
    },
  };
};
