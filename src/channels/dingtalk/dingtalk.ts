import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, extname, join } from "node:path";
import { promisify } from "node:util";
import { DWClient, TOPIC_ROBOT } from "dingtalk-stream";
import type {
  Capabilities,
  Channel,
  ChannelConnection,
  ChannelEvents,
  ChatRef,
  Credentials,
  Degradation,
  InboundAttachment,
  InboundMessage,
  LiveOptions,
  LiveReply,
  OnboardingEvent,
  OutboundMessage,
  ReactionState,
  SendResult,
} from "../channel.ts";
import { type Mention, mentionSpots, renderMentions } from "../mention.ts";

/**
 * DingTalk (钉钉): app created by scanning (device registration), messages over the
 * Stream connection (no public address), replies through the robot OpenAPI, and an
 * AI card for the live progress view.
 *
 * Endpoints, the shared AI card template and the text-reaction parameters follow
 * dsh-im's implementation, src/channels/dingtalk/, https://github.com/xmanrui/dsh-im
 * (MIT, Copyright (c) xmanrui).
 */

const REGISTRATION_BASE = "https://oapi.dingtalk.com";
const API_BASE = "https://api.dingtalk.com";
/** Device-registration source identifier, as in dsh-im's device-auth.mjs. */
const REGISTRATION_SOURCE = "DING_DWS_CLAW";
/** DingTalk's shared AI card template (streaming markdown in msgContent). */
const AI_CARD_TEMPLATE = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema";
const THINKING = "🤔思考中";
const TEXT_REACTION_ID = "2659900";
const TEXT_REACTION_BACKGROUND = "im_bg_1";
const QR_MAX_MS = 15 * 60_000;
/** DingTalk's media upload takes images and files up to 20 MB. */
const MAX_MEDIA_BYTES = 20 * 1024 * 1024;
/** What the robot's image message (sampleImageMsg) shows inline; everything else goes as a file. */
const IMAGE_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".gif", ".bmp"]);
/** The app is started from Finder, so Homebrew's bin directories are not on its PATH. */
const FFMPEG_DIRS = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"];

/**
 * An mp4's length in whole seconds and its first frame as a JPEG: a video message needs both.
 * Undefined when ffmpeg is not installed or cannot read the file; the caller then sends a file.
 */
async function videoPoster(path: string): Promise<{ seconds: string; poster: Buffer } | undefined> {
  const run = promisify(execFile);
  for (const dir of FFMPEG_DIRS) {
    const out = join(tmpdir(), `mingle-poster-${randomUUID()}.jpg`);
    try {
      const { stdout } = await run(
        join(dir, "ffprobe"),
        ["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", path],
        { timeout: 20_000 },
      );
      await run(join(dir, "ffmpeg"), ["-y", "-v", "error", "-i", path, "-vframes", "1", "-q:v", "3", out], {
        timeout: 30_000,
      });
      return { seconds: String(Math.max(1, Math.round(Number(stdout.trim()) || 1))), poster: await readFile(out) };
    } catch {
      // Not installed in this directory, or the file is not a readable video: try the next one.
    } finally {
      await rm(out, { force: true });
    }
  }
  return undefined;
}

const CAPABILITIES: Capabilities = {
  quoteReply: false,
  reactions: true,
  typingIndicator: false,
  fileUpload: true,
  fileDownload: true,
  listMembers: false,
  // In groups, through the reply address of a recent message there (see sendBySession).
  mentions: true,
  maxTextLength: 5000,
};

type Fetch = typeof fetch;

export interface DingtalkMessage {
  msgId: string;
  conversationId: string;
  conversationType: string; // "1" direct, "2" group
  conversationTitle?: string;
  senderStaffId?: string;
  senderId?: string;
  senderNick?: string;
  isInAtList?: boolean;
  createAt?: number;
  robotCode?: string;
  /** Where to reply to this message; unlike the robot send API it takes `at`. */
  sessionWebhook?: string;
  /** When `sessionWebhook` stops working, epoch milliseconds. */
  sessionWebhookExpiredTime?: number;
  msgtype: string;
  text?: { content?: string };
  content?: {
    downloadCode?: string;
    fileName?: string;
    recognition?: string;
    richText?: { text?: string; downloadCode?: string; type?: string }[];
  };
}

/** The Stream client surface used here. Injected so tests run offline. */
export interface StreamClientLike {
  registerCallbackListener(
    topic: string,
    callback: (event: { headers: { messageId: string }; data: string }) => void,
  ): unknown;
  socketCallBackResponse(messageId: string, result: unknown): void;
  connect(): Promise<void>;
  disconnect(): void;
  /** False while the socket is down (dingtalk-stream's `DWClient.connected`). */
  connected?: boolean;
  /** dingtalk-stream's keep-alive timer, left running when its socket closes. */
  heartbeatIntervallId?: ReturnType<typeof setInterval>;
}

export function dingtalkApi(fetchImpl: Fetch = fetch) {
  const tokens = new Map<string, { token: string; until: number }>();

  async function json<T>(url: string, init: RequestInit): Promise<T> {
    const response = await fetchImpl(url, {
      ...init,
      redirect: "error",
      signal: init.signal ?? AbortSignal.timeout(15_000),
    });
    const body = (await response.json().catch(() => ({}))) as T & { errcode?: number; code?: string; message?: string };
    if (!response.ok || (body.errcode !== undefined && body.errcode !== 0)) {
      throw Object.assign(new Error(`DingTalk ${response.status} ${body.code ?? body.errcode ?? ""}`), {
        status: response.status,
      });
    }
    return body;
  }

  async function token(appKey: string, appSecret: string): Promise<string> {
    const cached = tokens.get(appKey);
    if (cached && cached.until > Date.now()) return cached.token;
    const body = await json<{ accessToken?: string; expireIn?: number }>(`${API_BASE}/v1.0/oauth2/accessToken`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ appKey, appSecret }),
    });
    if (!body.accessToken) throw new Error("DingTalk returned no access token");
    tokens.set(appKey, {
      token: body.accessToken,
      until: Date.now() + Math.max(60, (body.expireIn ?? 7200) - 60) * 1000,
    });
    return body.accessToken;
  }

  const call = async <T>(c: { appKey: string; appSecret: string }, path: string, body: unknown, method = "POST") =>
    json<T>(`${API_BASE}/${path}`, {
      method,
      headers: {
        "content-type": "application/json",
        "x-acs-dingtalk-access-token": await token(c.appKey, c.appSecret),
      },
      body: JSON.stringify(body),
    });

  /** Upload to the media store; the returned media ID is what image and file messages carry. */
  async function uploadMedia(
    c: { appKey: string; appSecret: string },
    file: { bytes: Buffer; name: string },
    type: "image" | "file" | "video",
  ): Promise<string> {
    const url = new URL(`${REGISTRATION_BASE}/media/upload`);
    url.searchParams.set("access_token", await token(c.appKey, c.appSecret));
    url.searchParams.set("type", type);
    const form = new FormData();
    form.append("media", new Blob([new Uint8Array(file.bytes)]), file.name);
    const body = await json<{ media_id?: string }>(url.toString(), {
      method: "POST",
      body: form,
      signal: AbortSignal.timeout(120_000),
    });
    if (!body.media_id) throw new Error("DingTalk returned no media ID");
    return body.media_id;
  }

  /**
   * Post to a message's reply address (`sessionWebhook`). Only DingTalk's own HTTPS hosts are
   * accepted: the address comes from the inbound callback, and the access token goes with it.
   */
  async function sendBySession(c: { appKey: string; appSecret: string }, address: string, body: unknown) {
    const url = trustedSessionWebhook(address);
    if (!url) throw new Error("DingTalk reply address is not on dingtalk.com");
    await json(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-acs-dingtalk-access-token": await token(c.appKey, c.appSecret),
      },
      body: JSON.stringify(body),
    });
  }

  return {
    token,
    call,
    uploadMedia,
    sendBySession,
    register: {
      async begin(signal?: AbortSignal) {
        const post = <T>(path: string, body: unknown) =>
          json<T>(`${REGISTRATION_BASE}${path}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(body),
            ...(signal ? { signal: AbortSignal.any([signal, AbortSignal.timeout(15_000)]) } : {}),
          });
        const init = await post<{ nonce?: string }>("/app/registration/init", { source: REGISTRATION_SOURCE });
        if (!init.nonce) throw new Error("DingTalk returned no nonce");
        const begun = await post<{
          device_code?: string;
          verification_uri_complete?: string;
          expires_in?: number;
          interval?: number;
        }>("/app/registration/begin", { nonce: init.nonce });
        if (!begun.device_code || !begun.verification_uri_complete) throw new Error("DingTalk returned no QR");
        const url = new URL(begun.verification_uri_complete);
        if (url.protocol !== "https:" || !(url.hostname === "dingtalk.com" || url.hostname.endsWith(".dingtalk.com"))) {
          throw new Error("DingTalk returned a QR outside dingtalk.com");
        }
        return {
          deviceCode: begun.device_code,
          url: url.toString(),
          expiresInMs: Math.min((begun.expires_in ?? 600) * 1000, QR_MAX_MS),
          intervalMs: Math.max(1, begun.interval ?? 5) * 1000,
        };
      },
      poll(deviceCode: string, signal?: AbortSignal) {
        return json<{ status?: string; client_id?: string; client_secret?: string }>(
          `${REGISTRATION_BASE}/app/registration/poll`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ device_code: deviceCode }),
            ...(signal ? { signal: AbortSignal.any([signal, AbortSignal.timeout(15_000)]) } : {}),
          },
        );
      },
    },
  };
}

export type DingtalkApi = ReturnType<typeof dingtalkApi>;

export interface DingtalkOptions {
  readonly api?: DingtalkApi;
  readonly createClient?: (credentials: { clientId: string; clientSecret: string }) => StreamClientLike;
}

export function dingtalkChannel(options: DingtalkOptions = {}): Channel {
  const api = options.api ?? dingtalkApi();
  const create =
    options.createClient ??
    ((c) =>
      new DWClient({
        clientId: c.clientId,
        clientSecret: c.clientSecret,
        keepAlive: true,
      }) as unknown as StreamClientLike);

  return {
    platform: "dingtalk",
    // Only people in the organization can message the bot or add it to a group.
    trust: "org",
    onboarding: { kind: "qr", desktopLink: true },
    manualSetupUrl: "https://open-dev.dingtalk.com/",
    manualFields: [
      { key: "clientId", labelKey: "dingtalk.clientId", secret: false },
      { key: "clientSecret", labelKey: "dingtalk.clientSecret", secret: true },
    ],

    async *onboard(input, signal, context): AsyncIterable<OnboardingEvent> {
      const name = context?.agentName ?? "DingTalk bot";
      if (input?.clientId && input.clientSecret) {
        try {
          await api.token(input.clientId, input.clientSecret);
          yield {
            kind: "succeeded",
            credentials: { clientId: input.clientId, clientSecret: input.clientSecret, name },
            bot: { id: input.clientId, name },
          };
        } catch {
          yield { kind: "failed", code: "invalid_credentials" };
        }
        return;
      }

      let attempt: Awaited<ReturnType<DingtalkApi["register"]["begin"]>>;
      try {
        attempt = await api.register.begin(signal);
      } catch {
        yield { kind: "failed", code: signal.aborted ? "cancelled" : "network" };
        return;
      }
      const deadline = Date.now() + attempt.expiresInMs;
      yield { kind: "qr_ready", qrContent: attempt.url, expiresAt: deadline };
      while (!signal.aborted && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, attempt.intervalMs));
        if (signal.aborted) break;
        let state: { status?: string; client_id?: string; client_secret?: string };
        try {
          state = await api.register.poll(attempt.deviceCode, signal);
        } catch {
          continue;
        }
        const status = String(state.status ?? "").toUpperCase();
        if (status === "SUCCESS" && state.client_id && state.client_secret) {
          yield {
            kind: "succeeded",
            credentials: { clientId: state.client_id, clientSecret: state.client_secret, name },
            bot: { id: state.client_id, name },
          };
          return;
        }
        if (status === "EXPIRED") {
          yield { kind: "failed", code: "expired" };
          return;
        }
        if (status === "FAIL") {
          yield { kind: "failed", code: "denied" };
          return;
        }
      }
      yield { kind: "failed", code: signal.aborted ? "cancelled" : "expired" };
    },

    async connect(credentials: Credentials, events: ChannelEvents): Promise<ChannelConnection> {
      const keys = { appKey: credentials.clientId ?? "", appSecret: credentials.clientSecret ?? "" };
      // For apps created by registration the robot code is the client ID.
      const robotCode = credentials.robotCode ?? keys.appKey;
      await api.token(keys.appKey, keys.appSecret);
      const client = create({ clientId: keys.appKey, clientSecret: keys.appSecret });
      // dingtalk-stream reconnects via `setTimeout(this.connect)` and drops the promise, so one
      // failed endpoint fetch (a network blip) leaves inbound dead for good while sends still
      // work (2026-11-27: TLS error at 11:18, messages @-ing the bot never arrived). Its endpoint
      // fetch also has no timeout and can hang just as silently (same day, 13:06-14:10: no socket,
      // no error). So retry failures with backoff, and every 30 s reconnect a socket that is down
      // with no attempt running, or whose attempt has been stuck for over a minute.
      // dingtalk-stream also never clears the old socket's heartbeat timer, which then pings the
      // new socket while it is still connecting and throws, crashing Mingle (15:06), so clear it
      // before each reconnect.
      const connectOnce = client.connect.bind(client);
      let closed = false;
      let retryMs = 1_000;
      let attemptSince: number | undefined;
      client.connect = async () => {
        const started = Date.now();
        attemptSince = started;
        clearInterval(client.heartbeatIntervallId);
        try {
          await connectOnce();
          retryMs = 1_000;
        } catch (error) {
          if (closed || attemptSince !== started) return;
          console.error(`dingtalk stream reconnect failed, retrying in ${retryMs / 1000}s:`, error);
          setTimeout(() => void client.connect(), retryMs);
          retryMs = Math.min(retryMs * 2, 60_000);
        } finally {
          if (attemptSince === started) attemptSince = undefined;
        }
      };
      const watchdog = setInterval(() => {
        if (closed || client.connected !== false) return;
        if (attemptSince !== undefined && Date.now() - attemptSince < 60_000) return;
        console.error(
          attemptSince === undefined
            ? "dingtalk stream is down with no reconnect running; reconnecting"
            : "dingtalk stream reconnect stuck for over a minute; starting another",
        );
        void client.connect();
      }, 30_000);
      watchdog.unref?.();
      /** Per inbound message: the conversation it came from and who sent it. */
      const origins = new Map<string, { conversationId: string; staffId?: string; nick?: string }>();
      /** Per group: the reply address of the latest message there, the only send that can @ people. */
      const sessions = new Map<string, { address: string; until: number }>();

      client.registerCallbackListener(TOPIC_ROBOT, (event) => {
        try {
          client.socketCallBackResponse(event.headers.messageId, { success: true });
        } catch {}
        let message: DingtalkMessage;
        try {
          message = JSON.parse(event.data) as DingtalkMessage;
        } catch {
          return;
        }
        const inbound = toInbound(message);
        if (!inbound) return;
        origins.set(inbound.platformId, {
          conversationId: message.conversationId,
          ...(message.senderStaffId ? { staffId: message.senderStaffId } : {}),
          ...(message.senderNick ? { nick: message.senderNick } : {}),
        });
        if (origins.size > 2000) origins.delete(origins.keys().next().value as string);
        if (
          inbound.chat.kind === "group" &&
          message.sessionWebhook &&
          trustedSessionWebhook(message.sessionWebhook) &&
          (message.sessionWebhookExpiredTime ?? 0) > Date.now()
        ) {
          sessions.delete(message.conversationId);
          sessions.set(message.conversationId, {
            address: message.sessionWebhook,
            until: message.sessionWebhookExpiredTime as number,
          });
          if (sessions.size > 500) sessions.delete(sessions.keys().next().value as string);
        }
        events.onMessage(inbound);
      });
      await connectOnce(); // first connect still fails loudly (bad keys, offline at start)

      const sendRobot = async (chat: ChatRef, msgKey: string, param: Record<string, string | string[]>) => {
        const msgParam = JSON.stringify(param);
        if (chat.kind === "dm") {
          await api.call(keys, "v1.0/robot/oToMessages/batchSend", {
            robotCode,
            userIds: [chat.platformId],
            msgKey,
            msgParam,
          });
        } else {
          await api.call(keys, "v1.0/robot/groupMessages/send", {
            robotCode,
            openConversationId: chat.platformId,
            msgKey,
            msgParam,
          });
        }
      };
      const title = (text: string) => text.split("\n")[0]?.slice(0, 40) || "Mingle";
      const sendMarkdown = (chat: ChatRef, text: string) =>
        sendRobot(chat, "sampleMarkdown", { title: title(text), text });
      /**
       * A group message that @-mentions people: `@<staffId>` in the text plus the people's IDs is what
       * DingTalk turns into a mention that notifies. It goes through the reply address of a recent
       * message in that group when there is one, otherwise through the robot send API.
       */
      const sendMentioning = async (chat: ChatRef, text: string, mentions: readonly Mention[]): Promise<boolean> => {
        const session = sessions.get(chat.platformId);
        const ids = [...new Set(mentionSpots(text, mentions).map((s) => s.mention.id))];
        const body = renderMentions(text, mentions, (m) => `@${m.id}`);
        // Without a live reply address (none since the last restart, or expired), the robot send API
        // also mentions people when the markdown carries `atUserIds`: one message, no plain-text fallback.
        const viaRobot = async () => {
          await sendRobot(chat, "sampleMarkdown", { title: title(text), text: body, atUserIds: ids });
          return true;
        };
        if (!session || session.until < Date.now() + 30_000) return viaRobot();
        try {
          await api.sendBySession(keys, session.address, {
            msgtype: "markdown",
            markdown: { title: title(text), text: body },
            at: { atUserIds: ids, isAtAll: false },
          });
          return true;
        } catch (error) {
          // DingTalk answered and turned it down (expired, revoked): nothing went out, send it by robot.
          // Without an answer it may have gone out; let the caller report that rather than send it twice.
          if ((error as { status?: number }).status === undefined) throw error;
          sessions.delete(chat.platformId);
          return viaRobot();
        }
      };
      /** Each file is its own message: an inline image where DingTalk can show one, otherwise a file. */
      const sendFile = async (chat: ChatRef, file: { path: string; name?: string }): Promise<boolean> => {
        const { size } = await stat(file.path);
        if (size === 0 || size > MAX_MEDIA_BYTES) return false;
        const name = file.name ?? basename(file.path);
        const extension = extname(name).toLowerCase();
        const bytes = await readFile(file.path);
        const video = extension === ".mp4" ? await videoPoster(file.path) : undefined;
        if (IMAGE_EXTENSIONS.has(extension)) {
          const mediaId = await api.uploadMedia(keys, { bytes, name }, "image");
          await sendRobot(chat, "sampleImageMsg", { photoURL: mediaId });
        } else if (video) {
          // A video message plays inline in the chat; a file message only offers a download.
          const videoMediaId = await api.uploadMedia(keys, { bytes, name }, "video");
          const picMediaId = await api.uploadMedia(keys, { bytes: video.poster, name: "poster.jpg" }, "image");
          await sendRobot(chat, "sampleVideo", { duration: video.seconds, videoMediaId, videoType: "mp4", picMediaId });
        } else {
          const mediaId = await api.uploadMedia(keys, { bytes, name }, "file");
          await sendRobot(chat, "sampleFile", { mediaId, fileName: name, fileType: extension.slice(1) || "file" });
        }
        return true;
      };

      /**
       * Images sent with text go inside the markdown, so a report reads with its screenshots in place:
       * `![图1](01-list.png)` naming an attached image becomes that upload, and attached images the
       * text does not name are added after it. Other files, and images too large to upload, come back
       * to be sent on their own.
       */
      const embedImages = async (
        text: string,
        files: readonly { path: string; name?: string }[],
      ): Promise<{ text: string; rest: { path: string; name?: string }[] }> => {
        const rest: { path: string; name?: string }[] = [];
        const tail: string[] = [];
        for (const file of files) {
          const name = file.name ?? basename(file.path);
          const { size } = await stat(file.path);
          if (!IMAGE_EXTENSIONS.has(extname(name).toLowerCase()) || size === 0 || size > MAX_MEDIA_BYTES) {
            rest.push(file);
            continue;
          }
          const mediaId = await api.uploadMedia(keys, { bytes: await readFile(file.path), name }, "image");
          const named = new RegExp(
            `(!\\[[^\\]]*\\]\\()(?:[^)\\s]*/)?${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\)`,
            "g",
          );
          if (named.test(text)) text = text.replace(named, `$1${mediaId})`);
          else tail.push(`![${name}](${mediaId})`);
        }
        return { text: tail.length ? `${text}\n\n${tail.join("\n\n")}` : text, rest };
      };

      const emotion = async (platformMessageId: string, action: "reply" | "recall") => {
        const origin = origins.get(platformMessageId);
        if (!origin) return;
        await api.call(keys, `v1.0/robot/emotion/${action}`, {
          robotCode,
          openMsgId: platformMessageId,
          openConversationId: origin.conversationId,
          emotionType: 2,
          emotionName: THINKING,
          textEmotion: {
            emotionId: TEXT_REACTION_ID,
            emotionName: THINKING,
            text: THINKING,
            backgroundId: TEXT_REACTION_BACKGROUND,
          },
        });
      };

      return {
        bot: { id: keys.appKey, name: credentials.name ?? "DingTalk bot" },
        capabilities: () => CAPABILITIES,

        async send(message: OutboundMessage): Promise<SendResult> {
          const degraded: Degradation[] = message.replyTo ? ["quote_dropped"] : [];
          let sentAny = false;
          let files = message.files ?? [];
          try {
            if (message.text) {
              const embedded = await embedImages(message.text, files);
              files = embedded.rest;
              const mentioning =
                message.chat.kind !== "dm" &&
                message.mentions?.length &&
                mentionSpots(embedded.text, message.mentions).length > 0;
              if (!mentioning || !(await sendMentioning(message.chat, embedded.text, message.mentions ?? []))) {
                await sendMarkdown(message.chat, embedded.text);
                if (mentioning) degraded.push("mentions_plain");
              }
              sentAny = true;
            }
            for (const file of files) {
              if (await sendFile(message.chat, file)) sentAny = true;
              else if (!degraded.includes("files_dropped")) degraded.push("files_dropped");
            }
            if (!sentAny) return { kind: "failed", code: "too_large", retryable: false };
            return { kind: "sent", platformId: `out-${randomUUID()}`, degraded };
          } catch (error) {
            // The text, or some files, are out already: report what was sent rather than invite a resend.
            if (sentAny) {
              if (!degraded.includes("files_dropped")) degraded.push("files_dropped");
              return { kind: "sent", platformId: `out-${randomUUID()}`, degraded };
            }
            const status = (error as { status?: number }).status ?? 0;
            if (status === 429) return { kind: "failed", code: "rate_limited", retryable: true };
            if (status === 403) return { kind: "failed", code: "forbidden", retryable: false };
            return {
              kind: "failed",
              code: status >= 500 ? "unknown" : "network",
              retryable: status >= 500 || status === 0,
            };
          }
        },

        /** DingTalk shows a "🤔思考中" tag on the message while the agent works; it is removed once done. */
        async react(_chat: ChatRef, platformMessageId: string, state: ReactionState) {
          await emotion(platformMessageId, state === "processing" ? "reply" : "recall").catch(() => {});
        },

        async startLive(chat: ChatRef, live: LiveOptions): Promise<LiveReply> {
          const outTrackId = `mingle_${randomUUID()}`;
          const origin = origins.get(live.replyTo);
          const atUserIds = chat.kind !== "dm" && origin?.staffId ? { [origin.staffId]: origin.nick ?? "" } : undefined;
          const withMention = (text: string) =>
            atUserIds
              ? `${Object.entries(atUserIds)
                  // DingTalk draws the `@` of an atId link itself; writing one here showed `@@nick`.
                  .map(([id, nick]) => `<a atId="${escapeHtml(id)}">${escapeHtml(nick)}</a>`)
                  .join(" ")} ${text}`
              : text;
          const data = (text: string, flowStatus: string) => ({
            cardParamMap: {
              flowStatus,
              msgContent: text,
              staticMsgContent: "",
              sys_full_json_obj: JSON.stringify({ order: ["msgContent"] }),
              config: JSON.stringify({ autoLayout: true }),
            },
          });
          const deliver =
            chat.kind === "dm"
              ? {
                  openSpaceId: `dtv1.card//IM_ROBOT.${chat.platformId}`,
                  imRobotOpenDeliverModel: { spaceType: "IM_ROBOT", robotCode },
                }
              : {
                  openSpaceId: `dtv1.card//IM_GROUP.${chat.platformId}`,
                  imGroupOpenDeliverModel: { robotCode, ...(atUserIds ? { atUserIds } : {}) },
                };
          await api.call(keys, "v1.0/card/instances/createAndDeliver", {
            ...deliver,
            outTrackId,
            userIdType: 1,
            cardTemplateId: AI_CARD_TEMPLATE,
            cardData: data(live.initial, "2"),
            callbackType: "STREAM",
            imGroupOpenSpaceModel: { supportForward: true },
            imRobotOpenSpaceModel: { supportForward: true },
          });
          const stream = (content: string, isFinalize: boolean) =>
            api.call(
              keys,
              "v1.0/card/streaming",
              { outTrackId, guid: randomUUID(), key: "msgContent", content, isFull: true, isFinalize, isError: false },
              "PUT",
            );
          return {
            update: async (markdown) => {
              await stream(markdown, false).catch(() => {});
            },
            finish: async (markdown, mentions) => {
              try {
                const text = withMention(markdown);
                // A card can only @ whom it was delivered to (the asker); anyone else stays plain text.
                const others = mentionSpots(markdown, mentions ?? []).some(
                  (spot) => !atUserIds || !(spot.mention.id in atUserIds),
                );
                await stream(text, true);
                await api.call(
                  keys,
                  "v1.0/card/instances",
                  { outTrackId, cardData: data(text, "3"), cardUpdateOptions: { updateCardDataByKey: true } },
                  "PUT",
                );
                return { kind: "sent", platformId: outTrackId, ...(others ? { degraded: ["mentions_plain"] } : {}) };
              } catch {
                return { kind: "failed", code: "unknown", retryable: false };
              }
            },
          };
        },

        async setTyping() {},

        async download(attachment: InboundAttachment, toPath: string) {
          const { downloadUrl } = await api.call<{ downloadUrl?: string }>(keys, "v1.0/robot/messageFiles/download", {
            downloadCode: attachment.id,
            robotCode,
          });
          if (!downloadUrl) throw new Error("DingTalk returned no download address");
          const response = await fetch(downloadUrl, { signal: AbortSignal.timeout(120_000) });
          if (!response.ok) throw new Error(`download failed: HTTP ${response.status}`);
          await writeFile(toPath, Buffer.from(await response.arrayBuffer()));
        },

        async listMembers() {
          return [];
        },

        async close() {
          closed = true;
          clearInterval(watchdog);
          client.disconnect();
        },
      };
    },
  };
}

/** A `sessionWebhook` from a callback, if it points at DingTalk over HTTPS. */
function trustedSessionWebhook(address: string): string | undefined {
  try {
    const url = new URL(address);
    if (url.protocol !== "https:" || url.username || url.password || url.port) return undefined;
    if (url.hostname !== "dingtalk.com" && !url.hostname.endsWith(".dingtalk.com")) return undefined;
    return url.toString();
  } catch {
    return undefined;
  }
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

/** Map a DingTalk robot callback to Mingle's shape. Exported for tests. */
export function toInbound(message: DingtalkMessage): InboundMessage | undefined {
  if (!message.msgId || !message.conversationId) return undefined;
  const dm = message.conversationType === "1";
  const senderId = message.senderStaffId ?? message.senderId ?? "";
  if (!senderId) return undefined;
  const parts: string[] = [];
  const attachments: InboundAttachment[] = [];
  if (message.text?.content) parts.push(message.text.content.trim());
  if (message.msgtype === "audio" && message.content?.recognition) parts.push(message.content.recognition);
  if (message.msgtype === "picture" && message.content?.downloadCode) {
    attachments.push({ id: message.content.downloadCode, name: `image-${message.msgId}.png`, mime: "image/png" });
  }
  if (message.msgtype === "file" && message.content?.downloadCode) {
    attachments.push({ id: message.content.downloadCode, name: message.content.fileName ?? `file-${message.msgId}` });
  }
  for (const item of message.content?.richText ?? []) {
    if (item.text) parts.push(item.text);
    if (item.downloadCode)
      attachments.push({
        id: item.downloadCode,
        name: `image-${message.msgId}-${attachments.length}.png`,
        mime: "image/png",
      });
  }
  return {
    platformId: message.msgId,
    // Direct chats are addressed by the sender's staff ID, which is what the send API takes.
    chat: dm
      ? { platformId: senderId, kind: "dm", ...(message.senderNick ? { name: message.senderNick } : {}) }
      : {
          platformId: message.conversationId,
          kind: "group",
          ...(message.conversationTitle ? { name: message.conversationTitle } : {}),
        },
    sender: { id: senderId, name: message.senderNick ?? "", isBot: false },
    text: parts.join("\n").trim(),
    attachments,
    // In groups DingTalk only forwards messages that @ the bot.
    addressedToBot: dm || message.isInAtList !== false,
    mentioned: !dm && message.isInAtList !== false,
    sentAt: message.createAt ?? Date.now(),
  };
}
