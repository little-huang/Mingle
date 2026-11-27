import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { InboundMessage, OnboardingEvent } from "../src/channels/channel.ts";
import { dingtalkApi, dingtalkChannel, type StreamClientLike, toInbound } from "../src/channels/dingtalk/dingtalk.ts";
import { until } from "./helpers.ts";

/** A fake DingTalk HTTP API: routes by path, records calls. */
function fakeHttp(routes: Record<string, (body: Record<string, unknown>) => unknown>) {
  const calls: { path: string; method: string; body: Record<string, unknown>; url: URL; form?: FormData }[] = [];
  const fetchImpl = (async (input: string | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const form = init?.body instanceof FormData ? init.body : undefined;
    const body = init?.body && !form ? (JSON.parse(String(init.body)) as Record<string, unknown>) : {};
    calls.push({ path: url.pathname, method: init?.method ?? "GET", body, url, ...(form ? { form } : {}) });
    const route = routes[url.pathname];
    return Response.json(route ? route(body) : {});
  }) as typeof fetch;
  return { fetchImpl, calls };
}

function fakeStream() {
  let listener: ((event: { headers: { messageId: string }; data: string }) => void) | undefined;
  const acks: string[] = [];
  const client: StreamClientLike = {
    registerCallbackListener(_topic, callback) {
      listener = callback;
    },
    socketCallBackResponse(messageId) {
      acks.push(messageId);
    },
    async connect() {},
    disconnect() {},
  };
  return {
    client,
    acks,
    push: (id: string, data: unknown) => listener?.({ headers: { messageId: id }, data: JSON.stringify(data) }),
  };
}

const base = {
  msgId: "msg1",
  conversationId: "cid1",
  senderStaffId: "staff1",
  senderNick: "张三",
  createAt: 1_700_000_000_000,
};

describe("dingtalk messages", () => {
  it("maps direct and group messages", () => {
    expect(toInbound({ ...base, conversationType: "1", msgtype: "text", text: { content: " 你好 " } })).toMatchObject({
      platformId: "msg1",
      chat: { platformId: "staff1", kind: "dm", name: "张三" },
      sender: { id: "staff1", name: "张三" },
      text: "你好",
      addressedToBot: true,
    });
    expect(
      toInbound({
        ...base,
        conversationType: "2",
        conversationTitle: "徒步群",
        isInAtList: true,
        msgtype: "text",
        text: { content: "周六？" },
      }),
    ).toMatchObject({ chat: { platformId: "cid1", kind: "group", name: "徒步群" }, addressedToBot: true });
  });

  it("lists pictures and files by download code", () => {
    const inbound = toInbound({
      ...base,
      conversationType: "1",
      msgtype: "file",
      content: { downloadCode: "dc1", fileName: "route.gpx" },
    }) as InboundMessage;
    expect(inbound.attachments).toEqual([{ id: "dc1", name: "route.gpx" }]);
  });
});

describe("dingtalk onboarding", () => {
  it("registers with DingTalk's device-registration source and returns the app credentials", async () => {
    let polls = 0;
    const http = fakeHttp({
      "/app/registration/init": () => ({ errcode: 0, nonce: "n1" }),
      "/app/registration/begin": () => ({
        errcode: 0,
        device_code: "d1",
        verification_uri_complete: "https://open-dev.dingtalk.com/openapp/registration/openClaw?user_code=AB",
        expires_in: 600,
        interval: 0.01,
      }),
      "/app/registration/poll": () =>
        ++polls < 2
          ? { errcode: 0, status: "WAITING" }
          : { errcode: 0, status: "SUCCESS", client_id: "ding1", client_secret: "sec" },
    });
    const events: OnboardingEvent[] = [];
    for await (const e of dingtalkChannel({ api: dingtalkApi(http.fetchImpl) }).onboard(
      undefined,
      new AbortController().signal,
      { agentName: "Alex" },
    )) {
      events.push(e);
    }
    expect(events.map((e) => e.kind)).toEqual(["qr_ready", "succeeded"]);
    expect(events[0]).toMatchObject({
      qrContent: "https://open-dev.dingtalk.com/openapp/registration/openClaw?user_code=AB",
    });
    expect(events[1]).toMatchObject({ credentials: { clientId: "ding1", clientSecret: "sec", name: "Alex" } });
    expect(http.calls[0]?.body).toEqual({ source: "DING_DWS_CLAW" });
  });
});

describe("dingtalk connection", () => {
  it("keeps retrying a failed stream reconnect instead of going silent", async () => {
    vi.useFakeTimers();
    try {
      let attempts = 0;
      const stream = fakeStream();
      // first connect succeeds; the library's later reconnects (client.connect) fail twice
      stream.client.connect = async () => {
        attempts += 1;
        if (attempts === 2 || attempts === 3) throw new Error("socket disconnected before TLS");
      };
      const connection = await dingtalkChannel({
        api: dingtalkApi(
          fakeHttp({ "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }) }).fetchImpl,
        ),
        createClient: () => stream.client,
      }).connect(
        { clientId: "ding1", clientSecret: "sec", name: "Alex" },
        { onMessage: () => {}, onState: () => {}, onMembership: () => {} },
      );
      const errors = vi.spyOn(console, "error").mockImplementation(() => {});
      await stream.client.connect(); // what dingtalk-stream does on socket close
      expect(attempts).toBe(2);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(attempts).toBe(3);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(attempts).toBe(4); // recovered
      await vi.advanceTimersByTimeAsync(60_000);
      expect(attempts).toBe(4); // no retries after success
      expect(errors).toHaveBeenCalledTimes(2);
      errors.mockRestore();
      await connection.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("reconnects a socket that went down silently or whose reconnect hangs", async () => {
    vi.useFakeTimers();
    try {
      let attempts = 0;
      const stream = fakeStream();
      const client = stream.client as typeof stream.client & { connected?: boolean };
      client.connected = true;
      // attempt 1 is the first connect; attempt 2 hangs forever; attempt 3 succeeds
      client.connect = () => {
        attempts += 1;
        if (attempts === 2) return new Promise<void>(() => {});
        client.connected = true;
        return Promise.resolve();
      };
      const connection = await dingtalkChannel({
        api: dingtalkApi(
          fakeHttp({ "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }) }).fetchImpl,
        ),
        createClient: () => client,
      }).connect(
        { clientId: "ding1", clientSecret: "sec", name: "Alex" },
        { onMessage: () => {}, onState: () => {}, onMembership: () => {} },
      );
      const errors = vi.spyOn(console, "error").mockImplementation(() => {});
      await vi.advanceTimersByTimeAsync(60_000);
      expect(attempts).toBe(1); // up: the watchdog leaves it alone
      client.connected = false; // socket closed and the library's own reconnect never ran
      await vi.advanceTimersByTimeAsync(30_000);
      expect(attempts).toBe(2); // watchdog reconnects; this attempt hangs
      await vi.advanceTimersByTimeAsync(30_000);
      expect(attempts).toBe(2); // still within a minute
      await vi.advanceTimersByTimeAsync(30_000);
      expect(attempts).toBe(3); // stuck over a minute: started another, which connects
      await vi.advanceTimersByTimeAsync(120_000);
      expect(attempts).toBe(3);
      expect(errors).toHaveBeenCalledTimes(2);
      errors.mockRestore();
      await connection.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("acknowledges callbacks, replies through the right API, and drives the AI card", async () => {
    const http = fakeHttp({ "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }) });
    const stream = fakeStream();
    const received: InboundMessage[] = [];
    const connection = await dingtalkChannel({
      api: dingtalkApi(http.fetchImpl),
      createClient: () => stream.client,
    }).connect(
      { clientId: "ding1", clientSecret: "sec", name: "Alex" },
      { onMessage: (m) => received.push(m), onState: () => {}, onMembership: () => {} },
    );
    stream.push("cb1", { ...base, conversationType: "2", isInAtList: true, msgtype: "text", text: { content: "hi" } });
    await until(() => received.length === 1, 1000);
    expect(stream.acks).toEqual(["cb1"]);

    await connection.react(received[0]?.chat as never, "msg1", "processing");
    expect(http.calls.find((c) => c.path === "/v1.0/robot/emotion/reply")?.body).toMatchObject({
      robotCode: "ding1",
      openMsgId: "msg1",
      openConversationId: "cid1",
      emotionName: "🤔思考中",
    });

    const live = await connection.startLive?.(
      { platformId: "cid1", kind: "group" },
      { replyTo: "msg1", initial: "**正在思考** · 0 秒" },
    );
    await live?.update("**正在处理** · 2 秒");
    expect(await live?.finish("答案")).toMatchObject({ kind: "sent" });
    const created = http.calls.find((c) => c.path === "/v1.0/card/instances/createAndDeliver")?.body;
    expect(created).toMatchObject({
      openSpaceId: "dtv1.card//IM_GROUP.cid1",
      imGroupOpenDeliverModel: { atUserIds: { staff1: "张三" } },
    });
    const streamed = http.calls
      .filter((c) => c.path === "/v1.0/card/streaming")
      .map((c) => [c.body.content, c.body.isFinalize]);
    expect(streamed).toEqual([
      ["**正在处理** · 2 秒", false],
      ['<a atId="staff1">张三</a> 答案', true],
    ]);

    await connection.send({ chat: { platformId: "staff1", kind: "dm" }, text: "私聊消息" });
    expect(http.calls.find((c) => c.path === "/v1.0/robot/oToMessages/batchSend")?.body).toMatchObject({
      robotCode: "ding1",
      userIds: ["staff1"],
      msgKey: "sampleMarkdown",
    });
    await connection.close();
  });

  it("uploads files to the media store and sends images inline, other files as files", async () => {
    let media = 0;
    const http = fakeHttp({
      "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }),
      "/media/upload": () => ({ errcode: 0, media_id: `@media${++media}` }),
    });
    const connection = await dingtalkChannel({
      api: dingtalkApi(http.fetchImpl),
      createClient: () => fakeStream().client,
    }).connect(
      { clientId: "ding1", clientSecret: "sec" },
      { onMessage: () => {}, onState: () => {}, onMembership: () => {} },
    );
    const dir = await mkdtemp(join(tmpdir(), "mingle-dingtalk-"));
    await writeFile(join(dir, "map.png"), "png");
    await writeFile(join(dir, "plan.xlsx"), "xlsx");
    const result = await connection.send({
      chat: { platformId: "cid1", kind: "group" },
      text: "",
      files: [{ path: join(dir, "map.png") }, { path: join(dir, "plan.xlsx") }],
    });
    expect(result).toMatchObject({ kind: "sent", degraded: [] });
    const uploads = http.calls.filter((c) => c.path === "/media/upload");
    expect(uploads.map((c) => [c.url.searchParams.get("type"), c.url.searchParams.get("access_token")])).toEqual([
      ["image", "tok"],
      ["file", "tok"],
    ]);
    expect((uploads[1]?.form?.get("media") as File | undefined)?.name).toBe("plan.xlsx");
    const sent = http.calls.filter((c) => c.path === "/v1.0/robot/groupMessages/send").map((c) => c.body);
    expect(sent).toMatchObject([
      { openConversationId: "cid1", msgKey: "sampleImageMsg", msgParam: JSON.stringify({ photoURL: "@media1" }) },
      {
        msgKey: "sampleFile",
        msgParam: JSON.stringify({ mediaId: "@media2", fileName: "plan.xlsx", fileType: "xlsx" }),
      },
    ]);
    await connection.close();
  });

  it("puts images sent with text inside the markdown, where the text names them", async () => {
    let media = 0;
    const http = fakeHttp({
      "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }),
      "/media/upload": () => ({ errcode: 0, media_id: `@media${++media}` }),
    });
    const connection = await dingtalkChannel({
      api: dingtalkApi(http.fetchImpl),
      createClient: () => fakeStream().client,
    }).connect(
      { clientId: "ding1", clientSecret: "sec" },
      { onMessage: () => {}, onState: () => {}, onMembership: () => {} },
    );
    const dir = await mkdtemp(join(tmpdir(), "mingle-dingtalk-"));
    for (const name of ["a.png", "b.png", "plan.xlsx"]) await writeFile(join(dir, name), name);
    const result = await connection.send({
      chat: { platformId: "cid1", kind: "group" },
      text: "第 1 项\n\n![图1](evidence/a.png)\n\n第 2 项",
      files: [{ path: join(dir, "a.png") }, { path: join(dir, "b.png") }, { path: join(dir, "plan.xlsx") }],
    });
    expect(result).toMatchObject({ kind: "sent", degraded: [] });
    const sent = http.calls.filter((c) => c.path === "/v1.0/robot/groupMessages/send").map((c) => c.body);
    expect(sent).toMatchObject([
      {
        msgKey: "sampleMarkdown",
        msgParam: JSON.stringify({
          title: "第 1 项",
          text: "第 1 项\n\n![图1](@media1)\n\n第 2 项\n\n![b.png](@media2)",
        }),
      },
      { msgKey: "sampleFile" },
    ]);
    await connection.close();
  });

  it("@-mentions people in a group through a recent message's reply address", async () => {
    const http = fakeHttp({
      "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }),
      "/robot/sendBySession": () => ({ errcode: 0 }),
    });
    const stream = fakeStream();
    const received: InboundMessage[] = [];
    const connection = await dingtalkChannel({
      api: dingtalkApi(http.fetchImpl),
      createClient: () => stream.client,
    }).connect(
      { clientId: "ding1", clientSecret: "sec", name: "Alex" },
      { onMessage: (m) => received.push(m), onState: () => {}, onMembership: () => {} },
    );
    const group = { platformId: "cid1", kind: "group" as const };
    const mentions = [{ id: "staff2", name: "李四" }];

    // No message from the group yet: no reply address, so the robot API carries the mention.
    expect(await connection.send({ chat: group, text: "@李四 请确认", mentions })).toMatchObject({
      kind: "sent",
      degraded: [],
    });
    const viaRobot = http.calls.filter((c) => c.path === "/v1.0/robot/groupMessages/send");
    expect(viaRobot).toHaveLength(1);
    expect(viaRobot[0]?.body).toMatchObject({
      msgKey: "sampleMarkdown",
      msgParam: JSON.stringify({ title: "@李四 请确认", text: "@staff2 请确认", atUserIds: ["staff2"] }),
    });

    // An address outside dingtalk.com is never used.
    stream.push("cb0", {
      ...base,
      msgId: "msg0",
      conversationType: "2",
      msgtype: "text",
      text: { content: "hi" },
      sessionWebhook: "https://evil.example/robot/sendBySession?session=x",
      sessionWebhookExpiredTime: Date.now() + 3_600_000,
    });
    stream.push("cb1", {
      ...base,
      conversationType: "2",
      msgtype: "text",
      text: { content: "hi" },
      sessionWebhook: "https://oapi.dingtalk.com/robot/sendBySession?session=abc",
      sessionWebhookExpiredTime: Date.now() + 3_600_000,
    });
    await until(() => received.length === 2, 1000);

    expect(await connection.send({ chat: group, text: "@李四 请确认，`@李四` 不算", mentions })).toEqual({
      kind: "sent",
      platformId: expect.any(String),
      degraded: [],
    });
    const viaSession = http.calls.filter((c) => c.path === "/robot/sendBySession");
    expect(viaSession).toHaveLength(1);
    expect(viaSession[0]?.url.hostname).toBe("oapi.dingtalk.com");
    expect(viaSession[0]?.body).toEqual({
      msgtype: "markdown",
      markdown: { title: "@李四 请确认，`@李四` 不算", text: "@staff2 请确认，`@李四` 不算" },
      at: { atUserIds: ["staff2"], isAtAll: false },
    });

    // Without mentions, or in a direct chat, the robot API is used as before.
    await connection.send({ chat: group, text: "普通消息" });
    await connection.send({ chat: { platformId: "staff2", kind: "dm" }, text: "@李四 你好", mentions });
    expect(http.calls.filter((c) => c.path === "/robot/sendBySession")).toHaveLength(1);
    expect(http.calls.filter((c) => c.path === "/v1.0/robot/groupMessages/send")).toHaveLength(2);
    expect(http.calls.filter((c) => c.path === "/v1.0/robot/oToMessages/batchSend")).toHaveLength(1);

    // A live card only @-mentions the asker; anyone else named in the answer is reported as plain.
    const live = await connection.startLive?.(group, { replyTo: "msg1", initial: "…" });
    expect(await live?.finish("@张三 好的", [{ id: "staff1", name: "张三" }])).toEqual({
      kind: "sent",
      platformId: expect.any(String),
    });
    const live2 = await connection.startLive?.(group, { replyTo: "msg1", initial: "…" });
    expect(await live2?.finish("问问 @李四", mentions)).toMatchObject({ degraded: ["mentions_plain"] });
    await connection.close();
  });

  it("mentions through the robot API when DingTalk turns the reply address down", async () => {
    const http = fakeHttp({
      "/v1.0/oauth2/accessToken": () => ({ accessToken: "tok", expireIn: 7200 }),
      "/robot/sendBySession": () => ({ errcode: 300001, errmsg: "session expired" }),
    });
    const stream = fakeStream();
    const received: InboundMessage[] = [];
    const connection = await dingtalkChannel({
      api: dingtalkApi(http.fetchImpl),
      createClient: () => stream.client,
    }).connect(
      { clientId: "ding1", clientSecret: "sec", name: "Alex" },
      { onMessage: (m) => received.push(m), onState: () => {}, onMembership: () => {} },
    );
    stream.push("cb1", {
      ...base,
      conversationType: "2",
      msgtype: "text",
      text: { content: "hi" },
      sessionWebhook: "https://oapi.dingtalk.com/robot/sendBySession?session=abc",
      sessionWebhookExpiredTime: Date.now() + 3_600_000,
    });
    await until(() => received.length === 1, 1000);
    const send = () =>
      connection.send({
        chat: { platformId: "cid1", kind: "group" },
        text: "@李四 在吗",
        mentions: [{ id: "staff2", name: "李四" }],
      });
    expect(await send()).toMatchObject({ kind: "sent", degraded: [] });
    // The refused address is dropped, not tried again.
    expect(await send()).toMatchObject({ kind: "sent", degraded: [] });
    expect(http.calls.filter((c) => c.path === "/robot/sendBySession")).toHaveLength(1);
    expect(http.calls.filter((c) => c.path === "/v1.0/robot/groupMessages/send")).toHaveLength(2);
    await connection.close();
  });
});
