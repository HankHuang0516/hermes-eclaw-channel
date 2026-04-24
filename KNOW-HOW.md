# Hermes × EClaw Channel — Know-How

踩過的坑與正確做法。以 openclaw-channel-eclaw (TypeScript) 為藍本，Python plugin 移植到 Hermes Agent 時遇到的問題。

---

## 1. EClaw API 參數是 `snake_case` 不是 `camelCase`

文件上常見 `apiKey`、`callbackUrl`，但實際 API 只吃 `snake_case`：

```json
// ❌ 錯
{"apiKey":"eck_...", "callbackUrl":"https://..."}
// server 回 {"success":false,"message":"channel_api_key required"}

// ✅ 對
{"channel_api_key":"eck_...", "callback_url":"https://...", "callback_token":"..."}
```

`entityId` 是例外（用 camelCase）。混用可讀 openclaw-channel-eclaw 的 `src/client.ts` 對照。

---

## 2. EClaw 用 `Authorization: Bearer <callback_token>` 不是 HMAC

Hermes 預設 webhook adapter 驗 HMAC (`X-Hub-Signature-256`)，EClaw 推的是 `Authorization: Bearer <token>`。兩邊不相容。

**POC 繞法**：`~/.hermes/config.yaml` 的 route secret 設成 `INSECURE_NO_AUTH` → 完全跳過 auth 檢查（僅限開發）。

```yaml
platforms:
  webhook:
    extra:
      routes:
        eclaw:
          secret: "INSECURE_NO_AUTH"   # bypass HMAC, EClaw uses Bearer
```

**Production 應該**：寫個 aiohttp middleware 在 Hermes 之前接受 Bearer token，驗證後再轉發到 `/webhooks/eclaw`。或 fork Hermes 加 Bearer auth mode。

---

## 3. Quick Tunnel (`--url`) 在 Docker 容器跑 QUIC 很容易掛

開 `cloudflared tunnel --url http://localhost:8644` 時一直：
```
failed to run the datagram handler error="Application error 0x0"
failed to serve tunnel connection error="control stream encountered a failure"
```

容器網路對 UDP buffer size 不友善，QUIC 建不起來。

**解法**：**用命名 tunnel（Named Tunnel）**，走 Cloudflare Dashboard 或 API 建好，拿 connector token 啟動 cloudflared。命名 tunnel 默認 HTTP/2 fallback 比較穩。

命名 tunnel 還順便得到固定 domain（`hermes-b.eclawbot.com`），重啟不換網址 — 對 EClaw callback registration 很重要，不然每次重啟都要重 register。

---

## 4. Cloudflare API Token 新版是 **帳號範圍**（`cfat_` 前綴）

舊 token (40 字元純 alnum) 用 `/client/v4/user/tokens/verify` 驗證。
新 token (`cfat_RQ9gige...`, 53 字元) 用 **`/client/v4/accounts/{account_id}/tokens/verify`**。

用錯端點會回 `{"success":false,"errors":[{"code":1000,"message":"Invalid API Token"}]}` 誤以為 token 壞了。

**記得**：Cloudflare 給 token 時 dashboard 上會顯示 example curl — 照那個 endpoint 走就對。

---

## 5. Token 權限要同時涵蓋 **Tunnel:Edit** + **DNS:Edit** + **Zone:Read**

少一個就會在某步驟回 `"Authentication error"`（而不是明確的 permission denied），不好 debug。

| 權限 | 做什麼 |
|------|-------|
| `Cloudflare Tunnel: Edit` | 建 tunnel、設 ingress |
| `DNS: Edit`（zone 範圍）| 加 CNAME `hermes-b.eclawbot.com` |
| `Zone: Read`（zone 範圍）| 查 zone_id |

建 token 時 Zone Resources 務必選 `Include → Specific zone → eclawbot.com`（不能 All zones 也不能 none）。

---

## 6. `DELETE /api/device/entity/:id/permanent` **不可逆** — 很危險

本來想 unbind 舊 Mac_B 換 Hermes，看 API hint 說 "Unbind first via DELETE /api/entity/:entityId"。但那個需要 `botSecret`。沒有 → 我跳去用 `/permanent`（`deviceSecret` 即可）。

後果：**Entity slot 永久刪除，ID 不能重建**。後來再 bind `entityId=0` 會回：
```
"entityId 0 does not exist on this device. Available: [1,2,3,4,5]"
```

**正確做法**：
- 要換綁 → 先用 `DELETE /api/entity/:id`（要 botSecret）只 unbind、不刪 entity
- 沒有 botSecret → 去 EClaw portal UI 手動 unbind，而不是繞路用 admin API
- **永遠別用 `/permanent` 除非真要永久清除 entity 歷史**

---

## 7. `openclaw.json` 本地 config 可能跟 server 實際狀態不同步

本以為 Entity 0 = Mac_B（config 寫的），結果 server 上 Mac_B 早解綁，Entity 0 綁著其他 bot。

**規則**：動 entity 前先 `GET /api/entities?deviceId=...&deviceSecret=...` 或 `POST /api/channel/register`（回 entity 列表）查當下實際狀態，**不要信 local config**。

---

## 8. Hermes webhook 路徑固定是 `/webhooks/{route_name}` 不是 `/webhook/{name}`

`~/.hermes/config.yaml` route 名 = `eclaw` → URL = `https://hermes-b.eclawbot.com/webhooks/eclaw`

常被誤寫成 `/webhook/eclaw`（少一個 `s`）或 `/eclaw-webhook`。`/health` 回 200 能快速確認 gateway 本身存活。

---

## 9. Entity slot ID 是線性增加 不是 hash

新建 entity 只能加到**下一個可用 slot**（API: `POST /api/device/add-entity`），不能指定 ID。

目前 device 有 Entity 1-5，Entity 0 已永久刪除。下一次 add-entity 會開出 Entity 6 (不是 0)。

---

## 10. Hermes CLI flags 踩坑

`hermes chat` 的非互動模式：
- `-q QUERY`（單句查詢）— **不是** `-p`。`-p` 被保留給 profile 之類的東西（實際會被當成錯誤）
- `-Q`（quiet mode）— 抑制 banner / spinner / tool preview，只留最終回覆
- stdout **還是會有 `session_id: xxxxx` 那一行**在回覆之前，要自己過濾掉

```python
# 正確
proc = asyncio.create_subprocess_exec("uv", "run", "hermes", "chat", "-Q", "-q", text, ...)
reply = "\n".join(l for l in stdout.decode().splitlines() if not l.startswith("session_id:"))
```

---

## 11. Hermes v0.x → v1 config schema 變更：`provider`/`model` 搬進 `model:` section

舊寫法（hermes doctor 會警告 `Stale root-level config keys`）：
```yaml
provider: minimax
model: MiniMax-M2.7
```

新寫法：
```yaml
model:
  provider: minimax
  name: MiniMax-M2.7
```

**症狀**：用舊寫法時 `hermes chat -q ...` 會回 `No inference provider configured` 即使 `auth.json` 裡有 key。`hermes doctor` 會明確指出 stale keys。

---

## 12. EClaw 會在每個 message body 後面塞 `[Local Variables available: ...]`

這是 EClaw 為了幫 bot 知道有哪些 env vars / skills 可用加的 context block。對外部 bridge 沒用（bridge 本身已經有 env），應該在餵 Hermes 之前 strip 掉，省 token、避免被當成使用者訊息一部分。

```python
def _strip_eclaw_context(text: str) -> str:
    for marker in ("\n[Local Variables available:", "\n[AVAILABLE TOOLS"):
        idx = text.find(marker)
        if idx >= 0:
            text = text[:idx]
    return text.strip()
```

---

## 13. Docker 容器 PID 1 常常不是 init → subprocess 變 zombie 會拖住 asyncio

openclaw-b 的 PID 1 是 `openclaw` 本身（node process），**不 reap 子孫**。從 bridge `asyncio.create_subprocess_exec` spawn 出 hermes process 後若異常退出，`await proc.communicate()` 會永遠卡住。

症狀：bridge log 卡在「event=message... spawning chat」之後沒有 reply/error，`ps` 卻看不到 hermes process。

**解法**：一律包 `asyncio.wait_for(..., timeout=...)`，timeout 時 `proc.kill()` + `await proc.wait()` 兜底。永遠不要假設 subprocess 會乖乖結束。

```python
try:
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90)
except asyncio.TimeoutError:
    proc.kill()
    await proc.wait()
    return "[timeout]"
```

**另一層**：Hermes session file 是本地共享資源，併發 spawn 會搶著寫。用 `asyncio.Lock` 串行化。

---

## 14. Hermes cold start 每次 ~7-9 秒

`hermes chat -q` 冷啟動時間分布：
- Python + venv 載入：~2-3s
- Provider SDK + credential pool 初始化：~2-3s
- 第一次 LLM API 呼叫：~2-3s

省掉 `uv run`（直接執行 `.venv/bin/hermes`）只省 ~0.2s，不顯著。

**目前 POC 接受這個延遲**，因為一對一 chat 且訊息頻率不高。

**未來優化方向**（未實作）：
1. 自寫 Python worker：startup 時 `from run_agent import AIAgent` 一次，listen on Unix socket，bridge 透過 socket 送請求
2. 改用 Hermes MCP server (`hermes mcp`)：JSON-RPC 介面可能支援 keep-alive
3. 改用 Hermes dashboard HTTP API（存在於 `hermes dashboard`）
4. 直接丟開 Hermes 用 SDK（minimax client）— 但失去 Hermes 的 tools/memory/skills

---

## 15a. bot-to-bot / broadcast 本體訊息被 strip 掉（prompt_len ≈ 36 / Hermes 回「沒有附帶任何內容」）

**症狀**：從其他 entity 用 `/api/entity/speak-to` 或 `/api/transform speakTo:[...]` 送訊息給 Hermes，Hermes 回覆「收到空白訊息」。bridge log 顯示：

```
event=entity_message from=2 text='[Bot-to-Bot message from Entity 2 (LOBSTER)]\n[Quota: ...'
[hermes] spawning chat, prompt_len=36    ← 只剩 bridge 自己 prepend 的 header，body 全沒
```

**根因（自家 bridge 的 bug，不是 Hermes）**：

`process_message` 組 prompt 時把 `missionHints`（內容是 `[AVAILABLE TOOLS — Mission Dashboard]...`）插在 `text` 前面：

```python
# ❌ 壞的寫法
hints = eclaw_ctx.get("missionHints", "")
prompt = "\n".join([f"[{prefix} {sender}]", hints, text])
```

然後 `ask_hermes` 呼 `_strip_eclaw_context(prompt)`，marker `\n[AVAILABLE TOOLS` 在 prompt 的**最前面**（就在 bridge header 之後）就命中了，把**後面整個 body 一起截掉**。

關鍵點：EClaw server 的 `materializeChannelText` **已經**把 missionHints 嵌在 `text` 尾端了。bridge 再從 `eclaw_context.missionHints` 讀一次、塞到 body 前面，就等於把 strip marker 往前搬、把 body 炸掉。

**修法**：不要再從 `eclaw_context` 讀 missionHints — text 已經包了。

```python
# ✅ 正確寫法
prompt = f"[{prefix} {sender}]\n{text}" if text else f"[{prefix} {sender}]"
```

這樣 `_strip_eclaw_context` 還是會正確截掉 text 尾端的 `[Local Variables]` / `[AVAILABLE TOOLS]` block，但 body 會完整保留。

**診斷方法（以後碰到 silent-body 問題先跑這個）**：

```python
# 暫時在 process_message 加：
log.info("raw_text_len=%d head400=%r tail=%r", len(text), text[:400], text[-200:])
```

比對 `raw_text_len` 和最終 `prompt_len`：
- 若 raw 很大 / prompt_len 只剩 ~36 → 本 bug（strip 吃掉 body）。
- 若 raw 就接近 envelope 大小 → EClaw server 那邊沒把 body 傳過來，查 backend 的 `unifiedPush` / `channelPayload.text` 覆寫邏輯（`enrichedMessage !== payload.message` 條件）。

---

## 15. Hermes gateway `deliver: log` 只寫 log，不會自動回 reply 給 EClaw

Built-in `deliver` 選項只有 `telegram / discord / slack / github_comment / log`，**沒有「POST 回 source webhook」**。

所以 reply 必須靠 **plugin hook**（`post_llm_call`）主動 POST 回 `/api/channel/message`。這是這個 plugin 存在的主因。

---

## 索引：主要 endpoint cheatsheet

| 動作 | Method | Path | Auth |
|------|--------|------|------|
| Register callback | POST | `/api/channel/register` | channel_api_key |
| Bind entity | POST | `/api/channel/bind` | channel_api_key |
| Send reply | POST | `/api/channel/message` | channel_api_key + botSecret |
| Bot-to-bot | POST | `/api/entity/speak-to` | channel_api_key + botSecret |
| Unbind (safe) | DELETE | `/api/entity/:id` | botSecret |
| **Permanent delete (danger)** | DELETE | `/api/device/entity/:id/permanent` | deviceSecret |
| List entities | GET | `/api/entities?deviceId=...&deviceSecret=...` | deviceSecret |

---

## Related files

- `plugin/eclaw_channel.py` — Hermes plugin: Bearer auth + POST reply back
- `scripts/setup-tunnel.sh` — 建 CF tunnel + DNS + 啟 cloudflared container
- `scripts/bind-entity.sh` — register + bind + 把 botSecret 寫進 keychain
- `scripts/teardown.sh` — 反向清理
