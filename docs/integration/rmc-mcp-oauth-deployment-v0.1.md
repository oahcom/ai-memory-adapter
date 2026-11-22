# RMC MCP OAuth Deployment

Version: 0.1.0
Status: OPERATIONAL
Owner: Infrastructure
Components: RMC Hub, RMC Node, Keycloak, nginx, cloudflared
Host: 43.138.133.238 (Ubuntu)
Public endpoint: `https://mcp.oachom0831.xyz/mcp`
Effective: 2026-11-22

## 1. Purpose

This document records the operational deployment that exposes a remote
machine-control MCP server to ChatGPT through OAuth 2.0.

It covers the request path end to end, the file and service inventory, the
constraints that must not be changed, and the recovery procedure.

Scope is deployment and operations. It does not describe the RMC tool
semantics beyond the authorization model.

## 2. Architecture

```
ChatGPT (web)
    |  HTTPS + OAuth 2.0 Authorization Code + PKCE
    v
Cloudflare  mcp.oachom0831.xyz          valid public TLS
    |  Named Tunnel: relay
    v
cloudflared (host)                      -> https://127.0.0.1:443
    v
nginx :443
    |-- /mcp              -> RMC Hub  127.0.0.1:8787
    |-- /auth/*           -> Keycloak 127.0.0.1:8180
    `-- /.well-known/*    -> Hub / Keycloak
    v
RMC Hub 127.0.0.1:8787
    |-- JWKS signature verification
    |-- validates iss / aud / scope / sub
    `-- WebSocket dispatch
    v
RMC Node  vm-43-ubuntu (uid 1000)
    `-- exec_command, read, write_file, apply_patch, ...
```

### 2.1 Authorization flow

1. Client calls `/mcp` without a token. Hub responds `401` with
   `WWW-Authenticate`, including `resource_metadata`.
2. Client reads `/.well-known/oauth-protected-resource/mcp` for the resource
   identifier and authorization server.
3. Client reads `/.well-known/oauth-authorization-server` for the issuer.
4. Client performs dynamic client registration.
5. Browser is redirected to Keycloak for login and consent, using PKCE S256.
6. Client exchanges the authorization code for tokens carrying
   `sub`, `aud`, and `scope=rmc:tools`.
7. Client repeats `/mcp` with the bearer token and receives `200`.

### 2.2 Authorization model

RMC nodes are identified by a device id and an OS account. A bearer token
grants the native authority of the account running the node service. There is
no command or directory allowlist.

Two authorization layers apply:

- Keycloak decides which user may obtain a token for `rmc:tools`.
- RMC Hub decides which `sub` may act on which device, through
  `oauth.principals` in `hub.json`.

Both must agree. A valid token from a subject absent from `oauth.principals`
is rejected with `credential_rejected`.

## 3. File inventory

| Component | Binary | Configuration |
|-----------|--------|---------------|
| RMC Hub | `/usr/local/bin/rmc-hub` | `/var/lib/rmc/hub.json` |
| RMC Node | `/usr/local/bin/rmc-node` | `/home/ubuntu/.rmc/ubuntu-node.json` |
| Keycloak | `/opt/keycloak/bin/kc.sh` | `/etc/keycloak/keycloak.env` |
| nginx | `/usr/sbin/nginx` | `/etc/nginx/conf.d/rmc-native-ip.conf` |
| cloudflared | `/usr/local/bin/cloudflared` | `/etc/cloudflared/config.yml` |
| systemd units | — | `/etc/systemd/system/rmc-*.service` |

RMC version: `0.3.0-rc.2`, commit `381b3d110c2b`, build `2026-11-10`.
RMC is distributed as a prebuilt binary. No source tree is present on the
host.

Keycloak stores realm state in the `keycloak` PostgreSQL database.

## 4. Service inventory

| Service | Boot | Role |
|---------|------|------|
| `rmc-hub` | enabled | MCP server |
| `rmc-ubuntu-node` | enabled | execution node, uid 1000 |
| `keycloak` | enabled | OAuth authorization server |
| `cloudflared` | enabled | public tunnel |
| `nginx` | enabled | reverse proxy |
| `postgresql@18-main` | enabled-runtime | Keycloak database |
| `rmc-root-node` | disabled | retained for rollback, not started |

Startup ordering is enforced with drop-ins rather than hard requirements:

- `rmc-hub` declares `After=` and `Wants=keycloak.service`.
- `rmc-ubuntu-node` declares `After=` and `Wants=rmc-hub.service`.

`Wants` is used so a Keycloak failure does not cascade into the hub.

## 5. Invariants

The following settings are load-bearing. Changing any of them breaks the
authorization chain.

1. The `rmc:tools` client scope must carry an `oidc-sub-mapper` with
   `lightweight.claim=true`. Keycloak 26 no longer emits `sub` as a built-in
   claim; without this mapper every access token lacks `sub`.
2. Audience mappers must use `included.custom.audience`. The
   `included.client.audience` field accepts a client id only and silently
   ignores a URL.
3. `lightweightAccessTokenEnabled` is `false` at realm level. Lightweight
   tokens omit identity claims other than those explicitly mapped.
4. Every acting user's `sub` must appear in `oauth.principals` in `hub.json`
   with the target device in `devices`.
5. `devices` entries must be explicit identifiers. A wildcard `"*"` aborts
   hub startup with `OAuth node grants must be explicit IDs`.
6. The node must connect to `http://127.0.0.1:8787`. Routing the node through
   the public hostname fails TLS verification against the self-signed origin
   certificate and leaves the device permanently offline.
7. Dynamically registered clients must have `consent_required=false` and
   `oauth2.resource.indicator.allow.list` set to the MCP resource. Database
   triggers in the `keycloak` schema enforce both on insert.
8. `nginx` must forward `X-Forwarded-Proto: https` on `/auth/*`, otherwise
   Keycloak emits insecure-cookie warnings and drops the session.

## 6. Operations

```bash
# status
systemctl status rmc-hub rmc-ubuntu-node keycloak cloudflared nginx

# restart in dependency order
sudo systemctl restart keycloak
sudo systemctl restart rmc-hub
sudo systemctl restart rmc-ubuntu-node

# device state
sudo rmc-hub device list --config /var/lib/rmc/hub.json

# live authorization events
sudo journalctl -u rmc-hub -f | grep -E 'tool_call|auth_denied|device_'

# endpoint health, expected 401
curl -sI https://mcp.oachom0831.xyz/mcp
```

nginx writes bearer tokens presented to `/mcp` to
`/var/log/nginx/mcp_auth.log` under the `auth_log` format. Decode the payload
segment of the JWT to inspect `sub`, `aud`, and `scope` when diagnosing a
rejection.

## 7. Backup

Location: `/var/backups/rmc/`, mode `600`.

- `keycloak-*.dump` — full `pg_dump -Fc` of the Keycloak database
- `hub-*.json` — hub configuration, including principals and device hashes
- `triggers-*.sql` — trigger definitions from the `keycloak` schema
- `nginx-rmc-*.conf`, `cloudflared-*.yml`, `node-ubuntu-*.json`

Restore:

```bash
sudo -u postgres pg_restore -d keycloak --clean /var/backups/rmc/keycloak-<ts>.dump
```

The database dump is the authoritative copy. Realm partial export returns an
empty body on Keycloak 26 and is not usable as a backup.

## 8. Failure modes

| Symptom | Cause | Action |
|---------|-------|--------|
| `401` with `invalid_token` on every call | token lacks `sub`, or `sub` absent from principals | decode `mcp_auth.log`, verify the sub mapper and `oauth.principals` |
| `502` from the tunnel | hub restarting or crashed | `systemctl status rmc-hub`, check for the wildcard-device abort |
| device reports `online: false` | node cannot reach hub | `journalctl -u rmc-ubuntu-node`, confirm `hub_url` is loopback |
| token has `aud` but no `sub` | `rmc:tools` missing the sub mapper | add `oidc-sub-mapper` with `lightweight.claim=true` |
| token has `sub` but no `aud` | mapper uses `included.client.audience` | switch to `included.custom.audience` |
| `credential_rejected` with a valid token | subject not authorized for the device | add the `sub` to `oauth.principals` |

## 9. Known limitations

### 9.1 Effective privilege exceeds the service account

The node runs as `ubuntu` (uid 1000), not root. However `ubuntu` is a member
of the `sudo` group with passwordless sudo. Commands issued through the MCP
surface can therefore escalate to root with `sudo`.

Real privilege reduction requires removing `ubuntu` from the `sudo` group,
which also removes interactive administrative access for that account. A
dedicated management account should be provisioned first.

### 9.2 Single device

One device is registered and authorized: `vm-43-ubuntu`. The former
`vm-43-root` device record remains in `hub.json` but its service is disabled
and it is not present in any principal's `devices` list.

The MCP surface reaches this host only. It does not reach any local
workstation unless a node is installed and enrolled there.

### 9.3 Chat-side token caching

A client that previously received a rejected token may cache the failure and
report `401` without contacting the server. Re-adding the connector forces a
complete authorization cycle.

## 10. Change log

- 2026-11-22 — Initial deployment. OAuth chain completed. Node reduced from
  root to `ubuntu`. Startup ordering and backups added.
