# Tableau Server — Connecting to Skadi SQL Gateway

This guide covers publishing and connecting Tableau Desktop and Tableau Server to the
Skadi SQL Gateway using the **PostgreSQL connector**.

---

## Prerequisites

- Skadi SQL Gateway running and healthy (`GET /actuator/health/pgWire` → `UP`)
- Tableau Desktop 2022.1+ or Tableau Server 2022.1+
- Network path from the Tableau machine to the gateway host on port `15432`
- (Optional) Tableau Server Repository password if publishing data sources

---

## 1. Connect from Tableau Desktop

1. Open Tableau Desktop.
2. In the left panel under **Connect → To a Server**, click **PostgreSQL**.
3. Fill in the connection dialog:

| Field | Value |
|---|---|
| Server | `<skadi-gateway-host>` |
| Port | `15432` |
| Database | `postgres` |
| Username | Your configured user (e.g. `alice`), or any value in `trust` mode |
| Password | User password (leave blank in `trust` mode) |
| Require SSL | Check if TLS proxy is configured; leave unchecked for plain TCP |

4. Click **Sign In**.

Tableau sends a series of `information_schema` queries first — these are served from
the in-memory metadata facade and return quickly. The first data query is forwarded to
Databricks.

### Selecting the schema

After connecting, Tableau shows a database/schema picker:

- **Database**: always `postgres` (the gateway presents a single logical database)
- **Schema**: the schema configured as `skadi.sql-gateway.metadata.dbx-schema`
  (default `default`). Set this to the Unity Catalog schema you want to expose.

---

## 2. Publish a data source to Tableau Server

From Tableau Desktop with an open data source:

1. **Server → Publish Data Source → \<data source name\>**
2. Choose the Tableau Server project.
3. Under **Data Source**, select **Allow refresh access**.
4. Click **Publish**.

Tableau Server will try to refresh the data source on its scheduled interval.
The gateway must be reachable from Tableau Server's network — not just from your desktop.

---

## 3. Embedded credentials

When publishing, Tableau will ask whether to **embed** the Skadi credentials in the
published data source or prompt users at connect time.

- **Embed credentials** — recommended for service accounts. Tableau Server stores the
  username/password and uses them for background refreshes.
- **Prompt users** — users authenticate with their own Skadi credentials when they open
  the workbook from the browser.

> For multi-tenant deployments, `PrincipalPolicy` ACLs in the gateway limit each user to
> their authorized schemas regardless of what the workbook specifies.

---

## 4. Scheduling data source refreshes

On Tableau Server:

1. Open the data source on the server.
2. Click **Schedule** → choose a refresh schedule.
3. Verify the first refresh completes: check **Data Source → Refresh History**.

The gateway serves cached results for repeat queries within the TTL window (`cache.ttl`,
default 5 min). If the schedule fires more frequently than the TTL, Tableau still gets
fresh data, but the gateway deduplicates the underlying Databricks calls.

---

## 5. Firewall / network requirements

| Direction | Source | Destination | Port | Protocol |
|---|---|---|---|---|
| Inbound to gateway | Tableau Desktop / Server | Skadi gateway | 15432 | TCP |
| Outbound from gateway | Skadi gateway | Databricks workspace | 443 | HTTPS (JDBC) |

If Tableau Server sits behind a corporate proxy, ensure the proxy allows outbound TCP
on port 15432 (or whichever port the gateway is on), or use a TLS-terminating proxy
on port 5432.

---

## 6. Configuring SSL

If the gateway is fronted by a TLS-terminating proxy (stunnel or nginx — see
[docker.md](docker.md#tls-in-front-of-the-gateway-recommended-for-production)):

1. In Tableau Desktop connection dialog, check **Require SSL**.
2. In the published data source on Tableau Server, edit the connection and set SSL mode
   to **Required**.
3. Point the connection at the proxy host/port (e.g., port 5432), not the raw gateway.

Without a TLS proxy, leave SSL unchecked. The gateway does not handle TLS natively.

---

## 7. Supported Tableau features

| Feature | Supported | Notes |
|---|---|---|
| Live connection | Yes | Queries forwarded to Databricks in real time |
| Extract | Yes | Tableau pulls rows; gateway streams them |
| Published data source | Yes | Credentials embedded or user-prompted |
| Scheduled refresh | Yes | Background refresh via embedded credentials |
| Row-level security | Partial | Use `PrincipalPolicy` ACL to restrict schemas |
| Custom SQL | Yes | Arbitrary SQL forwarded and translated |
| Cross-database joins | No | Single logical database (`postgres`) |
| Tableau Prep | Yes | Connects via same PostgreSQL connector |

---

## Troubleshooting

See [troubleshooting.md](troubleshooting.md) for common Tableau connection errors and fixes.
