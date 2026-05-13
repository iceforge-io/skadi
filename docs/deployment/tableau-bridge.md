# Tableau Bridge / Tableau Cloud — Connecting to Skadi SQL Gateway

This guide covers connecting **Tableau Cloud** to Skadi SQL Gateway via **Tableau Bridge**,
which runs on-premises and relays queries from Tableau Cloud to the gateway.

---

## Overview

```
Tableau Cloud (cloud.tableau.com)
        │  Tableau Bridge relay (outbound from your network)
        ▼
 Tableau Bridge Agent (on-premises / VPC)
        │  PostgreSQL wire protocol  port 15432
        ▼
  Skadi SQL Gateway
        │  Databricks JDBC  port 443
        ▼
  Databricks SQL Warehouse
```

Tableau Bridge runs inside your network and makes **outbound** connections to Tableau Cloud.
No inbound firewall rules are needed for Tableau Cloud itself — only the Bridge agent needs
outbound TCP 443 to `online.tableau.com` and TCP 15432 to the Skadi gateway.

---

## Prerequisites

- Tableau Cloud site (formerly Tableau Online)
- Tableau Bridge agent installed on a Windows or Linux machine with access to the gateway
- Skadi SQL Gateway running and healthy
- Bridge agent machine can reach `<skadi-host>:15432` over TCP

---

## 1. Install Tableau Bridge

Download Tableau Bridge from your Tableau Cloud site:

1. Sign in to Tableau Cloud.
2. Navigate to **Settings → Bridge**.
3. Download the Bridge client for your OS (Windows recommended; Linux available as of Bridge 2023.1).
4. Install and sign in with your Tableau Cloud site credentials.
5. Leave Bridge running — it will appear in the Bridge pool.

---

## 2. Create the data source in Tableau Desktop

Configure the connection on Tableau Desktop first, then publish it to Tableau Cloud:

1. Open Tableau Desktop.
2. Connect → **PostgreSQL**.
3. Fill in connection details pointing to the Skadi gateway (same as for Tableau Server —
   see [tableau-server.md](tableau-server.md#1-connect-from-tableau-desktop)).
4. Build your view or data model.
5. **Server → Sign In to Tableau Cloud**.
6. **Server → Publish Data Source**.

When publishing, Tableau Desktop will prompt you to choose a **connectivity method**:

- Select **Tableau Bridge** (not "Tableau Extracts" unless you want a static snapshot).

---

## 3. Configure the Bridge connection

After publishing, the data source appears in Tableau Cloud with status **Waiting for Bridge**:

1. In Tableau Cloud, open the data source.
2. Under **Connections**, verify the connection points to your gateway host and port 15432.
3. Under **Scheduled Refreshes**, Bridge will now handle live queries or scheduled extracts.

If the data source shows **Connection Error**, check that:
- The Bridge agent is running and signed in.
- The Bridge machine can reach the gateway host on port 15432.
- Credentials are embedded (Bridge uses them to authenticate to the gateway).

---

## 4. Embedded credentials for Bridge

Bridge requires credentials to be embedded in the published data source for background
refresh to work:

1. During publish from Tableau Desktop, choose **Embed password in connection**.
2. After publishing, in Tableau Cloud under the data source → **Connections → Edit**,
   verify the credentials are stored.

> In `password` auth mode, the gateway validates these credentials against its user store.
> In `trust` mode, any username is accepted (suitable only for dev/staging).

---

## 5. Live connections vs. extracts

| Mode | How it works | When to use |
|---|---|---|
| **Live (via Bridge)** | Tableau Cloud routes each query through Bridge to the gateway; Bridge relays to Databricks | When data must be fresh; Databricks SQL Warehouse is always-on |
| **Extract** | Bridge pulls a full dataset snapshot and uploads to Tableau Cloud | When Databricks Warehouse is serverless (cold start adds latency); when data volume is manageable |
| **Scheduled extract** | Bridge fetches at a schedule; intermediate data stored in Tableau Cloud | Best for most Tableau Cloud deployments |

For extract mode, the gateway handles the full data pull (which may be slow for large tables).
The gateway's cache does not help significantly here since extracts are typically ad-hoc large scans.

---

## 6. Firewall and network summary

| Connection | Source | Destination | Port |
|---|---|---|---|
| Bridge → Tableau Cloud | Bridge agent | `online.tableau.com` | 443 (HTTPS) |
| Bridge → Skadi gateway | Bridge agent | `<skadi-host>` | 15432 (TCP) |
| Skadi gateway → Databricks | Gateway | `<workspace>.azuredatabricks.net` | 443 (HTTPS) |

No inbound ports needed on the Bridge machine or the gateway (beyond Tableau Server or
other internal clients that connect directly).

---

## 7. TLS considerations

Tableau Bridge supports SSL on the PostgreSQL connection:

- If the gateway is behind a TLS proxy (port 5432 with SSL), set **SSL Mode = Required**
  in the Tableau Desktop connection dialog before publishing.
- Bridge will preserve the SSL setting from the published data source.

Without TLS: connections between Bridge and the gateway are plain TCP. This is acceptable
when Bridge and the gateway are co-located in the same VPC or private network.

---

## 8. Bridge agent sizing

| Workload | Recommendation |
|---|---|
| \< 10 concurrent users | 4 vCPU, 8 GB RAM, single Bridge agent |
| 10–50 concurrent users | 8 vCPU, 16 GB RAM, or 2 Bridge agents in pool |
| \> 50 concurrent users | Multiple Bridge agents + gateway replicas |

Bridge agents are stateless — add more by installing on additional machines and signing
in to the same Tableau Cloud site.

---

## Troubleshooting

See [troubleshooting.md](troubleshooting.md) for common Bridge connectivity errors and fixes.
