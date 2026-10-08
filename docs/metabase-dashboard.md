# 📊 Metabase Dashboard

A comprehensive guide to building real-time dashboards for the streaming data pipeline using Metabase.

**Version:** 1.0.0
**Last Updated:** 2026-10-08
**Metabase Version:** Latest
**ClickHouse Version:** 24.10

---

## Table of Contents

1. [Overview](#1-overview)
2. [Why Metabase?](#2-why-metabase)
3. [Prerequisites](#3-prerequisites)
4. [Setup](#4-setup)
5. [Connecting to ClickHouse](#5-connecting-to-clickhouse)
6. [Building Questions](#6-building-questions)
7. [Building the Dashboard](#7-building-the-dashboard)
8. [Advanced Features](#8-advanced-features)
9. [Troubleshooting](#9-troubleshooting)
10. [References](#10-references)

---

## 1. Overview

This document describes how to set up **Metabase** — an open-source Business Intelligence (BI) tool — to visualize the data stored in the **Gold layer** of our ClickHouse Medallion Architecture.

### What You'll Build

A real-time dashboard with 5 visualizations:

| # | Chart | Table |
|---|---|---|
| 1 | Total Wiki Edits (Number) | `silver_wiki_events` |
| 2 | Hourly Edit Activity (Line) | `gold_wiki_hourly_stats` |
| 3 | Top 10 Human Users (Bar) | `gold_top_users_daily` |
| 4 | Language Distribution (Pie) | `gold_language_hourly` |
| 5 | Top Pages (Table) | `gold_top_pages_hourly` |

### Architecture

```
┌─────────────────────┐
│   Wikimedia SSE     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Producer.py       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Kafka: wiki-events│
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────┐
│         ClickHouse                  │
│  ┌──────────┬──────────┬──────────┐ │
│  │ BRONZE   │ SILVER   │  GOLD    │ │
│  └──────────┴──────────┴────┬─────┘ │
└─────────────────────────────┼───────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │    Metabase      │
                    │  (Port 3000)     │
                    └──────────────────┘
```

---

## 2. Why Metabase?

### Benefits

| Feature | Description |
|---|---|
| **No-Code Query Builder** | Build questions without writing SQL |
| **Native SQL Support** | Write complex SQL when needed |
| **Auto-Dashboards** | Create dashboards in minutes |
| **Auto-Refresh** | Update visualizations every minute |
| **Sharing** | Public links, embeds, or user-level access |
| **Open-Source** | Free, self-hosted, no vendor lock-in |
| **Multiple Charts** | Line, Bar, Pie, Number, Table, Map, ... |

### Comparison with Alternatives

| Tool | Pros | Cons |
|---|---|---|
| **Metabase** | Easy, fast, self-hosted | Limited advanced analytics |
| **Grafana** | Powerful, great for monitoring | Steeper learning curve |
| **Superset** | Very powerful | Complex setup |
| **Tableau** | Beautiful visuals | Expensive |

For our use case (visualizing Gold data), **Metabase** is the best fit.

---

## 3. Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Docker Desktop | 24.0+ | Running Metabase |
| ClickHouse | 24.10 | Data source |
| Metabase | Latest | BI tool |

**Ensure ClickHouse is running:**

```bash
docker ps | grep clickhouse
```

---

## 4. Setup

### 4.1. Update `docker-compose.yaml`

Add the Metabase service:

```yaml
  metabase:
    image: metabase/metabase:latest
    container_name: metabase
    hostname: metabase
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: h2
      MB_DB_FILE: /metabase-data/metabase.db
      JAVA_OPTS: "-Xmx1g"
    volumes:
      - metabase-data:/metabase-data
      - ./metabase-plugins:/plugins
    networks:
      - services
    depends_on:
      clickhouse:
        condition: service_healthy
    restart: unless-stopped
```

Add the volume:

```yaml
volumes:
  clickhouse-volume:
  kafka-volume:
  metabase-data:
```

### 4.2. Install the ClickHouse Driver

Metabase doesn't ship with ClickHouse support by default. You need to add the driver JAR.

```bash
# Create the plugins folder
mkdir -p metabase-plugins

# Download the driver
curl -L -o metabase-plugins/clickhouse.metabase-driver.jar \
  https://github.com/ClickHouse/metabase-clickhouse-driver/releases/latest/download/clickhouse.metabase-driver.jar
```

Verify:

```bash
ls -lh metabase-plugins/
```

You should see a file around 20-30 MB.

### 4.3. Start Metabase

```bash
make up
```

Or manually:

```bash
docker compose up -d metabase
```

### 4.4. Access Metabase

Open your browser:

```
http://localhost:3000
```

On first run, Metabase asks you to create an admin account. Fill in the form.

---

## 5. Connecting to ClickHouse

### 5.1. Navigate to Admin Settings

1. Click the **gear icon** (top-right).
2. Select **Admin settings**.

### 5.2. Add a Database

1. Click **Databases** (left menu).
2. Click **Add a database**.
3. Select **ClickHouse** from the list.

### 5.3. Fill the Form

| Field | Value | Notes |
|---|---|---|
| **Display name** | `ClickHouse Tutorial` | Any name you like |
| **Connection string** | *(leave empty)* | Use the fields below |
| **Host** | `clickhouse` | ⚠️ Docker service name, **not** `localhost` |
| **Port** | `8123` | HTTP port |
| **Username** | `admin` | ClickHouse user |
| **Password** | `admin123` | ClickHouse password |
| **Enable multiple databases** | OFF | We only use `tutorial` |
| **Database name** | `tutorial` | Our database |

### 5.4. Save

Click **Connect database**. Metabase will start syncing schemas (this takes a few minutes).

### ⚠️ Important Note About Host

Since Metabase runs inside a Docker container, it **cannot** access ClickHouse via `localhost`. Use the Docker service name: `clickhouse`.

To verify:

```bash
docker exec -it metabase ping clickhouse
```

You should see responses from the ClickHouse container.

---

## 6. Building Questions

Metabase has two ways to build questions:
1. **Question Builder** (no code)
2. **SQL Query** (write SQL) — recommended for ClickHouse

We'll use **SQL Query** because ClickHouse has specific SQL syntax.

### 6.1. Total Wiki Edits (Number)

**Create a new question:**

- Click **+ New** (top-right).
- Select **SQL query**.
- Select database: `ClickHouse Tutorial`.

**SQL:**

```sql
SELECT count() AS total_edits
FROM tutorial.silver_wiki_events;
```

**Visualization:** `Number`

**Save as:** `Total Wiki Edits`

**Screenshot:**

![Total Wiki Edits](images/total-edits.png)

---

### 6.2. Hourly Edit Activity (Line)

**SQL:**

```sql
SELECT
    hour,
    sum(total_edits) AS edits
FROM tutorial.gold_wiki_hourly_stats
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour ASC;
```

**Visualization:** `Line`

**Settings:**
- **X-axis:** `hour`
- **Y-axis:** `edits`
- **Title:** `Hourly Edit Activity`

**Save as:** `Hourly Edit Activity`

**Screenshot:**

![Hourly Edit Activity](images/hourly-activity.png)

---

### 6.3. Top 10 Human Users (Bar)

**SQL:**

```sql
SELECT
    user,
    sum(edits) AS total_edits
FROM tutorial.gold_top_users_daily
WHERE user_is_bot = 0
  AND user_is_anonymous = 0
GROUP BY user
ORDER BY total_edits DESC
LIMIT 10;
```

**Visualization:** `Bar`

**Save as:** `Top 10 Human Users`

**Screenshot:**

![Top 10 Human Users](images/top-users.png)

---

### 6.4. Language Distribution (Pie)

**SQL:**

```sql
SELECT
    language,
    sum(total_edits) AS edits
FROM tutorial.gold_language_hourly
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY language
ORDER BY edits DESC
LIMIT 10;
```

**Visualization:** `Pie`

**Settings:**
- **Dimension:** `language`
- **Metric:** `edits`
- **Max slices:** 10

**Save as:** `Language Distribution`

**Screenshot:**

![Language Distribution](images/language-distribution.png)

---

### 6.5. Top Pages (Table)

**SQL:**

```sql
SELECT
    title,
    wiki,
    sum(edits) AS total_edits
FROM tutorial.gold_top_pages_hourly
WHERE hour >= now() - INTERVAL 12 HOUR
GROUP BY title, wiki
ORDER BY total_edits DESC
LIMIT 10;
```

**Visualization:** `Table`

**Save as:** `Top Pages`

**Screenshot:**

![Top Pages](images/top-pages.png)

---

## 7. Building the Dashboard

### 7.1. Create the Dashboard

1. Click **+ New** → **Dashboard**.
2. **Name:** `Wiki Real-time Analytics`.

### 7.2. Add Questions

1. Click **Add a saved question**.
2. Select each of the 5 questions you created.

### 7.3. Arrange the Layout

Recommended layout:

```
┌────────────────────────────────────────────┐
│       Total Wiki Edits (Number)            │
├────────────────────────────────────────────┤
│       Hourly Edit Activity (Line)          │
├──────────────────┬─────────────────────────┤
│  Top 10 Users    │  Language Distribution  │
│  (Bar)           │  (Pie)                  │
├──────────────────┴─────────────────────────┤
│       Top Pages (Table)                    │
└────────────────────────────────────────────┘
```

### 7.4. Enable Auto-Refresh

1. Click the **clock icon** (top-right of dashboard).
2. Select **Auto-refresh** → `1 minute` or `5 minutes`.

### 7.5. Save

Click **Save**.

**Final result:**

![Complete Dashboard](images/dashboard-full.png)

---

## 8. Advanced Features

### 8.1. Add Dashboard Filters

Filters let users interactively change the data range:

1. In the dashboard, click **Edit** (pencil icon).
2. From the right panel, click **Add a filter** → **Time** → **Hour**.
3. Connect each chart to this filter.

### 8.2. Share the Dashboard

**Public Link:**

1. Click the **Sharing** icon (top-right).
2. Enable **Public link**.
3. Copy the URL.

> ⚠️ **Warning:** Anyone with the link can view the dashboard.

**Embed in a Website:**

```html
<iframe
    src="http://localhost:3000/public/dashboard/YOUR-UUID"
    frameborder="0"
    width="100%"
    height="800"
    allowtransparency
></iframe>
```

### 8.3. Scheduled Reports

Metabase can email dashboards on a schedule:

1. Go to **Dashboard → Subscriptions**.
2. Set a schedule (e.g., daily at 9 AM).
3. Add email recipients.

### 8.4. Alerts

Set up alerts when a metric crosses a threshold:

1. Open any question.
2. Click **Bell icon** (top-right).
3. Set condition (e.g., "when total_edits > 10000").
4. Enter email.

### 8.5. Row-Level Permissions

For multi-user setups:

1. Go to **Admin settings → Permissions**.
2. Create groups (e.g., `Analysts`, `Viewers`).
3. Set collection and data permissions.

---

## 9. Troubleshooting

### Error: `Connection refused`

**Cause:** Metabase cannot reach ClickHouse.

**Solution:**

1. Verify the **Host** is `clickhouse`, not `localhost`:
   ```bash
   docker exec -it metabase ping clickhouse
   ```
2. Verify both containers are on the same network:
   ```bash
   docker inspect metabase --format "{{json .NetworkSettings.Networks}}"
   docker inspect clickhouse --format "{{json .NetworkSettings.Networks}}"
   ```

### Error: `Authentication failed`

**Cause:** Wrong username or password.

**Solution:**

```bash
docker exec -it clickhouse clickhouse-client --user admin --password admin123 --query "SELECT currentUser()"
```

If this works, the credentials are correct.

### Error: `Unknown codec family code: 0`

**Cause:** Corrupted data parts (usually from ClickHouse 24.8).

**Solution:**

1. Upgrade to ClickHouse 24.10+:
   ```yaml
   image: clickhouse/clickhouse-server:24.10
   ```
2. Reset everything:
   ```bash
   make reset
   ```

### Error: `No driver for ClickHouse`

**Cause:** Driver JAR not found.

**Solution:**

```bash
ls -lh metabase-plugins/
# Should show: clickhouse.metabase-driver.jar

# Restart Metabase
docker restart metabase
```

### Error: `Query took too long`

**Cause:** Querying large tables (Bronze, Silver) directly.

**Solution:**

1. Use **Gold tables** only (they're aggregated).
2. Add `WHERE hour >= now() - INTERVAL 24 HOUR`.
3. Add `LIMIT`.

### Error: `Table doesn't exist`

**Cause:** Wrong table name or database.

**Solution:**

```bash
docker exec -it clickhouse clickhouse-client --user admin --password admin123 --query "SHOW TABLES FROM tutorial"
```

Use the exact table name in your SQL.

### Charts Not Refreshing

**Cause:** Auto-refresh disabled.

**Solution:**

1. Click the **clock icon** in dashboard.
2. Set **Auto-refresh** to `1 minute`.

### Empty Charts

**Cause:** No data or filters too strict.

**Solution:**

1. Check the Producer is running:
   ```bash
   docker logs clickhouse | tail -20
   ```
2. Check data exists:
   ```bash
   docker exec -it clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.silver_wiki_events"
   ```
3. Loosen filters in Metabase.

---

## 10. References

### Official Documentation

- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [ClickHouse Metabase Driver](https://github.com/ClickHouse/metabase-clickhouse-driver)
- [Metabase ClickHouse Setup](https://www.metabase.com/docs/latest/databases/connections/clickhouse)

### Related Articles

- [Building Effective Dashboards](https://www.metabase.com/learn/building-analytics/dashboards)
- [SQL Snippets in Metabase](https://www.metabase.com/learn/sql-questions/sql-snippets)

### Similar Projects

- [Metabase Examples](https://github.com/metabase/metabase/tree/master/resources)
- [ClickHouse + Metabase Tutorial](https://clickhouse.com/docs/en/integrations/metabase)

---

## Appendix: Quick Setup Checklist

- [ ] Add `metabase` service to `docker-compose.yaml`
- [ ] Add `metabase-data` volume
- [ ] Create `metabase-plugins/` folder
- [ ] Download ClickHouse driver JAR
- [ ] Run `make up`
- [ ] Open `http://localhost:3000`
- [ ] Create admin account
- [ ] Add ClickHouse database connection
- [ ] Build 5 questions
- [ ] Build dashboard
- [ ] Enable auto-refresh
- [ ] Take screenshots for documentation

---

## License

This document is part of the [stream-realtime-data](https://github.com/YOUR-USERNAME/stream-realtime-data) project.