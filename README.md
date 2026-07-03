# ShipIt 🚀

An autonomous CI/CD platform that detects build failures, uses an AI agent to diagnose the root cause, fix the code, and open a GitHub Pull Request — all without human intervention.

> **"HTTP is the front door. Kafka is the internal hallway."**

---

## Architecture

```
Developer CLI
     │
     │  shipit trigger --repo owner/repo --commit abc123
     ▼
┌──────────────────┐   HTTP    ┌──────────────────┐
│   shipit-cli     │ ────────▶ │ pipeline-service │──── PostgreSQL
│   (Go CLI)       │           │   (Go HTTP :8080)│
└──────────────────┘           └────────┬─────────┘
                                        │
                               [pipeline.triggered]  ← Kafka topic
                                        │
                               ┌────────▼─────────┐
                               │  build-service   │  simulates builds
                               └────────┬─────────┘
                                        │
                               [build.completed]  ← Kafka topic
                          ┌─────────────┼─────────────┐
                          ▼             ▼             ▼
               ┌──────────────┐  ┌───────────┐  ┌──────────────┐
               │deploy-service│  │  notify-  │  │  ai-worker   │
               │  (Go)        │  │  service  │  │  (Python)    │
               └──────────────┘  │  (Go)     │  │  LangGraph   │
                                 └───────────┘  └──────┬───────┘
                                                        │
                                               Inspector → Analyst
                                               → Fixer  → Critic
                                                        │
                                                  GitHub PR ✅
```

---

## Services

| Service | Language | Port | Role |
|---|---|---|---|
| `shipit-cli` | Go | — | Developer CLI tool |
| `pipeline-service` | Go | 8080 | HTTP API + PostgreSQL pipeline state |
| `build-service` | Go | — | Kafka consumer, simulates builds |
| `deploy-service` | Go | — | Kafka consumer, simulates deployments |
| `notify-service` | Go | — | Kafka consumer, sends notifications |
| `ai-worker` | Python | — | LangGraph AI agent — diagnoses failures, opens PRs |
| Kafka | — | 9092 | Internal message bus (KRaft, no Zookeeper) |
| PostgreSQL | — | 5432 | Pipeline state store |

---

## The AI Agent

When a build fails, the `ai-worker` runs a **4-node LangGraph pipeline**:

```
Inspector → Analyst → Fixer → Critic
                ↑              │
                └──────────────┘   (up to 6 retry loops)
```

| Agent | Role |
|---|---|
| **Inspector** | Reads the build log, fetches ALL failing source files from GitHub, classifies the error |
| **Analyst** | Diagnoses root cause across all files with a per-file fix strategy. Absorbs full Critic history on retries. |
| **Fixer** | Fetches live file content from GitHub, generates complete fixed versions, creates a branch, pushes all files, opens the PR |
| **Critic** | Reviews every fixed file against the original errors. Accepts (≥80% confidence) or rejects with specific feedback. |

The agent uses **Cerebras** (llama-3.3-70b) for ultra-fast inference and **Pydantic structured outputs** to force machine-readable JSON from every LLM call.

---

## Quickstart

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose
- [Go 1.22+](https://go.dev/dl/) (for the CLI)
- A [Cerebras API key](https://cloud.cerebras.ai)
- A GitHub Personal Access Token with `repo` scope

### 1. Clone & configure

```bash
git clone https://github.com/DharhshiniVJ/dx-agent.git
cd shipit

# Copy the example env file and fill in your keys
cp ai-worker/.env.example ai-worker/.env
# Edit ai-worker/.env with your CEREBRAS_API_KEY, GITHUB_TOKEN, GITHUB_REPO
```

### 2. Start everything

```bash
docker compose up -d
```

This starts Kafka, PostgreSQL, and all 5 microservices.

### 3. Install the CLI

```bash
cd shipit-cli
go install .
```

### 4. Trigger a pipeline

```bash
shipit trigger --repo your-org/your-repo --commit main
```

### 5. Check the status

```bash
shipit status <pipeline-id>
shipit list
```

---

## Environment Variables

All secrets live in `ai-worker/.env` (never committed — see `.gitignore`).

| Variable | Where used | Description |
|---|---|---|
| `CEREBRAS_API_KEY` | ai-worker | LLM inference API key |
| `GITHUB_TOKEN` | ai-worker | PAT for reading files and opening PRs |
| `GITHUB_REPO` | ai-worker | Target repo (`owner/repo`) for PRs |
| `KAFKA_BROKER` | All services | Set automatically by Docker Compose |
| `DB_HOST` | pipeline-service, deploy-service | Set automatically by Docker Compose |

See [`ai-worker/.env.example`](ai-worker/.env.example) for the full template.

---

## How the Kafka Flow Works

```
POST /trigger
   → pipeline-service stores run in PostgreSQL (status: pending)
   → publishes to [pipeline.triggered]

build-service consumes [pipeline.triggered]
   → simulates a build (10s, random pass/fail)
   → publishes to [build.completed] with status + build log

[build.completed] fans out to 3 consumers:
   → pipeline-service   updates status in PostgreSQL
   → deploy-service     deploys if success, skips if failed
   → ai-worker          if failed → runs LangGraph → opens GitHub PR
```

Each consumer has its own **Kafka consumer group ID** so every service gets every message independently.

---

## Project Structure

```
shipit/
├── pipeline-service/     Go HTTP API + Kafka producer
├── build-service/        Go Kafka consumer (build simulator)
├── deploy-service/       Go Kafka consumer (deploy simulator)
├── notify-service/       Go Kafka consumer (notifications)
├── shipit-cli/           Go CLI tool
├── ai-worker/
│   ├── agents/
│   │   ├── inspector.py  Classifies failures, fetches files from GitHub
│   │   ├── analyst.py    Diagnoses root cause
│   │   ├── fixer.py      Generates fixes, pushes to GitHub, opens PR
│   │   └── critic.py     Validates fixes, gates the PR
│   ├── tools/
│   │   └── github_tools.py  GitHub REST API helpers
│   ├── graph.py          LangGraph pipeline definition
│   ├── models.py         Pydantic schemas for structured LLM outputs
│   ├── main.py           Kafka consumer entry point
│   └── .env.example      Template for secrets
└── docker-compose.yml    Full stack orchestration
```

---

## Safety Model

The AI agent **never merges to main**. It:

1. Creates a `shipit/fix-<pipeline-id>` branch
2. Pushes the fix there
3. Opens a Pull Request
4. **Stops** — a human reviews and merges

```
AI Agent                       You
────────────────────────────────────────
Detects failure
Diagnoses + fixes
Opens PR ──────────────────────▶ Review diff
                                 Approve
                                 Merge  ← only you can do this
```

---

## Roadmap

- [ ] QA approval gate — manual hold before deploy proceeds
- [ ] GitHub Actions — lint, test, build/push Docker images
- [ ] Prometheus + Grafana — metrics and dashboards
- [ ] Kubernetes — K8s manifests for all services
- [ ] OpenTelemetry — distributed tracing across the full pipeline

---

## Tech Stack

| Layer | Technology |
|---|---|
| Microservices | Go (standard library + kafka-go + lib/pq) |
| AI Agent | Python, LangGraph, LangChain, Cerebras LLM |
| Message Bus | Apache Kafka (KRaft mode) |
| Database | PostgreSQL |
| Containerization | Docker Compose |
| LLM Structured Output | Pydantic |
| GitHub Integration | GitHub REST API v3 |
