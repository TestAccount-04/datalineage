# Solving the Enterprise Data Mystery
### *The Story of How We Cracked Cross-System Data Lineage & Dependency Tracking*

![Enterprise Data Lineage Vision](assets/platform_vision_banner.png)

> **Quick Navigation:**  
> 📖 **You are reading:** *The Story Mode & Executive Architecture Journey* (Intuitive, clue-by-clue overview).  
> 🔬 **Need the full technical reference?** Check out the exhaustive 1,000-line specification in [detailed_overview.md](detailed_overview.md) (Complete JSON schemas, Python interfaces, risk matrices, and glossary).

---

## The Plot: Table of Contents
1. [Prologue: The Monday Morning Crime Scene](#1-prologue-the-monday-morning-crime-scene)
2. [Act I: The 20 Suspects in the Enterprise Fog](#2-act-i-the-20-suspects-in-the-enterprise-fog)
3. [Act II: Collecting the Three Clues (The Evidence Planes)](#3-act-ii-collecting-the-three-clues-the-evidence-planes)
4. [Act III: Solving the Hardest Riddles](#4-act-iii-solving-the-hardest-riddles)
   - [Riddle 1: The Doppelgänger Trap (Dataset Identity)](#riddle-1-the-doppelgänger-trap-dataset-identity)
   - [Riddle 2: The 5 Layers of Reality](#riddle-2-the-5-layers-of-reality)
   - [Riddle 3: The Ghost Dependency (Control vs. Data Flow)](#riddle-3-the-ghost-dependency-control-vs-data-flow)
5. [Act IV: Assembling the Deduction Engine (System Architecture)](#5-act-iv-assembling-the-deduction-engine-system-architecture)
6. [Act V: Simulating the Crime (Automated Blast Radius)](#6-act-v-simulating-the-crime-automated-blast-radius)
7. [Act VI: The 90-Day Blueprint & The Final Verdict](#7-act-vi-the-90-day-blueprint--the-final-verdict)

---

# 1. Prologue: The Monday Morning Crime Scene

It is **09:00 AM on Monday**. The CFO opens the executive dashboard to prepare for the board meeting, and the main revenue KPI widget displays a catastrophic error:

$$\text{\textbf{500: Database Column Not Found}}$$

Alarms trigger across Slack. Teams scramble:
- The **BI Team** says: *"Our dashboard is untouched; the Snowflake warehouse table `daily_risk_summary` stopped receiving updates!"*
- The **Data Engineering Team** says: *"Our dbt pipeline failed because upstream table `risk_enriched_orders` had null values!"*
- The **Platform Team** says: *"The Python job failed because the backend Java service changed on Friday afternoon!"*
- The **Backend Engineer** says: *"I only renamed a variable in one method in a Java repository! How could that break an executive board dashboard four systems away?!"*

```mermaid
flowchart LR
    DEV["Friday 4 PM:<br/>Java Dev renames field"] -->|Unknown dependency| S1["Postgres Table"]
    S1 -->|Silent breakage| S2["Python Job"]
    S2 -->|Missing column| S3["Snowflake dbt Model"]
    S3 -->|Pipeline crash| DASH["Monday 9 AM:<br/>CFO Dashboard Goes Dark 💥"]

    style DEV fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    style DASH fill:#ffebee,stroke:#c62828,stroke-width:2px;
```

### The Real Problem
**Nobody had the full map.**  
In modern enterprises, code lives in GitHub, jobs live in Airflow, schemas live in databases, transformations live in dbt, and dashboards live in Power BI. 

**The Mission:** Build **Google Maps for Enterprise Data**—an intelligent platform that traces any data asset from source code to dashboard, explains *why* each connection exists, and predicts **what will break before changes are merged.**

---

# 2. Act I: The 20 Suspects in the Enterprise Fog

To solve this mystery, we first interrogated the typical suspects in the company:

```text
       THE 20 DISCONNECTED SUSPECTS ACROSS THE ESTATE
 ┌──────────────────────────────────────────────────────────┐
 │ • Java Microservices      • Python Analytics Scripts     │
 │ • PostgreSQL Databases    • Oracle Stored Procedures     │
 │ • Snowflake Warehouse     • Apache Kafka Event Topics    │
 │ • AWS S3 Data Lakes       • Apache Airflow DAGs          │
 │ • dbt Transformation Models • Power BI Dashboards        │
 └──────────────────────────────────────────────────────────┘
```

Why couldn't our existing tools solve this?

1. **Detective Git (Source Code Parsers):** Knows what the code *might* do, but has no clue what runs in production. It cannot see runtime SQL generated dynamically or database triggers.
2. **Detective Catalog (DataHub / OpenMetadata):** Knows table and column names, but stops at the database door. It cannot inspect the logic inside a Java method or calculate math inside a complex SQL CTE.
3. **Detective LLM (AI Models):** Hallucinates! When asked to map relationships across a 500,000-line codebase, it invents plausible-looking connections that do not exist.

> [!IMPORTANT]
> **The Epiphany:**  
> A single tool or AI model cannot solve this puzzle. We must construct a **Unified Lineage Fabric** that gathers forensic clues from three independent planes and binds them with auditable physical evidence.

---

# 3. Act II: Collecting the Three Clues (The Evidence Planes)

To build undeniable proof for every relationship, our detective engine collects **three distinct categories of clues**:

![Data Lineage Evidence Model](assets/modern_evidence_model.png)

### Clue 1: The Static Breadcrumbs (Code & SQL AST)
*Found in Git repositories without executing anything.*
- **How we find it:** We deploy **Tree-sitter** to parse Java and Python into Abstract Syntax Trees (ASTs), and **SQLGlot** to parse SQL queries across dialects.
- **What it reveals:** Potential paths, method calls, and column transformation math (`salary * 0.10 AS bonus`).
- **Limitation:** Cannot guarantee whether an `if-else` branch actually ran in production.

### Clue 2: The Metadata Fingerprints (Schemas & Catalogs)
*Found in database information schemas, data catalogs, and orchestrator manifests.*
- **How we find it:** Read-only connectors query catalog APIs and database dictionaries (`information_schema.columns`, dbt `manifest.json`).
- **What it reveals:** Official table definitions, declared data types, and job ownership.
- **Limitation:** Lacks internal code transformations and stops at vendor boundaries.

### Clue 3: The Runtime Eyewitness (Telemetry & Logs)
*Found in live production execution telemetry.*
- **How we find it:** Ingesting **OpenLineage** runtime event payloads, database query audit logs, and distributed traces.
- **What it reveals:** Proves what datasets were *actually read and written* during an execution run at a specific timestamp.
- **Limitation:** Only covers observed runs during the retention window.

```mermaid
flowchart TD
    C1["<b>Clue 1: Static AST</b><br/>(What code COULD do)"] --> ENGINE["<b>Normalization Engine</b><br/>(Combines & Calibrates Clues)"]
    C2["<b>Clue 2: Metadata Schema</b><br/>(What catalogs DECLARE)"] --> ENGINE
    C3["<b>Clue 3: Runtime Events</b><br/>(What ACTUALLY ran)"] --> ENGINE

    ENGINE --> VERDICT["<b>Evidence-Backed Edge</b><br/>• Source: RiskService.java:112<br/>• Verified: OpenLineage Run #8912<br/>• Confidence: Confirmed 🟢"]

    classDef c fill:#e3f2fd,stroke:#1565c0,stroke-width:1.5px;
    classDef e fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    classDef v fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;

    class C1,C2,C3 c;
    class ENGINE e;
    class VERDICT v;
```

---

# 4. Act III: Solving the Hardest Riddles

### Riddle 1: The Doppelgänger Trap (Dataset Identity)
**The Puzzle:** In a global enterprise, we found five different tables named `CUSTOMERS`. Are they the same object?
- `DEV.CUSTOMERS`
- `STAGING.CUSTOMERS`
- `PROD.CUSTOMERS` (Snowflake US)
- `PROD.CUSTOMERS` (Postgres EU)

If we merge them blindly, our lineage graph becomes a tangled web of lies.

**The Solution: The 6-Tuple Canonical URI**  
We assign every dataset a deterministic global fingerprint:
$$\text{dataset://}\{\text{platform}\}/\{\text{tenant}\}/\{\text{environment}\}/\{\text{cluster}\}/\{\text{database}\}/\{\text{schema}\}/\{\text{table}\}$$

*Result: No more mistaken identities. Development tables never accidentally link to production dashboards.*

---

### Riddle 2: The 5 Layers of Reality
**The Puzzle:** Different users need different levels of detail. The CFO wants to see business domains; the engineer wants to see the exact line of code.

**The Solution:** An unbroken 5-layer zoom hierarchy:

```mermaid
graph TD
    L1["<b>Level 1: Enterprise Domain</b> (e.g., Wealth Management)"]
    L2["<b>Level 2: Application / Service</b> (e.g., RiskService.java)"]
    L3["<b>Level 3: Dataset / Table</b> (e.g., Snowflake: customer_risk)"]
    L4["<b>Level 4: Column / Field</b> (e.g., risk_score = amount * credit_factor)"]
    L5["<b>Level 5: Code & Runtime</b> (e.g., Method: calculateRisk() • Run #4812)"]

    L1 --> L2 --> L3 --> L4 --> L5

    style L1 fill:#e8eaf6,stroke:#3f51b5;
    style L2 fill:#e3f2fd,stroke:#2196f3;
    style L3 fill:#e0f2f1,stroke:#009688;
    style L4 fill:#fffde7,stroke:#fbc02d;
    style L5 fill:#ffebee,stroke:#f44336;
```

---

### Riddle 3: The Ghost Dependency (Control vs. Data Flow)
**The Puzzle:** In Airflow, Task B runs after Task A (`Task A >> Task B`). Does data flow between them?  
**The Revelation:** Not necessarily! Task A might just be clearing a temporary lock or sending a Slack alert.  
**The Rule:** We strictly separate `DEPENDS_ON` (execution order) from `TRANSFORMS_TO` (data lineage). We never draw a data arrow unless bytes physically moved.

---

# 5. Act IV: Assembling the Deduction Engine (System Architecture)

With our clues and rules in place, we built the machine that continuously stitches the enterprise together:

![Modern System Architecture](assets/modern_architecture.png)

### The 4 Engine Components:
1. **The Ingestion Net:** Connectors safely read Git commits, database schemas, and OpenLineage events using least-privilege access.
2. **The Compiler Core:** Tree-sitter and SQLGlot extract ASTs, trace CTE expressions, and unwrap dynamic aliases into normalized entities.
3. **The Brain (Neo4j + PostgreSQL):**
   - **Neo4j:** Traverses millions of interconnected dependencies across 8+ hops in milliseconds using Cypher.
   - **PostgreSQL:** Stores tenant settings, audit logs, run histories, and forensic proof cards.
4. **The Detective's UI (React Flow):** An interactive visual canvas that lets users click any node, expand upstream origins, inspect code evidence, and simulate downstream changes.

---

# 6. Act V: Simulating the Crime (Automated Blast Radius)

Remember our Monday morning crisis?  
With our deduction engine running, that crisis **can never happen again.**

Before any engineer merges code, they run an automated **Impact Analysis Simulation**:

```mermaid
flowchart TD
    PROP["Engineer proposes PR:<br/><b>ALTER TABLE customers DROP COLUMN credit_score</b>"]
    
    PROP --> ENGINE["🔍 Impact Analysis Engine<br/>(Forward Cypher Traversal)"]
    
    ENGINE --> A1["⚠️ Alert 1: RiskService.java:114<br/>(SQL Query Compilation Error)"]
    ENGINE --> A2["⚠️ Alert 2: batch_risk_enricher.py<br/>(Missing Input Attribute)"]
    ENGINE --> A3["💥 Alert 3: dbt daily_risk_summary<br/>(Downstream Model Failure)"]
    ENGINE --> A4["🚨 Alert 4: Executive Revenue Dashboard<br/>(Tile 'Average Risk Score' Breaks!)"]

    classDef pr fill:#fff8e1,stroke:#f57f17,stroke-width:2px;
    classDef eng fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
    classDef al fill:#ffebee,stroke:#c62828,stroke-width:2px;

    class PROP pr;
    class ENGINE eng;
    class A1,A2,A3,A4 al;
```

### What the Engineer Sees in GitHub Pull Request:
```text
⚠️ BLAST RADIUS DETECTED: 4 Downstream Assets Affected!
-------------------------------------------------------------------------
• 1 Direct Breaking Change: RiskService.java (Class: RiskCalculator)
• 1 Broken Pipeline: batch_risk_enricher.py (Owner: Data Eng Team)
• 1 Failed Model: Snowflake dbt daily_risk_summary (Owner: Finance Analytics)
• 1 Impacted Dashboard: Executive Revenue Dashboard (Owner: VP Risk)
-------------------------------------------------------------------------
PR Blocked by Lineage Governance Gate until migration plan is provided.
```

The crime is prevented before it reaches production.

---

# 7. Act VI: The 90-Day Blueprint & The Final Verdict

How do we take this from concept to working enterprise reality? We don't try to build the universe on day one. We follow a strict **90-Day Focused Pilot**:

```mermaid
flowchart LR
    S1["<b>Days 1–15</b><br/>Phase 0<br/>• Ground truth set<br/>• Canonical spec<br/>• Security check"] --> S2["<b>Days 16–35</b><br/>Phase 1<br/>• Tree-sitter & SQLGlot<br/>• Table read/write<br/>• Neo4j store"]
    S2 --> S3["<b>Days 36–55</b><br/>Phase 2<br/>• Identity resolver<br/>• Column AST trace<br/>• DB metadata connector"]
    S3 --> S4["<b>Days 56–75</b><br/>Phase 3 & 4<br/>• React Flow UI<br/>• Impact analysis API<br/>• Incremental diffs"]
    S4 --> S5["<b>Days 76–90</b><br/>Phase 5 Pilot Demo<br/>• Full traversal<br/>• Ground truth eval<br/>• Production ready!"]

    style S1 fill:#e8eaf6,stroke:#3f51b5;
    style S2 fill:#e3f2fd,stroke:#2196f3;
    style S3 fill:#e0f2f1,stroke:#009688;
    style S4 fill:#fff9c4,stroke:#fbc02d;
    style S5 fill:#e8f5e9,stroke:#4caf50;
```

### What We Reuse vs. What We Build

| What We Reuse (Commodity Infrastructure) | What We Build (Our Proprietary Value) |
| :--- | :--- |
| • Syntax Parsing: **Tree-sitter** | • **Cross-System Evidence Engine** (3 Planes) |
| • SQL Dialects: **SQLGlot** | • **Canonical Identity Resolver** (6-Tuple URI) |
| • Event Schemas: **OpenLineage** | • **Code-to-Column Expression Tracer** |
| • Graph Engine: **Neo4j** | • **Blast-Radius Impact Analysis Engine** |
| • Relational Store: **PostgreSQL** | • **Incremental Git Reconciliation Engine** |
| • UI Canvas: **React Flow** | • **Evidence-Backed Trust & Coverage Dashboard** |

---

## The Final Verdict

Data lineage is not just a diagram tool, and it is not a data catalog clone.  
**It is an auditable evidence fabric that answers the fundamental question of enterprise data:**

> *"Where did this data originate, how was it transformed, who is using it right now, and what will break if I change it today?"*

---

### Interactive Web App & GitHub Repository

- 🌐 **Interactive Web Experience:** Open [`docs/index.html`](docs/index.html) to run the live Node & Edge Lineage Graph, interactive clue collection, and blast radius simulation.
- 📦 **GitHub Repository:** [https://github.com/TestAccount-04/datalineage.git](https://github.com/TestAccount-04/datalineage.git)
- 🚀 **Clone & Run:**
  ```bash
  git clone https://github.com/TestAccount-04/datalineage.git
  cd datalineage
  # Open docs/index.html in any browser, or enable GitHub Pages from /docs in repo settings!
  ```

---

### Ready for the Complete Technical Deep-Dive?
This story gives you the complete conceptual journey. To inspect the **full 1,000-line engineering blueprint**, including:
- Complete JSON schemas for evidence records and impact analysis
- Formal Pydantic Python interface for `BaseConnector`
- Detailed trade-off matrices for 20+ technologies
- 12 risk mitigations, FinOps token budgets, and 17 glossary terms

👉 **Read the full engineering blueprint in [detailed_overview.md](detailed_overview.md)**
