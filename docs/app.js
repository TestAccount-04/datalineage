/* ==========================================================================
   LineageForge Interactive Engine (app.js)
   State Management, Deterministic SVG Node-Edge Lineage Graph, 
   Clue Collection, Blast Radius Simulator, and Final Output Lineage DAG
   ========================================================================== */

// Canonical Lineage Graph Data Definition (Deterministic Coordinate System)
const graphData = {
  stages: [
    { name: "1. Source Tables", subtitle: "PostgreSQL OLTP" },
    { name: "2. Service Code", subtitle: "Java Spring Boot" },
    { name: "3. ETL Pipeline", subtitle: "Airflow / PySpark" },
    { name: "4. Data Warehouse", subtitle: "Snowflake / dbt" },
    { name: "5. Final Lineage", subtitle: "Power BI KPI" }
  ],
  nodes: [
    {
      id: "node-orders",
      title: "orders",
      tech: "PostgreSQL OLTP",
      type: "table",
      badge: "SOURCE",
      badgeColor: "#0284c7",
      badgeBg: "#e0f2fe",
      icon: "🗄️",
      x: 35, y: 70, width: 175, height: 95,
      owner: "Checkout & Sales Team",
      platform: "Postgres 16",
      location: "tables/public.orders",
      columns: ["id (PK)", "customer_id (FK)", "amount (DECIMAL)", "created_at"],
      evidence: "Static schema scan + JDBC read query in RiskService.java:112",
      confidence: "98% (Strong) 🟢",
      upstream: "Root Data Source (No Upstream)",
      downstream: "RiskService.java (calculateScore)"
    },
    {
      id: "node-customers",
      title: "customers",
      tech: "PostgreSQL OLTP",
      type: "table",
      badge: "SOURCE",
      badgeColor: "#0284c7",
      badgeBg: "#e0f2fe",
      icon: "🗄️",
      x: 35, y: 240, width: 175, height: 95,
      owner: "User Identity Team",
      platform: "Postgres 16",
      location: "tables/public.customers",
      columns: ["id (PK)", "credit_score (INT)", "email (VARCHAR)", "tier"],
      evidence: "Database Information Schema + SQLGlot SELECT ast",
      confidence: "99% (Confirmed) 🟢",
      upstream: "Root Data Source (No Upstream)",
      downstream: "RiskService.java (calculateScore)"
    },
    {
      id: "node-risk-service",
      title: "RiskService.java",
      tech: "Java Microservice",
      type: "code",
      badge: "CODE",
      badgeColor: "#7e22ce",
      badgeBg: "#f3e8ff",
      icon: "💻",
      x: 275, y: 155, width: 185, height: 95,
      owner: "Risk Platform Squad",
      platform: "Spring Boot 3.2",
      location: "src/main/java/com/corp/risk/RiskService.java:114",
      columns: ["calculateScore()", "creditFactor", "riskMultiplier"],
      evidence: "Tree-sitter AST symbol table + git commit sha_8f4bc92a",
      confidence: "95% (Strong) 🟢",
      upstream: "orders (PostgreSQL), customers (PostgreSQL)",
      downstream: "batch_risk_enricher.py (Pipeline)"
    },
    {
      id: "node-enricher-job",
      title: "batch_risk_enricher.py",
      tech: "Python Pipeline",
      type: "job",
      badge: "JOB",
      badgeColor: "#047857",
      badgeBg: "#ecfdf5",
      icon: "🔄",
      x: 520, y: 155, width: 195, height: 95,
      owner: "Data Engineering",
      platform: "Airflow / PySpark",
      location: "dags/risk_enrichment_dag.py (Task: enrich_scores)",
      columns: ["risk_enriched_orders", "batch_timestamp", "partition_date"],
      evidence: "OpenLineage RunFacet event ID #8912 (Active runtime proof)",
      confidence: "99% (Confirmed) 🟢",
      upstream: "RiskService.java (Microservice)",
      downstream: "daily_risk_summary (Snowflake)"
    },
    {
      id: "node-dbt-summary",
      title: "daily_risk_summary",
      tech: "Snowflake Warehouse",
      type: "table",
      badge: "DWH",
      badgeColor: "#4338ca",
      badgeBg: "#e0e7ff",
      icon: "❄️",
      x: 775, y: 155, width: 190, height: 95,
      owner: "Finance Analytics Team",
      platform: "Snowflake + dbt Core",
      location: "models/marts/finance/daily_risk_summary.sql",
      columns: ["date", "risk_category", "risk_score = SUM(amount) * credit_multiplier"],
      evidence: "dbt manifest.json dependency graph + SQLGlot CTE compiler",
      confidence: "98% (Strong) 🟢",
      upstream: "batch_risk_enricher.py (Pipeline)",
      downstream: "Executive Revenue KPI (Power BI Dashboard)"
    },
    {
      id: "node-bi-dashboard",
      title: "Executive Revenue KPI",
      tech: "Power BI Dashboard",
      type: "bi",
      badge: "FINAL CONSUMER",
      badgeColor: "#b45309",
      badgeBg: "#fef3c7",
      icon: "📊",
      x: 1020, y: 155, width: 195, height: 95,
      owner: "VP Finance & Risk Committee",
      platform: "Power BI Service",
      location: "workspaces/Executive_Reports/tiles/daily_risk_kpi",
      columns: ["Visual: Daily Revenue at Risk", "Metric: Aggregate Loss Expectancy"],
      evidence: "Power BI Workspace REST API dataset binding metadata",
      confidence: "96% (Strong) 🟢",
      upstream: "daily_risk_summary (Snowflake Warehouse)",
      downstream: "End Lineage Destination (No Downstream)"
    }
  ],
  edges: [
    { id: "edge-1", from: "node-orders", to: "node-risk-service", label: "READS_FROM" },
    { id: "edge-2", from: "node-customers", to: "node-risk-service", label: "READS_FROM" },
    { id: "edge-3", from: "node-risk-service", to: "node-enricher-job", label: "TRANSFORMS" },
    { id: "edge-4", from: "node-enricher-job", to: "node-dbt-summary", label: "WRITES_TO" },
    { id: "edge-5", from: "node-dbt-summary", to: "node-bi-dashboard", label: "SERVES" }
  ]
};

// Application State
const state = {
  activeMode: 'story', // 'story' | 'detailed'
  selectedNode: 'node-risk-service',
  activeFilter: 'all',
  isTracing: false,
  isBrokenMode: false,
  isCllMode: false,
  cluesCollected: {
    static: false,
    metadata: false,
    runtime: false
  },
  scenarios: {
    drop_credit_score: {
      action: "ALTER TABLE customers DROP COLUMN credit_score",
      severity: "CRITICAL BREAKING",
      nodes: [
        { name: "RiskService.java (calculateScore)", type: "Code Method", effect: "Compilation error: missing getCreditScore() field" },
        { name: "batch_risk_enricher.py", type: "Python Pipeline", effect: "Runtime KeyError on 'credit_score' attribute in PySpark batch" },
        { name: "Snowflake: daily_risk_summary", type: "dbt Mart", effect: "SQLGlot CTE compiler failure: column does not exist" },
        { name: "Executive Revenue Dashboard", type: "Power BI Tile", effect: "Crash: HTTP 500 Column Not Found on 'Risk Category KPI'" }
      ],
      gateStatus: "🚨 PULL REQUEST BLOCKED. Manual migration or backward-compatible view required before merge."
    },
    rename_amount: {
      action: "ALTER TABLE orders RENAME COLUMN amount TO total_amount",
      severity: "HIGH BREAKING",
      nodes: [
        { name: "OrderRepository.java", type: "ORM Mapping", effect: "JPA Column mapping failure: property amount not found" },
        { name: "order_stream.kafka", type: "Event Stream", effect: "Schema Registry Avro payload incompatible" },
        { name: "dbt: daily_revenue_mart", type: "Data Warehouse", effect: "Downstream revenue metrics output NULL" }
      ],
      gateStatus: "⚠️ PULL REQUEST BLOCKED. Schema contract violation detected."
    },
    add_audit_col: {
      action: "ALTER TABLE customers ADD COLUMN audit_timestamp TIMESTAMP",
      severity: "SAFE (ADDITIVE)",
      nodes: [
        { name: "customers (Postgres)", type: "Source Table", effect: "Added non-blocking column with NULL default" },
        { name: "dbt: stg_customers", type: "dbt Stage", effect: "Schema evolution test passed successfully" }
      ],
      gateStatus: "🟢 SAFE CHANGE APPROVED. No breaking downstream impacts detected."
    }
  }
};

// Initialize Application on DOM Ready
document.addEventListener('DOMContentLoaded', () => {
  setupModeSwitching();
  renderSvgLineageGraph();
  setupGraphControls();
  setupClueCollection();
  setupIdentityResolver();
  setupBlastRadiusSimulator();
  setupSidebarNavigation();
  setupAct5LineageActions();
  initializeMermaid();
});

// Setup Mermaid.js with Plain White / Light SaaS Theme
function initializeMermaid() {
  if (window.mermaid) {
    window.mermaid.initialize({
      startOnLoad: false,
      theme: 'default',
      securityLevel: 'loose',
      themeVariables: {
        fontFamily: 'Inter, sans-serif',
        primaryColor: '#ffffff',
        primaryTextColor: '#0f172a',
        primaryBorderColor: '#2563eb',
        lineColor: '#64748b',
        secondaryColor: '#f8fafc',
        tertiaryColor: '#ffffff',
        background: '#ffffff',
        mainBkg: '#ffffff',
        nodeBorder: '#cbd5e1'
      }
    });
  }
}
window.initializeMermaid = initializeMermaid;

// Explicitly render Mermaid diagrams when Detailed Mode is visible
async function renderMermaidDiagrams() {
  if (!window.mermaid) {
    setTimeout(renderMermaidDiagrams, 150);
    return;
  }
  
  const elements = document.querySelectorAll('#view-detailed .mermaid');
  for (const el of elements) {
    if (!el.dataset.code) {
      el.dataset.code = el.textContent.trim();
    }
    
    if (!el.dataset.rendered) {
      const code = el.dataset.code;
      const id = 'mermaid-svg-' + Math.random().toString(36).substring(2, 9);
      try {
        const { svg } = await window.mermaid.render(id, code);
        el.innerHTML = svg;
        el.dataset.rendered = 'true';
      } catch (err) {
        console.error('Mermaid render error:', err);
        el.innerHTML = '<div style="color:#dc2626; padding:1rem; font-family:var(--font-mono); font-size:0.85rem;">Diagram render error: ' + err.message + '</div>';
      }
    }
  }
}

// Mode Switching Logic
function setupModeSwitching() {
  const btnStory = document.getElementById('btn-mode-story');
  const btnDetailed = document.getElementById('btn-mode-detailed');
  const viewStory = document.getElementById('view-story');
  const viewDetailed = document.getElementById('view-detailed');
  const statusBar = document.getElementById('case-status-bar');

  if (btnStory && btnDetailed) {
    btnStory.addEventListener('click', () => {
      state.activeMode = 'story';
      btnStory.classList.add('active');
      btnDetailed.classList.remove('active');
      viewStory.classList.add('active');
      viewDetailed.classList.remove('active');
      if (statusBar) statusBar.style.display = 'flex';
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    btnDetailed.addEventListener('click', () => {
      state.activeMode = 'detailed';
      btnDetailed.classList.add('active');
      btnStory.classList.remove('active');
      viewDetailed.classList.add('active');
      viewStory.classList.remove('active');
      if (statusBar) statusBar.style.display = 'none';
      window.scrollTo({ top: 0, behavior: 'smooth' });
      window.dispatchEvent(new Event('scroll'));
    });
  }
}

// ==========================================================================
// DETERMINISTIC SVG LINEAGE GRAPH ENGINE (Nodes & Edges)
// ==========================================================================

function renderSvgLineageGraph() {
  const svg = document.getElementById('lineage-svg-canvas');
  if (!svg) return;

  svg.setAttribute('viewBox', '0 0 1260 410');
  svg.innerHTML = '';

  // 1. Defs: Arrowhead Markers & Filters
  const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
  defs.innerHTML = `
    <marker id="arrow-default" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#94a3b8" />
    </marker>
    <marker id="arrow-active" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#2563eb" />
    </marker>
    <marker id="arrow-flowing" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#059669" />
    </marker>
    <marker id="arrow-broken" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 1 L 10 5 L 0 9 z" fill="#dc2626" />
    </marker>
  `;
  svg.appendChild(defs);

  // 2. Render Edges (Smooth Cubic Bezier Curves)
  const edgesGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  edgesGroup.setAttribute('id', 'svg-edges-layer');

  graphData.edges.forEach(edge => {
    const fromNode = graphData.nodes.find(n => n.id === edge.from);
    const toNode = graphData.nodes.find(n => n.id === edge.to);
    if (!fromNode || !toNode) return;

    const startX = fromNode.x + fromNode.width;
    const startY = fromNode.y + fromNode.height / 2;
    const endX = toNode.x;
    const endY = toNode.y + toNode.height / 2;

    const dx = Math.abs(endX - startX) * 0.55;
    const pathD = `M ${startX} ${startY} C ${startX + dx} ${startY}, ${endX - dx} ${endY}, ${endX} ${endY}`;

    // Edge Path
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', pathD);
    path.setAttribute('class', 'svg-edge');
    path.setAttribute('id', edge.id);
    path.setAttribute('marker-end', 'url(#arrow-default)');
    path.dataset.from = edge.from;
    path.dataset.to = edge.to;
    edgesGroup.appendChild(path);

    // Flowing Animation Particle (Hidden by default, shown during Trace Flow)
    const particle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    particle.setAttribute('r', '4');
    particle.setAttribute('fill', '#059669');
    particle.setAttribute('class', 'svg-flow-particle');
    particle.style.display = 'none';
    particle.innerHTML = `<animateMotion path="${pathD}" dur="2.4s" repeatCount="indefinite" />`;
    edgesGroup.appendChild(particle);

    // Edge Label Pill (Centered)
    const midX = (startX + endX) / 2;
    const midY = (startY + endY) / 2 - 12;

    const labelBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    labelBg.setAttribute('x', midX - 42);
    labelBg.setAttribute('y', midY - 9);
    labelBg.setAttribute('width', 84);
    labelBg.setAttribute('height', 18);
    labelBg.setAttribute('class', 'svg-edge-label-bg');
    edgesGroup.appendChild(labelBg);

    const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    labelText.setAttribute('x', midX);
    labelText.setAttribute('y', midY);
    labelText.setAttribute('class', 'svg-edge-label-text');
    labelText.textContent = edge.label;
    edgesGroup.appendChild(labelText);
  });
  svg.appendChild(edgesGroup);

  // 3. Render Nodes (Clean SaaS Card Rectangles with Rich Text)
  const nodesGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  nodesGroup.setAttribute('id', 'svg-nodes-layer');

  graphData.nodes.forEach(node => {
    const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    group.setAttribute('class', 'svg-node-group' + (node.id === state.selectedNode ? ' selected' : ''));
    group.setAttribute('id', node.id);
    group.dataset.nodeId = node.id;
    group.dataset.type = node.type;

    // Card Background
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', node.x);
    rect.setAttribute('y', node.y);
    rect.setAttribute('width', node.width);
    rect.setAttribute('height', node.height);
    rect.setAttribute('class', 'svg-node-card');
    group.appendChild(rect);

    // Accent Top Border Strip
    const topStrip = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    topStrip.setAttribute('x', node.x + 10);
    topStrip.setAttribute('y', node.y);
    topStrip.setAttribute('width', node.width - 20);
    topStrip.setAttribute('height', 3);
    topStrip.setAttribute('fill', node.badgeColor);
    topStrip.setAttribute('rx', 1.5);
    group.appendChild(topStrip);

    // Title & Icon
    const titleText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    titleText.setAttribute('x', node.x + 14);
    titleText.setAttribute('y', node.y + 26);
    titleText.setAttribute('font-family', 'Outfit, sans-serif');
    titleText.setAttribute('font-size', '13.5px');
    titleText.setAttribute('font-weight', '700');
    titleText.setAttribute('fill', '#0f172a');
    titleText.textContent = `${node.icon} ${node.title}`;
    group.appendChild(titleText);

    // Subtitle / Technology
    const subText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    subText.setAttribute('x', node.x + 14);
    subText.setAttribute('y', node.y + 46);
    subText.setAttribute('font-family', 'Inter, sans-serif');
    subText.setAttribute('font-size', '11px');
    subText.setAttribute('fill', '#64748b');
    subText.textContent = node.tech;
    group.appendChild(subText);

    // Badge Pill Background
    const badgeBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    badgeBg.setAttribute('x', node.x + 14);
    badgeBg.setAttribute('y', node.y + 60);
    badgeBg.setAttribute('width', node.badge.length * 6.5 + 14);
    badgeBg.setAttribute('height', 17);
    badgeBg.setAttribute('rx', 8.5);
    badgeBg.setAttribute('fill', node.badgeBg);
    badgeBg.setAttribute('stroke', node.badgeColor);
    badgeBg.setAttribute('stroke-width', '0.75');
    group.appendChild(badgeBg);

    // Badge Pill Text
    const badgeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    badgeText.setAttribute('x', node.x + 21);
    badgeText.setAttribute('y', node.y + 72);
    badgeText.setAttribute('font-family', 'Inter, sans-serif');
    badgeText.setAttribute('font-size', '8.5px');
    badgeText.setAttribute('font-weight', '700');
    badgeText.setAttribute('fill', node.badgeColor);
    badgeText.textContent = node.badge;
    group.appendChild(badgeText);

    // Click Event Listener
    group.addEventListener('click', () => {
      selectGraphNode(node.id);
    });

    nodesGroup.appendChild(group);
  });
  svg.appendChild(nodesGroup);

  // Initialize inspector with current selected node
  selectGraphNode(state.selectedNode);
}

// Select a node and populate inspector drawer
function selectGraphNode(nodeId) {
  state.selectedNode = nodeId;
  const node = graphData.nodes.find(n => n.id === nodeId);
  if (!node) return;

  // Highlight selected SVG node
  document.querySelectorAll('.svg-node-group').forEach(g => {
    g.classList.toggle('selected', g.id === nodeId);
  });

  // Highlight connected SVG edges
  document.querySelectorAll('.svg-edge').forEach(edge => {
    const isConnected = edge.dataset.from === nodeId || edge.dataset.to === nodeId;
    edge.classList.toggle('active', isConnected);
    edge.setAttribute('marker-end', isConnected ? 'url(#arrow-active)' : 'url(#arrow-default)');
  });

  // Update Inspector Drawer
  const inspector = document.getElementById('node-inspector');
  if (inspector) {
    document.getElementById('insp-name').textContent = `${node.icon} ${node.title}`;
    document.getElementById('insp-type').textContent = `${node.tech} • Platform: ${node.platform}`;
    document.getElementById('insp-owner').textContent = node.owner;
    document.getElementById('insp-location').textContent = node.location;
    document.getElementById('insp-confidence').textContent = node.confidence;
    document.getElementById('insp-evidence').textContent = node.evidence;

    const upstreamEl = document.getElementById('insp-upstream');
    if (upstreamEl) upstreamEl.textContent = node.upstream;

    const downstreamEl = document.getElementById('insp-downstream');
    if (downstreamEl) downstreamEl.textContent = node.downstream;

    const colsList = document.getElementById('insp-columns');
    if (colsList) {
      colsList.innerHTML = '';
      node.columns.forEach(col => {
        const li = document.createElement('li');
        li.textContent = col;
        colsList.appendChild(li);
      });
    }

    inspector.classList.add('active');
  }
}

// Setup Interactive Graph Toolbar Controls
function setupGraphControls() {
  const btnTrace = document.getElementById('btn-trace-flow');
  const btnCll = document.getElementById('btn-show-cll');
  const btnIncident = document.getElementById('btn-simulate-break');
  const btnReset = document.getElementById('btn-reset-graph');
  const filterButtons = document.querySelectorAll('.btn-filter-tier');

  // 1. Trace Data Flow Animation
  if (btnTrace) {
    btnTrace.addEventListener('click', () => {
      if (state.isTracing) return;
      state.isTracing = true;
      btnTrace.classList.add('active');
      btnTrace.textContent = '⏳ Flow In Progress...';

      // Show animated flowing edges
      document.querySelectorAll('.svg-edge').forEach(e => {
        e.classList.add('flowing');
        e.setAttribute('marker-end', 'url(#arrow-flowing)');
      });
      document.querySelectorAll('.svg-flow-particle').forEach(p => p.style.display = 'block');

      // Sequentially pulse nodes from left to right
      const sequence = ["node-orders", "node-customers", "node-risk-service", "node-enricher-job", "node-dbt-summary", "node-bi-dashboard"];
      sequence.forEach((id, idx) => {
        setTimeout(() => {
          const el = document.getElementById(id);
          if (el) {
            el.classList.add('flow-active');
            setTimeout(() => el.classList.remove('flow-active'), 700);
          }
        }, idx * 450);
      });

      // Completion
      setTimeout(() => {
        document.querySelectorAll('.svg-edge').forEach(e => {
          e.classList.remove('flowing');
          e.setAttribute('marker-end', 'url(#arrow-default)');
        });
        document.querySelectorAll('.svg-flow-particle').forEach(p => p.style.display = 'none');
        btnTrace.textContent = '▶ Trace Data Flow Animation';
        btnTrace.classList.remove('active');
        state.isTracing = false;
        selectGraphNode(state.selectedNode);
      }, sequence.length * 450 + 600);
    });
  }

  // 2. Inspect Column Lineage (CLL) Mode
  if (btnCll) {
    btnCll.addEventListener('click', () => {
      state.isCllMode = !state.isCllMode;
      btnCll.classList.toggle('active', state.isCllMode);
      
      const inspector = document.getElementById('node-inspector');
      if (state.isCllMode) {
        selectGraphNode('node-dbt-summary');
        if (inspector) {
          inspector.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
      }
    });
  }

  // 3. Simulate Friday Breakage
  if (btnIncident) {
    btnIncident.addEventListener('click', () => {
      state.isBrokenMode = !state.isBrokenMode;
      btnIncident.classList.toggle('active', state.isBrokenMode);

      const edge2 = document.getElementById('edge-2'); // Edge from customers to RiskService
      const custNode = document.getElementById('node-customers');
      const serviceNode = document.getElementById('node-risk-service');

      if (state.isBrokenMode) {
        if (edge2) {
          edge2.classList.add('broken');
          edge2.setAttribute('marker-end', 'url(#arrow-broken)');
        }
        if (custNode) custNode.classList.add('broken-active');
        if (serviceNode) serviceNode.classList.add('broken-active');
        btnIncident.textContent = '🚨 Outage Simulated (Click to Heal)';
        selectGraphNode('node-risk-service');
      } else {
        if (edge2) {
          edge2.classList.remove('broken');
          edge2.setAttribute('marker-end', 'url(#arrow-default)');
        }
        if (custNode) custNode.classList.remove('broken-active');
        if (serviceNode) serviceNode.classList.remove('broken-active');
        btnIncident.textContent = '💥 Simulate Friday Outage';
        selectGraphNode('node-risk-service');
      }
    });
  }

  // 4. Reset Graph
  if (btnReset) {
    btnReset.addEventListener('click', () => {
      state.isBrokenMode = false;
      state.isCllMode = false;
      if (btnIncident) btnIncident.textContent = '💥 Simulate Friday Outage';
      document.querySelectorAll('.btn-graph-action').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.btn-filter-tier').forEach(b => b.classList.remove('active'));
      const allFilter = document.querySelector('.btn-filter-tier[data-tier="all"]');
      if (allFilter) allFilter.classList.add('active');

      document.querySelectorAll('.svg-node-group').forEach(g => {
        g.style.opacity = '1';
        g.classList.remove('flow-active', 'broken-active');
      });
      document.querySelectorAll('.svg-edge').forEach(e => {
        e.classList.remove('flowing', 'broken');
        e.setAttribute('marker-end', 'url(#arrow-default)');
      });
      selectGraphNode('node-risk-service');
    });
  }

  // 5. Tier Filter Buttons
  filterButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      filterButtons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const tier = btn.dataset.tier;

      document.querySelectorAll('.svg-node-group').forEach(group => {
        const type = group.dataset.type;
        if (tier === 'all' || type === tier) {
          group.style.opacity = '1';
          group.style.pointerEvents = 'auto';
        } else {
          group.style.opacity = '0.2';
          group.style.pointerEvents = 'none';
        }
      });
    });
  });
}

// Clue Collection Interaction
function setupClueCollection() {
  const clueButtons = document.querySelectorAll('.btn-collect');
  const progressFill = document.getElementById('clue-progress-fill');
  const progressText = document.getElementById('clue-count-text');
  const deductionUnlock = document.getElementById('deduction-unlock-card');
  const dossierUnlock = document.getElementById('dossier-unlock-card');

  clueButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const clueType = btn.dataset.clue;
      if (!state.cluesCollected[clueType]) {
        state.cluesCollected[clueType] = true;
        
        // Update Card UI
        const card = btn.closest('.clue-card');
        if (card) card.classList.add('collected');
        btn.textContent = '✓ Clue Verified & Logged';
        btn.disabled = true;

        // Calculate Progress
        updateClueProgress(progressFill, progressText, deductionUnlock, dossierUnlock);
      }
    });
  });
}

function updateClueProgress(progressFill, progressText, deductionUnlock, dossierUnlock) {
  const collectedCount = Object.values(state.cluesCollected).filter(Boolean).length;
  const percentage = Math.round((collectedCount / 3) * 100);

  if (progressFill) progressFill.style.width = `${percentage}%`;
  if (progressText) progressText.textContent = `${collectedCount}/3 Clues Collected (${percentage}%)`;

  if (collectedCount === 3) {
    if (deductionUnlock) {
      deductionUnlock.style.display = 'block';
      deductionUnlock.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
    if (dossierUnlock) {
      dossierUnlock.style.display = 'block';
    }
  }
}

// Identity Resolver Interactive
function setupIdentityResolver() {
  const btnResolve = document.getElementById('btn-resolve-identities');
  const tableList = document.getElementById('identity-table-list');
  const resolvedList = document.getElementById('identity-resolved-list');

  if (btnResolve && tableList && resolvedList) {
    btnResolve.addEventListener('click', () => {
      tableList.style.display = 'none';
      resolvedList.style.display = 'grid';
      btnResolve.textContent = '✓ 4 Distinct Canonical URIs Resolved (0 Collisions)';
      btnResolve.style.background = '#059669';
      btnResolve.style.color = '#fff';
      btnResolve.style.borderColor = '#059669';
    });
  }
}

// Blast Radius Simulator
function setupBlastRadiusSimulator() {
  const simSelect = document.getElementById('sim-scenario-select');
  const btnSimulate = document.getElementById('btn-run-simulation');
  const simOutput = document.getElementById('sim-output-container');
  const actionText = document.getElementById('sim-action-text');
  const severityBadge = document.getElementById('sim-severity-badge');
  const gateBanner = document.getElementById('sim-gate-banner');
  const nodesGrid = document.getElementById('sim-nodes-grid');

  if (btnSimulate && simSelect && simOutput) {
    btnSimulate.addEventListener('click', () => {
      const selectedKey = simSelect.value;
      const scenario = state.scenarios[selectedKey];

      if (scenario) {
        actionText.textContent = scenario.action;
        severityBadge.textContent = scenario.severity;
        gateBanner.textContent = scenario.gateStatus;

        nodesGrid.innerHTML = '';
        scenario.nodes.forEach((node, index) => {
          const nodeEl = document.createElement('div');
          nodeEl.className = 'blast-node impacted';
          nodeEl.innerHTML = `
            <div class="blast-node-title">${index + 1}. ${node.name}</div>
            <div style="font-size:0.75rem; color:#64748b; margin-bottom:0.4rem;">Type: ${node.type}</div>
            <div class="blast-node-detail">${node.effect}</div>
          `;
          nodesGrid.appendChild(nodeEl);
        });

        simOutput.classList.add('active');
        simOutput.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  }
}

// Act 5 Lineage Actions (JSON Export, Cypher Query Preview)
function setupAct5LineageActions() {
  const btnExport = document.getElementById('btn-export-lineage-json');
  const btnCypher = document.getElementById('btn-view-cypher');
  const modalContainer = document.getElementById('cypher-modal');

  if (btnExport) {
    btnExport.addEventListener('click', () => {
      const lineagePayload = {
        platform: "LineageForge Enterprise v1.0",
        repository: "https://github.com/TestAccount-04/datalineage.git",
        targetAsset: "dataset://snowflake/corp_risk/prod/dw_finance/analytics/daily_risk_summary",
        verifiedAt: new Date().toISOString(),
        confidenceScore: 0.98,
        pathLength: 5,
        nodes: graphData.nodes,
        edges: graphData.edges,
        columnLevelDerivations: [
          {
            targetColumn: "risk_score",
            sourceColumns: ["orders.amount", "customers.credit_score"],
            formula: "SUM(orders.amount) * (customers.credit_multiplier)",
            parser: "SQLGlot 23.0 + Tree-sitter"
          }
        ]
      };

      const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(lineagePayload, null, 2));
      const downloadAnchor = document.createElement('a');
      downloadAnchor.setAttribute("href", dataStr);
      downloadAnchor.setAttribute("download", "canonical_lineage_proof.json");
      document.body.appendChild(downloadAnchor);
      downloadAnchor.click();
      downloadAnchor.remove();
    });
  }

  if (btnCypher && modalContainer) {
    btnCypher.addEventListener('click', () => {
      modalContainer.style.display = modalContainer.style.display === 'none' ? 'block' : 'none';
      if (modalContainer.style.display === 'block') {
        modalContainer.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  }
}

// Detailed Mode Sidebar Active Scroll Indicator (Scrollspy Sync)
function setupSidebarNavigation() {
  const links = document.querySelectorAll('.sidebar-link');
  const sections = document.querySelectorAll('#view-detailed .doc-section');
  if (!links.length || !sections.length) return;

  // 1. Click Listener for smooth navigation
  links.forEach(link => {
    link.addEventListener('click', () => {
      links.forEach(l => l.classList.remove('active'));
      link.classList.add('active');
    });
  });

  // 2. Real-time Scroll Listener to sync active section with left sidebar
  let isScrolling = false;
  function handleScrollspy() {
    if (state.activeMode !== 'detailed') return;

    // Trigger line just below top navigation
    const scrollYPos = window.scrollY + 170;
    let activeId = null;

    for (let i = 0; i < sections.length; i++) {
      const section = sections[i];
      const top = section.offsetTop;
      const height = section.offsetHeight;

      if (scrollYPos >= top && scrollYPos < top + height) {
        activeId = section.getAttribute('id');
        break;
      }
    }

    // Edge case: scrolled to bottom of document
    if (!activeId && (window.innerHeight + window.scrollY >= document.body.offsetHeight - 150)) {
      activeId = sections[sections.length - 1].getAttribute('id');
    }

    if (activeId) {
      links.forEach(link => {
        const href = link.getAttribute('href');
        if (href === `#${activeId}`) {
          if (!link.classList.contains('active')) {
            links.forEach(l => l.classList.remove('active'));
            link.classList.add('active');

            // Ensure active link stays visible inside sticky sidebar
            const sidebar = document.querySelector('.doc-sidebar');
            if (sidebar) {
              const linkRect = link.getBoundingClientRect();
              const sidebarRect = sidebar.getBoundingClientRect();
              if (linkRect.top < sidebarRect.top || linkRect.bottom > sidebarRect.bottom) {
                link.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
              }
            }
          }
        }
      });
    }
  }

  window.addEventListener('scroll', () => {
    if (!isScrolling) {
      window.requestAnimationFrame(() => {
        handleScrollspy();
        isScrolling = false;
      });
      isScrolling = true;
    }
  }, { passive: true });

  // Initial check
  setTimeout(handleScrollspy, 200);
}

