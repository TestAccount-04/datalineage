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
  setupPlaneSimulation();
  setupSlackWarRoom();
  setupIdentityResolver();
  setupRiddleZoomSwitcher();
  setupGhostDepToggle();
  setupBlastRadiusSimulator();
  setupSidebarNavigation();
  setupAct5LineageActions();
  initializeMermaid();
  window.dalaGuide = new DaLaGuideController();
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

      // Switch DaLa to story mode
      if (window.dalaGuide && window.dalaGuide.isTourActive) {
        window.dalaGuide.switchMode('story');
      }
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

      // Switch DaLa to detailed blueprint mode
      if (window.dalaGuide && window.dalaGuide.isTourActive) {
        window.dalaGuide.switchMode('detailed');
      }
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

  // Notify DaLa guide if active
  if (window.dalaGuide) {
    window.dalaGuide.onNodeSelected(node);
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
// State tracking for Live Extraction Simulation
let simState = {
  static: false,
  metadata: false,
  runtime: false,
  nodeCount: 0,
  edgeCount: 0,
  confidence: 0
};

// Clue Collection Interaction & Simulation Hook
function setupClueCollection() {
  const clueButtons = document.querySelectorAll('.btn-collect');
  const progressFill = document.getElementById('clue-progress-fill');
  const progressText = document.getElementById('clue-count-text');
  const deductionUnlock = document.getElementById('deduction-unlock-card');
  const dossierUnlock = document.getElementById('dossier-unlock-card');

  clueButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const clueType = btn.dataset.clue;
      if (!clueType) return;

      if (!state.cluesCollected[clueType]) {
        state.cluesCollected[clueType] = true;
        
        // Update Card UI
        const card = btn.closest('.clue-card');
        if (card) card.classList.add('collected');
        btn.textContent = '✓ Clue Verified & Logged';
        btn.disabled = true;

        // Trigger Live Extraction Simulation
        triggerPlaneSimulation(clueType);

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
  if (progressText) {
    if (collectedCount === 3) {
      progressText.textContent = `3/3 Clues Verified (100% • All Evidence Planes Synced! 🟢)`;
      progressText.style.color = '#059669';
      progressText.style.fontWeight = '700';
    } else {
      progressText.textContent = `${collectedCount}/3 Clues Collected (${percentage}%)`;
    }
  }

  if (collectedCount === 3) {
    if (deductionUnlock) {
      deductionUnlock.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }
}

// Live Plane Extraction Simulation
function setupPlaneSimulation() {
  const btnSimulateAll = document.getElementById('btn-simulate-all-clues');
  if (btnSimulateAll) {
    btnSimulateAll.addEventListener('click', () => {
      runFullPlaneSimulation();
    });
  }
}

function triggerPlaneSimulation(planeType) {
  if (simState[planeType]) return;
  simState[planeType] = true;

  const terminalBody = document.getElementById('sim-terminal-body');
  const nodeCountEl = document.getElementById('sim-node-count');
  const edgeCountEl = document.getElementById('sim-edge-count');
  const confValEl = document.getElementById('sim-confidence-val');
  const logIndicator = document.getElementById('sim-log-indicator');

  if (logIndicator) logIndicator.textContent = 'EXTRACTING...';

  function appendLog(text, type = 'info') {
    if (!terminalBody) return;
    const line = document.createElement('div');
    line.className = `sim-log-line ${type}`;
    line.innerHTML = `&gt; ${text}`;
    terminalBody.appendChild(line);
    terminalBody.scrollTop = terminalBody.scrollHeight;
  }

  if (planeType === 'static') {
    const col = document.getElementById('sim-col-static');
    if (col) col.classList.add('active-plane');
    const badge = document.getElementById('sim-status-static');
    if (badge) badge.textContent = 'DISCOVERED (+2N, +1E)';

    const box = document.getElementById('sim-box-static');
    if (box) {
      const placeholder = box.querySelector('.sim-placeholder-text');
      if (placeholder) placeholder.style.display = 'none';
      box.querySelectorAll('.sim-entity-item, .sim-edge-item').forEach(el => el.classList.add('discovered'));
    }

    const tNode1 = document.getElementById('track-node-1');
    const tNode2 = document.getElementById('track-node-2');
    const tArr1 = document.getElementById('track-arr-1');
    if (tNode1) tNode1.classList.add('active-node');
    if (tNode2) tNode2.classList.add('active-node');
    if (tArr1) tArr1.classList.add('active-arrow');

    appendLog('<strong>[STATIC AST SCAN]</strong> Tree-sitter &amp; SQLGlot parsed <code>RiskService.java:112</code>. Extracted nodes: <code>RiskService.java</code> (Code) &amp; <code>customers</code> (Schema). Edge: AST SELECT query.', 'info');

    if (window.dalaBot) {
      window.dalaBot.speak("🧐 <strong>Static Clue Extracted!</strong> Tree-sitter parsed <code>RiskService.java</code> and discovered an AST SELECT referencing <code>customers.credit_score</code>. We've logged <strong>+2 Nodes and +1 Edge</strong>!", "sleuth");
    }
  } else if (planeType === 'metadata') {
    const col = document.getElementById('sim-col-metadata');
    if (col) col.classList.add('active-plane');
    const badge = document.getElementById('sim-status-metadata');
    if (badge) badge.textContent = 'DISCOVERED (+2N, +1E)';

    const box = document.getElementById('sim-box-metadata');
    if (box) {
      const placeholder = box.querySelector('.sim-placeholder-text');
      if (placeholder) placeholder.style.display = 'none';
      box.querySelectorAll('.sim-entity-item, .sim-edge-item').forEach(el => el.classList.add('discovered'));
    }

    const tNode4 = document.getElementById('track-node-4');
    const tArr3 = document.getElementById('track-arr-3');
    if (tNode4) tNode4.classList.add('active-node');
    if (tArr3) tArr3.classList.add('active-arrow');

    appendLog('<strong>[CATALOG FINGERPRINT]</strong> Snowflake catalog &amp; dbt manifest queried. Extracted nodes: <code>daily_risk_summary</code> (DWH) &amp; <code>risk_enriched_orders</code>. Edge: dbt DAG manifest dependency.', 'info');

    if (window.dalaBot) {
      window.dalaBot.speak("💡 <strong>Metadata Clue Extracted!</strong> dbt manifest confirmed that <code>daily_risk_summary</code> depends on <code>risk_enriched_orders</code>. But how did the Java service connect to this table? We need Runtime Eyewitness!", "eureka");
    }
  } else if (planeType === 'runtime') {
    const col = document.getElementById('sim-col-runtime');
    if (col) col.classList.add('active-plane');
    const badge = document.getElementById('sim-status-runtime');
    if (badge) badge.textContent = 'DISCOVERED (+2N, +2E)';

    const box = document.getElementById('sim-box-runtime');
    if (box) {
      const placeholder = box.querySelector('.sim-placeholder-text');
      if (placeholder) placeholder.style.display = 'none';
      box.querySelectorAll('.sim-entity-item, .sim-edge-item').forEach(el => el.classList.add('discovered'));
    }

    const tNode3 = document.getElementById('track-node-3');
    const tNode5 = document.getElementById('track-node-5');
    const tArr2 = document.getElementById('track-arr-2');
    const tArr4 = document.getElementById('track-arr-4');
    if (tNode3) tNode3.classList.add('active-node');
    if (tNode5) tNode5.classList.add('active-node');
    if (tArr2) tArr2.classList.add('active-arrow');
    if (tArr4) tArr4.classList.add('active-arrow');

    appendLog('<strong>[OPENLINEAGE RUNTIME]</strong> Telemetry event #8912 intercepted live PySpark execution. Extracted nodes: <code>batch_risk_enricher.py</code> (Job) &amp; <code>RunFacet #8912</code>. Formed <strong>Cross-Plane Bridge: Java ➔ Python</strong>!', 'success');

    if (window.dalaBot) {
      window.dalaBot.speak("⚡ <strong>Runtime Eyewitness Extracted!</strong> OpenLineage intercepted the live PySpark execution facet. This formed the critical bridge connecting Java to Python and into Snowflake!", "eureka");
    }
  }

  // Recalculate metrics
  let activePlanes = [simState.static, simState.metadata, simState.runtime].filter(Boolean).length;
  simState.nodeCount = activePlanes === 1 ? 2 : (activePlanes === 2 ? 4 : 6);
  simState.edgeCount = activePlanes === 1 ? 1 : (activePlanes === 2 ? 2 : 5);
  simState.confidence = activePlanes === 1 ? 33 : (activePlanes === 2 ? 68 : 98);

  if (nodeCountEl) nodeCountEl.textContent = `${simState.nodeCount} / 6`;
  if (edgeCountEl) edgeCountEl.textContent = `${simState.edgeCount} / 5`;
  if (confValEl) confValEl.textContent = `${simState.confidence}%`;

  if (activePlanes === 3) {
    if (logIndicator) logIndicator.textContent = 'SYNCHRONIZED';
    const convStatus = document.getElementById('sim-conv-status');
    if (convStatus) {
      convStatus.textContent = '✓ FABRIC SYNCHRONIZED (98% CONFIDENCE)';
      convStatus.classList.add('synced');
    }
    appendLog('🎉 <strong>[CONVERGENCE VERIFIED]</strong> Multi-plane lineage fabric unified! 6 nodes, 5 edges fully bound with 98% confidence.', 'success');

    if (window.dalaBot) {
      setTimeout(() => {
        window.dalaBot.speak("🎉 <strong>All 3 Evidence Planes Synced!</strong> The missing link is proven: <code>Postgres</code> ➔ <code>Java</code> ➔ <code>PySpark</code> ➔ <code>Snowflake</code> ➔ <code>Power BI</code>. You've uncovered the full lineage path!", "happy");
      }, 600);
    }
  } else {
    if (logIndicator) logIndicator.textContent = 'READY';
  }
}

function runFullPlaneSimulation() {
  const planes = ['static', 'metadata', 'runtime'];
  planes.forEach((plane, idx) => {
    setTimeout(() => {
      const btn = document.querySelector(`.btn-collect[data-clue="${plane}"]`);
      if (btn && !state.cluesCollected[plane]) {
        btn.click();
      } else {
        triggerPlaneSimulation(plane);
      }
    }, idx * 600);
  });
}

// Waypoints for DaLa's Journey Down the Page (From Top to Bottom)
const DALA_WAYPOINTS = [
  {
    id: "stop-1-outage",
    targetSelector: ".slack-war-room",
    scrollTarget: "#case-status-bar",
    stepLabel: "Stop 1/7",
    mood: "alert",
    concentrate: "The 09:00 AM System Outage and the 4 squads finger-pointing in Slack.",
    check: "Notice the 5-step Domino Trail: a single Friday afternoon Java rename broke the CFO dashboard.",
    action: "Click the squad chat cards or follow DaLa down to Act I!"
  },
  {
    id: "stop-2-suspects",
    targetSelector: ".detectives-grid",
    scrollTarget: ".detectives-grid",
    stepLabel: "Stop 2/7",
    mood: "sleuth",
    concentrate: "Why traditional single tools and LLMs fail to map distributed enterprise data.",
    check: "Interrogate the 3 Defeated Detectives: Git only sees code, catalogs only see schemas, LLMs hallucinate.",
    action: "Study the Epiphany callout: We need a Unified Multi-Plane Proof Fabric!"
  },
  {
    id: "stop-3-architecture",
    targetSelector: ".evidence-model-banner-wrap",
    scrollTarget: ".evidence-model-container",
    stepLabel: "Stop 3/7",
    mood: "eureka",
    concentrate: "The 3-Plane Forensic Blueprint positioned above the clue cards.",
    check: "Examine how Static Code, Metadata Catalogs, and Runtime Observers intersect.",
    action: "Review the top infographic before collecting clues below!"
  },
  {
    id: "stop-4-clues-sim",
    targetSelector: "#simulation-workbench",
    scrollTarget: ".clues-investigation-wrapper",
    stepLabel: "Stop 4/7",
    mood: "eureka",
    concentrate: "Forensic Node & Edge Extraction simulation.",
    check: "Watch raw code, catalog manifests, and runtime events turn into physical graph nodes & directed edges.",
    action: "Click the 3 clue cards or click 'Run Full Plane Simulation'!"
  },
  {
    id: "stop-5-riddles",
    targetSelector: ".ghost-dep-grid",
    scrollTarget: "#deduction-unlock-card",
    stepLabel: "Stop 5/7",
    mood: "sleuth",
    concentrate: "Ghost Dependencies & Hierarchical Zoom Levels in Act III.",
    check: "Riddle 3: Spot the difference between Control Flow (task order) and Data Flow (actual data movement).",
    action: "Click the Control vs Data buttons to reveal the hidden runtime dependency!"
  },
  {
    id: "stop-6-graph-engine",
    targetSelector: "#interactive-graph-engine",
    scrollTarget: "#interactive-graph-engine",
    stepLabel: "Stop 6/7",
    mood: "eureka",
    concentrate: "Interactive Unified Dual-Graph & Blast Radius Simulator.",
    check: "Notice the green (Control Flow) and blue (Data Flow) graph with 98% confidence.",
    action: "Click any node in the SVG canvas or run the blast radius simulator on RiskService.java!"
  },
  {
    id: "stop-7-proof-dossier",
    targetSelector: ".roadmap-grid",
    scrollTarget: "#dossier-unlock-card",
    stepLabel: "Stop 7/7",
    mood: "happy",
    concentrate: "The Undeniable Lineage Proof Fabric and 90-Day Production Roadmap.",
    check: "The 6-tuple canonical URI resolution proving root-cause Java change to dashboard.",
    action: "Explore the 90-day implementation roadmap or toggle to Detailed Blueprint mode!"
  }
];

// Bespoke 3D Traveling Cyber-Detective Companion: DaLa
class DaLa3DTravelingCompanion {
  constructor() {
    this.container = document.getElementById('dala-traveler');
    this.unit = document.getElementById('dala-unit');
    this.canvas3D = document.getElementById('dala-3d-canvas');
    this.chassis = document.getElementById('dala-3d-chassis');
    this.briefingCard = document.getElementById('dala-briefing-card');
    
    // HUD Elements
    this.hudConcentrate = document.getElementById('dala-hud-concentrate');
    this.hudCheck = document.getElementById('dala-hud-check');
    this.hudAction = document.getElementById('dala-hud-action');
    this.hudStop = document.getElementById('dala-stop-step');
    this.hudMood = document.getElementById('dala-mood-pill');
    this.unreadBadge = document.getElementById('dala-unread-badge');

    // Controls
    this.btnNext = document.getElementById('dala-btn-next-step');
    this.btnLaser = document.getElementById('dala-btn-laser');
    this.btnPoke = document.getElementById('dala-btn-poke');
    this.btnMinimize = document.getElementById('dala-btn-minimize');

    // Laser Beam SVG Elements
    this.laserLine = document.getElementById('dala-laser-line');
    this.laserRing = document.getElementById('dala-laser-target-ring');

    this.currentWaypointIdx = 0;
    this.currentMood = 'sleuth';
    this.isMinimized = false;
    this.laserTimeout = null;

    // Mouse Tracking for 3D Head Turn
    this.mouseX = 0;
    this.mouseY = 0;
    this.targetRotX = 0;
    this.targetRotY = 0;

    // Quotes for Poking
    this.pokeQuotes = [
      "🧐 Elementary, my dear engineer! Git only tracks who pushed the code, not whose pipeline crashed at 3 AM.",
      "💡 Schemas don't lie, but runtime environments often keep secrets. Always collect OpenLineage facets!",
      "🚨 73% of enterprise outages trace back to untracked schema drifts across microservice boundaries!",
      "🔬 A graph without verified edges is just a list of things waiting to break. Keep gathering clues!",
      "☕ Pro-tip: Renaming a database column on a Friday afternoon without blast radius analysis is an OSHA violation in Data Engineering!",
      "⚡ Notice how Static + Catalog + Runtime converge? One plane without the others is just a guess!"
    ];
    this.pokeIndex = 0;

    this.init();
  }

  init() {
    if (!this.container) return;

    // Setup 3D WebGL / Fallback Rig
    this.setup3DRig();

    // Mouse Tracking Listener
    window.addEventListener('mousemove', (e) => {
      this.mouseX = e.clientX;
      this.mouseY = e.clientY;
    });

    // Button Listeners
    if (this.btnNext) {
      this.btnNext.addEventListener('click', () => {
        this.advanceToNextWaypoint();
      });
    }

    if (this.btnLaser) {
      this.btnLaser.addEventListener('click', () => {
        this.fireLaserPointer();
      });
    }

    if (this.btnPoke) {
      this.btnPoke.addEventListener('click', () => {
        this.poke();
      });
    }

    if (this.chassis) {
      this.chassis.addEventListener('click', () => {
        if (this.isMinimized) {
          this.expand();
        } else {
          this.poke();
        }
      });
    }

    if (this.btnMinimize) {
      this.btnMinimize.addEventListener('click', (e) => {
        e.stopPropagation();
        this.toggleMinimize();
      });
    }

    // Scroll Observer for Traveling Down Page
    this.setupScrollTravelObserver();

    // Initial Waypoint Load
    this.applyWaypoint(0, false);
  }

  setup3DRig() {
    if (typeof THREE === 'undefined' || !this.canvas3D) {
      this.setup2DFallbackRig();
      return;
    }

    try {
      const width = 130;
      const height = 130;
      
      // Scene, Camera, Renderer
      this.scene = new THREE.Scene();
      this.camera = new THREE.PerspectiveCamera(40, width / height, 0.1, 100);
      this.camera.position.set(0, 0, 4.4);

      this.renderer = new THREE.WebGLRenderer({
        canvas: this.canvas3D,
        alpha: true,
        antialias: true
      });
      this.renderer.setSize(width, height);
      this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

      // Lighting
      const ambLight = new THREE.AmbientLight(0xffffff, 0.85);
      this.scene.add(ambLight);

      const dirLight = new THREE.DirectionalLight(0x38bdf8, 1.4);
      dirLight.position.set(3, 4, 3);
      this.scene.add(dirLight);

      const bluePoint = new THREE.PointLight(0x2563eb, 1.2, 8);
      bluePoint.position.set(-2, -1, 2);
      this.scene.add(bluePoint);

      // Character Master Group
      this.dalaGroup = new THREE.Group();
      this.scene.add(this.dalaGroup);

      // 1. Aero Head & Chassis (Pearlescent metallic orb)
      const headGeo = new THREE.SphereGeometry(1.05, 32, 32);
      const headMat = new THREE.MeshStandardMaterial({
        color: 0xf8fafc,
        roughness: 0.18,
        metalness: 0.35
      });
      this.headMesh = new THREE.Mesh(headGeo, headMat);
      this.dalaGroup.add(this.headMesh);

      // 2. Dynamic Procedural Eye Canvas Texture
      this.eyeCanvas = document.createElement('canvas');
      this.eyeCanvas.width = 256;
      this.eyeCanvas.height = 256;
      this.eyeCtx = this.eyeCanvas.getContext('2d');
      this.renderEyeTexture('sleuth');

      this.eyeTexture = new THREE.CanvasTexture(this.eyeCanvas);
      const visorGeo = new THREE.SphereGeometry(1.06, 32, 16, 0, Math.PI * 2, 0, Math.PI * 0.45);
      visorGeo.rotateX(Math.PI / 2);
      const visorMat = new THREE.MeshBasicMaterial({
        map: this.eyeTexture,
        transparent: true
      });
      this.visorMesh = new THREE.Mesh(visorGeo, visorMat);
      this.dalaGroup.add(this.visorMesh);

      // 3. Holographic Detective Hat Ring (Floating Torus)
      const haloGeo1 = new THREE.TorusGeometry(1.35, 0.065, 16, 48);
      this.haloMat1 = new THREE.MeshBasicMaterial({
        color: 0x38bdf8,
        transparent: true,
        opacity: 0.88
      });
      this.haloMesh1 = new THREE.Mesh(haloGeo1, this.haloMat1);
      this.haloMesh1.position.set(0, 0.55, 0);
      this.haloMesh1.rotation.x = 0.45;
      this.haloMesh1.rotation.z = -0.15;
      this.dalaGroup.add(this.haloMesh1);

      // Inner Halo Ring
      const haloGeo2 = new THREE.TorusGeometry(1.12, 0.03, 16, 36);
      this.haloMat2 = new THREE.MeshBasicMaterial({
        color: 0x60a5fa,
        transparent: true,
        opacity: 0.6
      });
      this.haloMesh2 = new THREE.Mesh(haloGeo2, this.haloMat2);
      this.haloMesh2.position.set(0, 0.52, 0);
      this.haloMesh2.rotation.x = -0.35;
      this.dalaGroup.add(this.haloMesh2);

      // 4. Floating Magnetic Gauntlets (Hands)
      const handMat = new THREE.MeshStandardMaterial({
        color: 0x1e293b,
        roughness: 0.2,
        metalness: 0.8
      });

      const handGeoL = new THREE.SphereGeometry(0.24, 16, 16);
      this.handL = new THREE.Mesh(handGeoL, handMat);
      this.handL.position.set(-1.25, -0.4, 0.35);
      this.dalaGroup.add(this.handL);

      const handGeoR = new THREE.SphereGeometry(0.24, 16, 16);
      this.handR = new THREE.Mesh(handGeoR, handMat);
      this.handR.position.set(1.25, -0.35, 0.45);
      this.dalaGroup.add(this.handR);

      // 5. Lineage Tail Nodes (Trailing data packet orbs)
      this.tailNodes = [];
      const tailMat = new THREE.MeshBasicMaterial({ color: 0x38bdf8 });
      for (let i = 0; i < 3; i++) {
        const node = new THREE.Mesh(new THREE.SphereGeometry(0.12 - i * 0.02, 12, 12), tailMat);
        node.position.set(0, -1.2 - i * 0.35, -0.2 * i);
        this.dalaGroup.add(node);
        this.tailNodes.push(node);
      }

      // Render Loop
      let clock = 0;
      const animate = () => {
        requestAnimationFrame(animate);
        clock += 0.025;

        // Hover Floating Sine Wave
        this.dalaGroup.position.y = Math.sin(clock * 1.8) * 0.12;

        // Halo Orbit Rotation
        this.haloMesh1.rotation.y += 0.022;
        this.haloMesh2.rotation.y -= 0.035;

        // Magnetic Hands Float
        this.handL.position.y = -0.4 + Math.sin(clock * 2.2) * 0.06;
        this.handR.position.y = -0.35 + Math.cos(clock * 2.2) * 0.06;

        // Tail Wave
        this.tailNodes.forEach((node, idx) => {
          node.position.x = Math.sin(clock * 2 + idx) * (0.08 + idx * 0.05);
        });

        // Mouse Cursor Head Tracking (Smooth Lerp)
        const targetRotY = ((this.mouseX / window.innerWidth) - 0.5) * 0.75;
        const targetRotX = ((this.mouseY / window.innerHeight) - 0.5) * 0.55;
        this.dalaGroup.rotation.y += (targetRotY - this.dalaGroup.rotation.y) * 0.08;
        this.dalaGroup.rotation.x += (targetRotX - this.dalaGroup.rotation.x) * 0.08;

        this.renderer.render(this.scene, this.camera);
      };
      animate();

      // Periodic Eye Blink
      setInterval(() => {
        this.blink();
      }, 4200);

    } catch (err) {
      console.warn("Three.js initialization failed, falling back to 2D canvas rig", err);
      this.setup2DFallbackRig();
    }
  }

  setup2DFallbackRig() {
    if (!this.canvas3D) return;
    const ctx = this.canvas3D.getContext('2d');
    let clock = 0;
    const render2D = () => {
      requestAnimationFrame(render2D);
      clock += 0.03;
      ctx.clearRect(0, 0, 130, 130);
      const bob = Math.sin(clock * 2) * 4;

      // Glow Aura
      const radGlow = ctx.createRadialGradient(65, 65 + bob, 20, 65, 65 + bob, 55);
      radGlow.addColorStop(0, 'rgba(56, 189, 248, 0.4)');
      radGlow.addColorStop(1, 'rgba(56, 189, 248, 0)');
      ctx.fillStyle = radGlow;
      ctx.beginPath();
      ctx.arc(65, 65 + bob, 55, 0, Math.PI * 2);
      ctx.fill();

      // Hat Halo
      ctx.strokeStyle = '#38bdf8';
      ctx.lineWidth = 3.5;
      ctx.beginPath();
      ctx.ellipse(65, 42 + bob, 38, 10, -0.15, 0, Math.PI * 2);
      ctx.stroke();

      // Orb Body
      const orbGrad = ctx.createRadialGradient(55, 60 + bob, 8, 65, 65 + bob, 42);
      orbGrad.addColorStop(0, '#ffffff');
      orbGrad.addColorStop(0.7, '#cbd5e1');
      orbGrad.addColorStop(1, '#64748b');
      ctx.fillStyle = orbGrad;
      ctx.beginPath();
      ctx.arc(65, 65 + bob, 38, 0, Math.PI * 2);
      ctx.fill();

      // Visor Screen
      ctx.fillStyle = '#030712';
      ctx.beginPath();
      ctx.roundRect(40, 55 + bob, 50, 24, 8);
      ctx.fill();

      // Eyes
      ctx.fillStyle = this.currentMood === 'alert' ? '#ef4444' : (this.currentMood === 'eureka' ? '#f59e0b' : '#38bdf8');
      ctx.beginPath();
      ctx.arc(52, 67 + bob, 5, 0, Math.PI * 2);
      ctx.arc(78, 67 + bob, 5, 0, Math.PI * 2);
      ctx.fill();
    };
    render2D();
  }

  renderEyeTexture(mood, isBlinking = false) {
    if (!this.eyeCtx) return;
    const ctx = this.eyeCtx;
    ctx.clearRect(0, 0, 256, 256);

    // Dark Curved Visor Base
    ctx.fillStyle = '#020617';
    ctx.beginPath();
    ctx.roundRect(24, 70, 208, 116, 32);
    ctx.fill();

    // Visor Cyan Border Glow
    ctx.strokeStyle = 'rgba(56, 189, 248, 0.4)';
    ctx.lineWidth = 3;
    ctx.stroke();

    if (isBlinking) {
      // Closed Eyelid slits
      ctx.strokeStyle = '#38bdf8';
      ctx.lineWidth = 5;
      ctx.beginPath();
      ctx.moveTo(60, 128); ctx.lineTo(105, 128);
      ctx.moveTo(151, 128); ctx.lineTo(196, 128);
      ctx.stroke();
      if (this.eyeTexture) this.eyeTexture.needsUpdate = true;
      return;
    }

    if (mood === 'alert') {
      // Flashing Exclamation Oculars
      ctx.fillStyle = '#ef4444';
      ctx.font = 'bold 54px monospace';
      ctx.textAlign = 'center';
      ctx.fillText('!', 82, 142);
      ctx.fillText('!', 174, 142);
    } else if (mood === 'eureka') {
      // Golden Star Eyes
      ctx.fillStyle = '#f59e0b';
      this.drawStar(ctx, 82, 128, 5, 20, 10);
      this.drawStar(ctx, 174, 128, 5, 20, 10);
    } else if (mood === 'happy') {
      // Curved Smiling Eyes ^ ^
      ctx.strokeStyle = '#10b981';
      ctx.lineWidth = 6;
      ctx.beginPath();
      ctx.arc(82, 134, 16, Math.PI, 0);
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(174, 134, 16, Math.PI, 0);
      ctx.stroke();
    } else {
      // Sleuth / Normal Scanning Oculars
      ctx.fillStyle = '#38bdf8';
      ctx.beginPath();
      ctx.arc(82, 128, 18, 0, Math.PI * 2);
      ctx.arc(174, 128, 18, 0, Math.PI * 2);
      ctx.fill();

      // Shiny Pupil Highlights
      ctx.fillStyle = '#ffffff';
      ctx.beginPath();
      ctx.arc(77, 123, 6, 0, Math.PI * 2);
      ctx.arc(169, 123, 6, 0, Math.PI * 2);
      ctx.fill();

      // Right Eye Scanning Crosshair Ring
      ctx.strokeStyle = 'rgba(56, 189, 248, 0.7)';
      ctx.lineWidth = 2.5;
      ctx.beginPath();
      ctx.arc(174, 128, 28, 0, Math.PI * 2);
      ctx.stroke();
    }

    // Expressive Cyber-Mouth Line
    ctx.strokeStyle = mood === 'alert' ? '#ef4444' : (mood === 'eureka' ? '#f59e0b' : '#38bdf8');
    ctx.lineWidth = 3;
    ctx.beginPath();
    if (mood === 'happy' || mood === 'eureka') {
      ctx.arc(128, 150, 14, 0, Math.PI);
    } else if (mood === 'alert') {
      ctx.arc(128, 166, 12, Math.PI, 0);
    } else {
      ctx.moveTo(112, 160);
      ctx.lineTo(144, 160);
    }
    ctx.stroke();

    if (this.eyeTexture) this.eyeTexture.needsUpdate = true;
  }

  drawStar(ctx, cx, cy, spikes, outerRadius, innerRadius) {
    let rot = Math.PI / 2 * 3;
    let x = cx;
    let y = cy;
    let step = Math.PI / spikes;

    ctx.beginPath();
    ctx.moveTo(cx, cy - outerRadius);
    for (let i = 0; i < spikes; i++) {
      x = cx + Math.cos(rot) * outerRadius;
      y = cy + Math.sin(rot) * outerRadius;
      ctx.lineTo(x, y);
      rot += step;

      x = cx + Math.cos(rot) * innerRadius;
      y = cy + Math.sin(rot) * innerRadius;
      ctx.lineTo(x, y);
      rot += step;
    }
    ctx.lineTo(cx, cy - outerRadius);
    ctx.closePath();
    ctx.fill();
  }

  blink() {
    this.renderEyeTexture(this.currentMood, true);
    setTimeout(() => {
      this.renderEyeTexture(this.currentMood, false);
    }, 180);
  }

  setMood(mood) {
    this.currentMood = mood;
    if (this.container) {
      this.container.classList.remove('mood-sleuth', 'mood-eureka', 'mood-alert', 'mood-happy', 'mood-idle');
      this.container.classList.add(`mood-${mood}`);
    }

    if (this.hudMood) {
      const labels = {
        sleuth: "🧐 Sleuth",
        eureka: "💡 Eureka!",
        alert: "🚨 Alert!",
        happy: "😊 Verified",
        idle: "😴 Standby"
      };
      this.hudMood.textContent = labels[mood] || "🧐 DaLa";
    }

    if (this.haloMat1) {
      const colors = {
        sleuth: 0x38bdf8,
        eureka: 0xf59e0b,
        alert: 0xef4444,
        happy: 0x10b981,
        idle: 0x64748b
      };
      this.haloMat1.color.setHex(colors[mood] || 0x38bdf8);
    }

    this.renderEyeTexture(mood, false);
  }

  applyWaypoint(idx, triggerLaser = true) {
    if (idx < 0 || idx >= DALA_WAYPOINTS.length) return;
    this.currentWaypointIdx = idx;
    const wp = DALA_WAYPOINTS[idx];

    if (this.hudStop) this.hudStop.textContent = wp.stepLabel;
    if (this.hudConcentrate) this.hudConcentrate.textContent = wp.concentrate;
    if (this.hudCheck) this.hudCheck.textContent = wp.check;
    if (this.hudAction) this.hudAction.textContent = wp.action;

    this.setMood(wp.mood);

    if (triggerLaser) {
      setTimeout(() => {
        this.fireLaserPointer();
      }, 350);
    }
  }

  advanceToNextWaypoint() {
    const nextIdx = (this.currentWaypointIdx + 1) % DALA_WAYPOINTS.length;
    const wp = DALA_WAYPOINTS[nextIdx];
    this.applyWaypoint(nextIdx, true);

    // Smoothly scroll to the target section
    const targetEl = document.querySelector(wp.scrollTarget);
    if (targetEl) {
      targetEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  }

  fireLaserPointer() {
    if (!this.laserLine || !this.laserRing) return;
    const wp = DALA_WAYPOINTS[this.currentWaypointIdx];
    const targetEl = document.querySelector(wp.targetSelector);
    if (!targetEl) return;

    // Remove any previous target highlight
    document.querySelectorAll('.dala-target-highlight').forEach(el => el.classList.remove('dala-target-highlight'));

    const targetRect = targetEl.getBoundingClientRect();
    const targetX = targetRect.left + targetRect.width / 2;
    const targetY = targetRect.top + targetRect.height / 2;

    const chassisRect = this.chassis ? this.chassis.getBoundingClientRect() : { left: window.innerWidth - 80, top: 200 };
    const originX = chassisRect.left + 35;
    const originY = chassisRect.top + 65;

    // Set SVG laser coordinates
    this.laserLine.setAttribute('x1', originX);
    this.laserLine.setAttribute('y1', originY);
    this.laserLine.setAttribute('x2', targetX);
    this.laserLine.setAttribute('y2', targetY);
    this.laserLine.style.opacity = '1';

    this.laserRing.setAttribute('cx', targetX);
    this.laserRing.setAttribute('cy', targetY);
    this.laserRing.style.opacity = '1';

    // Add pulsing highlight to target element
    targetEl.classList.add('dala-target-highlight');

    // Spin Hat in 3D
    if (this.haloMesh1) {
      this.haloMesh1.rotation.y += Math.PI * 2;
    }

    if (this.laserTimeout) clearTimeout(this.laserTimeout);
    this.laserTimeout = setTimeout(() => {
      if (this.laserLine) this.laserLine.style.opacity = '0';
      if (this.laserRing) this.laserRing.style.opacity = '0';
      setTimeout(() => {
        targetEl.classList.remove('dala-target-highlight');
      }, 1500);
    }, 3200);
  }

  poke() {
    const quote = this.pokeQuotes[this.pokeIndex % this.pokeQuotes.length];
    this.pokeIndex++;
    
    // Temporarily show quote in HUD
    if (this.hudCheck) {
      const originalCheck = this.hudCheck.textContent;
      this.hudCheck.innerHTML = `<em>"${quote}"</em>`;
      setTimeout(() => {
        this.hudCheck.textContent = originalCheck;
      }, 5500);
    }

    // Spin 3D hat and change mood
    const moods = ['eureka', 'sleuth', 'happy', 'alert'];
    this.setMood(moods[Math.floor(Math.random() * moods.length)]);

    // Sound chime
    this.playTone();
  }

  playTone() {
    try {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      const now = ctx.currentTime;
      gain.gain.setValueAtTime(0.04, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.22);
      osc.frequency.setValueAtTime(587, now);
      osc.frequency.exponentialRampToValueAtTime(880, now + 0.2);
      osc.start(now);
      osc.stop(now + 0.23);
    } catch (e) {}
  }

  toggleMinimize() {
    this.isMinimized = !this.isMinimized;
    if (this.isMinimized) {
      this.container.classList.add('minimized');
      if (this.unreadBadge) this.unreadBadge.style.display = 'block';
    } else {
      this.container.classList.remove('minimized');
      if (this.unreadBadge) this.unreadBadge.style.display = 'none';
    }
  }

  expand() {
    this.isMinimized = false;
    this.container.classList.remove('minimized');
    if (this.unreadBadge) this.unreadBadge.style.display = 'none';
  }

  setupScrollTravelObserver() {
    // Tracks active section and glides DaLa's unit smoothly down alongside content
    let ticking = false;

    window.addEventListener('scroll', () => {
      if (!ticking) {
        window.requestAnimationFrame(() => {
          this.updateTravelPosition();
          ticking = false;
        });
        ticking = true;
      }
    }, { passive: true });
  }

  updateTravelPosition() {
    if (!this.container) return;

    // Find closest waypoint based on scroll position
    const viewportMiddle = window.scrollY + window.innerHeight * 0.4;
    let closestIdx = 0;
    let minDistance = Infinity;

    DALA_WAYPOINTS.forEach((wp, idx) => {
      const el = document.querySelector(wp.targetSelector);
      if (el) {
        const top = el.getBoundingClientRect().top + window.scrollY;
        const dist = Math.abs(top - viewportMiddle);
        if (dist < minDistance) {
          minDistance = dist;
          closestIdx = idx;
        }
      }
    });

    if (closestIdx !== this.currentWaypointIdx) {
      this.applyWaypoint(closestIdx, false);
    }

    // Dynamic Glide Positioning alongside the active section
    const currentWp = DALA_WAYPOINTS[this.currentWaypointIdx];
    const activeEl = document.querySelector(currentWp.targetSelector);
    if (activeEl) {
      const rect = activeEl.getBoundingClientRect();
      const idealY = Math.max(90, Math.min(window.innerHeight - 440, rect.top));
      this.container.style.top = `${idealY}px`;
    }
  }

  speak(text, mood = 'sleuth') {
    if (this.hudCheck) {
      this.hudCheck.innerHTML = text;
    }
    this.setMood(mood);
    this.fireLaserPointer();
  }
}

// Slack War Room Card Highlighting
function setupSlackWarRoom() {
  const cards = document.querySelectorAll('.slack-msg-card');
  cards.forEach(card => {
    card.addEventListener('click', () => {
      cards.forEach(c => {
        c.style.borderColor = 'var(--border-color)';
        c.style.boxShadow = 'none';
      });
      card.style.borderColor = 'var(--blue-primary)';
      card.style.boxShadow = '0 4px 12px rgba(37, 99, 235, 0.15)';
    });
  });
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

// Riddle 2: 5 Zoom Levels Hierarchy Explorer
function setupRiddleZoomSwitcher() {
  const zoomData = {
    1: {
      badge: "Level 1 • High-Level Enterprise Domain",
      title: "Domain-Level Data Flow: Wealth Management ➔ Corporate Risk",
      desc: "<strong>Target Persona:</strong> CFO, CDO, and Business Executives.<br><strong>Reveals:</strong> Cross-domain data governance boundaries without cluttering the screen with database tables or code methods.",
      nodes: "Domain: Wealth_Management ──[EXCHANGES_DATA]──▶ Domain: Corporate_Risk"
    },
    2: {
      badge: "Level 2 • Application / Service Level",
      title: "Service Boundaries: Spring Boot ➔ PySpark Job",
      desc: "<strong>Target Persona:</strong> Enterprise Architects & Engineering Leads.<br><strong>Reveals:</strong> Cross-repository dependencies and team ownership boundaries (e.g. Risk Platform Squad owns RiskService.java).",
      nodes: "Service: Checkout_API ──▶ Service: RiskService.java ──▶ Pipeline: batch_risk_enricher.py"
    },
    3: {
      badge: "Level 3 • Dataset & Physical Table Level",
      title: "Table-to-Table Lineage: PostgreSQL ➔ Snowflake Analytics",
      desc: "<strong>Target Persona:</strong> Data Engineers & Analytics Engineers.<br><strong>Reveals:</strong> Canonical table definitions, warehouse marts, schema formats, partitions, and storage locations.",
      nodes: "Table: public.orders + Table: public.customers ──▶ DWH: daily_risk_summary (Snowflake)"
    },
    4: {
      badge: "Level 4 • Column-Level Lineage (CLL)",
      title: "Field Derivation: amount + credit_score ➔ risk_score",
      desc: "<strong>Target Persona:</strong> BI Developers & Financial Data Analysts.<br><strong>Reveals:</strong> Mathematical transformations and SQL CTE expressions: risk_score = SUM(amount * (credit_score / 850)).",
      nodes: "Column: customers.credit_score ──▶ Column: risk_enriched_orders.risk_factor ──▶ Column: daily_risk_summary.risk_score"
    },
    5: {
      badge: "Level 5 • Code AST & Runtime Execution",
      title: "Deepest Forensic Proof: Java AST + OpenLineage Run ID #8912",
      desc: "<strong>Target Persona:</strong> Core Backend Engineers & SREs.<br><strong>Reveals:</strong> Concrete line of code (RiskService.java:114), Git commit SHA, and exact runtime execution telemetry at 08:00 UTC.",
      nodes: "Git SHA sha_8f4bc: RiskService.java:114 ──[PARSED_BY_TREESITTER]──▶ OpenLineage RunFacet #8912"
    }
  };

  const buttons = document.querySelectorAll('.btn-zoom-level');
  const badgeEl = document.getElementById('zoom-preview-badge');
  const titleEl = document.getElementById('zoom-preview-title');
  const descEl = document.getElementById('zoom-preview-desc');
  const nodesEl = document.getElementById('zoom-preview-nodes');

  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      buttons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const level = btn.dataset.level;
      const data = zoomData[level];
      if (data && badgeEl && titleEl && descEl && nodesEl) {
        badgeEl.textContent = data.badge;
        titleEl.textContent = data.title;
        descEl.innerHTML = data.desc;
        nodesEl.textContent = data.nodes;
      }
    });
  });
}

// Riddle 3: Ghost Dependency Toggle (Control Flow vs Data Flow)
function setupGhostDepToggle() {
  const btnToggle = document.getElementById('btn-toggle-ghost-dep');
  const cardControl = document.getElementById('ghost-card-control');
  const cardData = document.getElementById('ghost-card-data');

  if (btnToggle && cardControl && cardData) {
    let showControl = false;
    btnToggle.addEventListener('click', () => {
      showControl = !showControl;
      if (showControl) {
        cardControl.classList.add('highlight');
        cardData.classList.remove('highlight');
        btnToggle.textContent = '👁️ Currently Viewing: Control Flow (Airflow Execution Order)';
        btnToggle.style.background = '#fef3c7';
        btnToggle.style.color = '#b45309';
      } else {
        cardData.classList.add('highlight');
        cardControl.classList.remove('highlight');
        btnToggle.textContent = '👁️ Currently Viewing: Physical Data Lineage (Byte Movement)';
        btnToggle.style.background = '#ecfdf5';
        btnToggle.style.color = '#047857';
      }
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

      const href = link.getAttribute('href');
      if (href && href.startsWith('#') && window.dalaGuide && window.dalaGuide.mode === 'detailed') {
        const targetId = href.substring(1);
        window.dalaGuide.jumpToSection(targetId);
      }
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

// ==========================================================================
// DALA: DYNAMIC POSITIONING ENGINE (Alternating Left/Right Sides & Dragging)
// ==========================================================================

class DaLaPositioner {
  constructor(hostElement) {
    this.host = hostElement;
    this.currentPosX = 0;
    this.currentPosY = 0;
    this.isUserCustomPositioned = false;
  }

  calculateSidePosition(targetElement, side = 'right', isTopGreeting = false) {
    const vpW = window.innerWidth;
    const vpH = window.innerHeight;
    const targetRect = targetElement.getBoundingClientRect();
    const container = document.querySelector('.container');
    const containerRect = container ? container.getBoundingClientRect() : null;

    let posX = 0;
    let posY = 0;

    if (isTopGreeting) {
      // Top Greeting: placed on the right side of the screen at the top
      posX = Math.max(16, vpW - 440);
      posY = 120;
    } else {
      // Alternating Side Positioning:
      if (side === 'right') {
        // Dock on the right flank: prefer outer gutter if space permits, else right side
        if (containerRect && vpW - containerRect.right >= 430) {
          posX = containerRect.right + 16;
        } else {
          posX = Math.max(16, vpW - 440);
        }
      } else {
        // Dock on the left flank: prefer outer left gutter if space permits, else left side
        if (containerRect && containerRect.left >= 430) {
          posX = containerRect.left - 425 - 16;
        } else {
          posX = 20;
        }
      }

      // Vertical position: centered against target's top area in viewport
      const targetCenterY = targetRect.top + Math.min(180, targetRect.height / 2) - 80;
      posY = Math.max(80, Math.min(targetCenterY, vpH - 280));
    }

    // Safety clamping within screen bounds
    posX = Math.max(12, Math.min(posX, vpW - 430));
    posY = Math.max(75, Math.min(posY, vpH - 260));

    return { posX, posY };
  }

  moveTo(targetElement, side = 'right', isTopGreeting = false, onComplete) {
    if (!targetElement) return;

    // Remove spotlight from previous target
    document.querySelectorAll('.dala-spotlight-active').forEach(el => {
      el.classList.remove('dala-spotlight-active');
    });

    // Add clean Driver.js-style spotlight focus to current target
    targetElement.classList.add('dala-spotlight-active');

    // Handle scroll behavior
    if (isTopGreeting) {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      targetElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }

    // Delay calculation slightly so smooth scroll establishes viewport position
    const delay = isTopGreeting ? 50 : 140;
    setTimeout(() => {
      // If user has not manually dragged DaLa, position automatically on alternating side
      if (!this.isUserCustomPositioned) {
        const { posX, posY } = this.calculateSidePosition(targetElement, side, isTopGreeting);
        this.currentPosX = posX;
        this.currentPosY = posY;

        // Apply directional class for pointer tail
        this.host.classList.remove('dock-right', 'dock-left');
        this.host.classList.add(side === 'right' ? 'dock-right' : 'dock-left');

        this.host.style.transform = `translate3d(${Math.round(posX)}px, ${Math.round(posY)}px, 0)`;
      }

      if (typeof onComplete === 'function') onComplete();
    }, delay);
  }
}

// ==========================================================================
// DALA: TOUR & CHARACTER GUIDE CONTROLLER (Alternating Story + Free Drag)
// ==========================================================================

class DaLaGuideController {
  constructor() {
    this.host = document.getElementById('dala-character-host');
    if (!this.host) return;

    this.unit = document.getElementById('dala-character-unit');
    this.bubble = document.getElementById('dala-speech-bubble');
    this.titleEl = document.getElementById('dala-bubble-title');
    this.msgEl = document.getElementById('dala-bubble-message');
    this.promptEl = document.getElementById('dala-action-text');
    this.moodBadge = document.getElementById('dala-mood-badge');
    this.stepBadge = document.getElementById('dala-step-badge');
    this.btnPrev = document.getElementById('dala-btn-prev');
    this.btnNext = document.getElementById('dala-btn-next');
    this.btnSkip = document.getElementById('dala-btn-skip');
    this.btnMin = document.getElementById('dala-btn-minimize');
    this.reopenPill = document.getElementById('dala-reopen-pill');

    // Finish Celebration Modal Elements
    this.finishModal = document.getElementById('dala-finish-modal');
    this.finishCloseBtn = document.getElementById('dala-finish-close-btn');
    this.finishBadge = document.getElementById('dala-finish-badge');
    this.finishTitle = document.getElementById('dala-finish-title');
    this.finishSubtitle = document.getElementById('dala-finish-subtitle');
    this.finishStatsGrid = document.getElementById('dala-finish-stats-grid');
    this.finishCalloutText = document.getElementById('dala-finish-callout-text');
    this.finishToDetailedBtn = document.getElementById('dala-finish-to-detailed');
    this.finishReplayBtn = document.getElementById('dala-finish-replay');
    this.finishExploreFreeBtn = document.getElementById('dala-finish-explore-free');

    this.positioner = new DaLaPositioner(this.host);
    this.currentStepIndex = 0;
    this.isMinimized = false;
    this.isTourActive = true;
    this.mode = 'story'; // 'story' | 'detailed'

    // Mode 1: Complete Scripted Investigation Tour (13 Steps, Alternating Left and Right)
    this.storySteps = [
      {
        id: "welcome-hero",
        targetSelector: '[data-dala="welcome-hero"]',
        side: "right",
        title: "Welcome to LineageForge! 👋",
        message: "Hey! I'm DaLa, your forensic data detective. Together we'll trace 20 enterprise systems to uncover why our dashboard went dark.",
        prompt: "Click 'Start Investigation ▶' to follow me down to the incident!",
        mood: "👋 Welcome",
        expression: "dala-welcome",
        btnText: "Start Investigation ▶"
      },
      {
        id: "incident",
        targetSelector: '[data-dala="incident"]',
        side: "left",
        title: "The Monday Morning Incident",
        message: "Here is the crime scene: 09:00 AM outage and 4 squads finger-pointing in Slack! Notice the 5-step Domino Trail.",
        prompt: "Review the incident outage and squad chat cards.",
        mood: "🚨 Outage",
        expression: "dala-warning"
      },
      {
        id: "detectives",
        targetSelector: '[data-dala="detectives"]',
        side: "right",
        title: "The 3 Defeated Approaches",
        message: "Git, catalogs, and LLMs all failed on their own! Code tools miss runtime; catalogs stop at the database door; LLMs hallucinate.",
        prompt: "Notice the fatal blind spot in each single-plane approach.",
        mood: "🧐 Sleuth",
        expression: "dala-thinking"
      },
      {
        id: "3plane-model",
        targetSelector: '[data-dala="3plane-model"]',
        side: "left",
        title: "The 3-Plane Evidence Model",
        message: "To eliminate blind spots, LineageForge intercepts proof across 3 independent planes: Static, Metadata, and Runtime.",
        prompt: "Review the modern 3-Plane Evidence convergence model above.",
        mood: "💡 Epiphany",
        expression: "dala-explaining"
      },
      {
        id: "static-evidence",
        targetSelector: '[data-dala="static-evidence"]',
        side: "right",
        title: "Clue #1 • Static Code Plane",
        message: "First clue: Static code analysis! Tree-sitter and SQLGlot inspect code before it even runs in production.",
        prompt: "Waiting for you: Click 'Inspect & Collect Clue #1'!",
        mood: "💻 Code AST",
        expression: "dala-explaining",
        actionId: "static"
      },
      {
        id: "metadata-evidence",
        targetSelector: '[data-dala="metadata-evidence"]',
        side: "left",
        title: "Clue #2 • Metadata Plane",
        message: "Second clue: Catalog fingerprints prove table definitions, schemas, and dbt models from database catalogs.",
        prompt: "Waiting for you: Click 'Query & Collect Clue #2'!",
        mood: "📚 Catalog",
        expression: "dala-explaining",
        actionId: "metadata"
      },
      {
        id: "runtime-evidence",
        targetSelector: '[data-dala="runtime-evidence"]',
        side: "right",
        title: "Clue #3 • Runtime Plane",
        message: "Third clue: Runtime eyewitness! OpenLineage execution events prove what actually executed in production.",
        prompt: "Waiting for you: Click 'Analyze & Collect Clue #3'!",
        mood: "⚡ Runtime",
        expression: "dala-explaining",
        actionId: "runtime"
      },
      {
        id: "evidence-synthesis",
        targetSelector: '[data-dala="evidence-synthesis"]',
        side: "left",
        title: "Evidence Synthesis Workbench",
        message: "All 3 clues synthesized! Watch our live engine weave raw artifacts into verified nodes and edges.",
        prompt: "Check out the 98% confidence fabric formed in the workbench.",
        mood: "🎉 Synthesized",
        expression: "dala-celebrating"
      },
      {
        id: "dataset-identity",
        targetSelector: '[data-dala="dataset-identity"]',
        side: "right",
        title: "Riddle 1 • The Doppelgänger Trap",
        message: "Here's a tricky trap: 4 different tables all named CUSTOMERS! Same name doesn't mean same dataset.",
        prompt: "Click 'Resolve into Canonical 6-Tuple URIs' to eliminate collisions.",
        mood: "🎭 Identity",
        expression: "dala-thinking"
      },
      {
        id: "ghost-riddles",
        targetSelector: '[data-dala="ghost-riddles"]',
        side: "left",
        title: "Riddle 3 • Ghost Dependencies",
        message: "Watch out for Ghost Links! Airflow task ordering is NOT data flow. We only draw arrows where bytes move.",
        prompt: "Toggle perspective to see Control Flow vs Physical Data Flow.",
        mood: "👻 Precision",
        expression: "dala-warning"
      },
      {
        id: "lineage-graph",
        targetSelector: '[data-dala="lineage-graph"]',
        side: "right",
        title: "The Live Node-Edge Lineage Map",
        message: "Welcome to the live data map! Every node is an enterprise asset, and every edge is backed by auditable proof.",
        prompt: "Click any node (like customers or RiskService) to inspect its dossier!",
        mood: "🗺️ Data Map",
        expression: "dala-explaining"
      },
      {
        id: "impact-simulator",
        targetSelector: '[data-dala="impact-simulator"]',
        side: "left",
        title: "Automated Blast Radius Simulator",
        message: "Now let's prevent the outage! The Blast Radius simulator detects breaking schema changes before merge.",
        prompt: "Select Scenario 1 and click 'Simulate Blast Radius ⚡'!",
        mood: "🛡️ PR Gate",
        expression: "dala-explaining"
      },
      {
        id: "audit-dossier",
        targetSelector: '[data-dala="audit-dossier"]',
        side: "right",
        title: "Case Closed • Master Evidence Dossier",
        message: "Mystery solved! 98% confidence end-to-end lineage verified from source code to executive dashboard. Case closed! 🎉",
        prompt: "Investigation complete! Click Finish to see our celebration summary.",
        mood: "🏆 Case Solved",
        expression: "dala-celebrating",
        btnText: "Finish 🎉"
      }
    ];

    // Mode 2: Detailed Technical Blueprint Architecture Tour (11 Sections, Alternating Sides)
    this.detailedSteps = [
      {
        id: "sec-overview",
        targetSelector: '#sec-overview',
        side: "right",
        title: "1. Executive Summary & Stack 🏗️",
        message: "Here is the production architecture! We combine Tree-sitter for code ASTs, SQLGlot for queries, OpenLineage for runtime telemetry, and Neo4j for fast graph queries.",
        prompt: "Review the recommended stack table and architectural rationale.",
        mood: "🏗️ Core Stack",
        expression: "dala-explaining"
      },
      {
        id: "sec-6layers",
        targetSelector: '#sec-6layers',
        side: "left",
        title: "2. 6-Layer Architecture Blueprint 🏛️",
        message: "The platform divides into 6 strict layers: Interception, Normalization, Graph Store, Serving Cache, Applications, and Governance Policy.",
        prompt: "Notice how each layer decouples collection from query serving.",
        mood: "🏛️ 6 Layers",
        expression: "dala-explaining"
      },
      {
        id: "sec-3planes",
        targetSelector: '#sec-3planes',
        side: "right",
        title: "3. The 3 Evidence Planes 📐",
        message: "Technical specification of Static, Catalog, and Runtime planes. Includes reconciliation heuristics and confidence weight scoring matrix.",
        prompt: "Check out the confidence scoring rules table above.",
        mood: "📐 3 Planes",
        expression: "dala-thinking"
      },
      {
        id: "sec-5levels",
        targetSelector: '#sec-5levels',
        side: "left",
        title: "4. The 5 Levels of Lineage 🔬",
        message: "We support 5 levels of granularity: System, Table/Dataset, Column-Level (CLL), Transformation Operation, and Cell-level micro-lineage!",
        prompt: "Inspect the 5 lineage levels table and architectural trade-offs.",
        mood: "🔬 Granularity",
        expression: "dala-explaining"
      },
      {
        id: "sec-schema",
        targetSelector: '#sec-schema',
        side: "right",
        title: "5. Canonical Graph Schema (ERD) 🗄️",
        message: "This ERD defines our unified property graph: Datasets, Fields, Jobs, and Runs, connected by auditable PRODUCED_BY and CONSUMES edges.",
        prompt: "Examine the Mermaid ERD model and edge schema properties.",
        mood: "🗄️ Graph ERD",
        expression: "dala-explaining"
      },
      {
        id: "sec-challenges",
        targetSelector: '#sec-challenges',
        side: "left",
        title: "6. 12 Engineering Challenges 🧩",
        message: "From dynamic SQL interpolation and temporary tables to Kafka streams and schema drift, see the concrete patterns solving each obstacle.",
        prompt: "Review the 12 battle-tested mitigation strategies.",
        mood: "🧩 Challenges",
        expression: "dala-warning"
      },
      {
        id: "sec-tools",
        targetSelector: '#sec-tools',
        side: "right",
        title: "7. Tool Evaluation Matrix ⚖️",
        message: "Objective benchmark comparing LineageForge against OpenLineage, Marquez, Manta, and DataHub across 10 mission-critical enterprise vectors.",
        prompt: "Review the side-by-side technical evaluation table.",
        mood: "⚖️ Evaluation",
        expression: "dala-thinking"
      },
      {
        id: "sec-pipeline",
        targetSelector: '#sec-pipeline',
        side: "left",
        title: "8. Extraction Pipeline & APIs ⚡",
        message: "See the ingestion cadence, streaming webhook hooks, and GraphQL / REST endpoints designed for sub-second CI/CD PR blocking.",
        prompt: "Check out the pipeline specifications and API contracts.",
        mood: "⚡ Extraction",
        expression: "dala-explaining"
      },
      {
        id: "sec-roadmap",
        targetSelector: '#sec-roadmap',
        side: "right",
        title: "9. 90-Day Implementation Roadmap 📅",
        message: "Phased 3-month deployment schedule: Phase 1 Core Foundations (Days 1-30), Phase 2 Breadth & CLL (Days 31-60), and Phase 3 PR Gates & Automation (Days 61-90).",
        prompt: "Review the quarterly deliverables and sprint milestones.",
        mood: "📅 Roadmap",
        expression: "dala-celebrating"
      },
      {
        id: "sec-risks",
        targetSelector: '#sec-risks',
        side: "left",
        title: "10. Risk Management Matrix 🛡️",
        message: "Production risk register detailing failure modes (parser timeouts, volume spikes, schema drift) and their automated fail-safe mitigations.",
        prompt: "Inspect the risk probability, impact, and mitigation controls.",
        mood: "🛡️ Risk Matrix",
        expression: "dala-warning"
      },
      {
        id: "sec-glossary",
        targetSelector: '#sec-glossary',
        side: "right",
        title: "11. Comprehensive Glossary 📖",
        message: "Standardized enterprise terminology: CLL, AST, Facets, True Lineage, and Provenance. You're fully equipped with enterprise data lineage mastery!",
        prompt: "Click 'Finish Blueprint 🎓' to complete the blueprint overview.",
        mood: "🏆 Mastered",
        expression: "dala-celebrating",
        btnText: "Finish Blueprint 🎓"
      }
    ];

    this.bindEvents();
    this.bindInteractiveStoryHooks();
    this.enableFreeHandDragging();

    // On initial page load: stay at top and greet user
    setTimeout(() => {
      if (state.activeMode === 'story') {
        this.goToStep(0);
      }
    }, 300);
  }

  get steps() {
    return this.mode === 'detailed' ? this.detailedSteps : this.storySteps;
  }

  setExpression(exprClass) {
    this.host.classList.remove(
      'dala-welcome',
      'dala-explaining',
      'dala-thinking',
      'dala-warning',
      'dala-celebrating',
      'dala-idle'
    );
    if (exprClass) {
      this.host.classList.add(exprClass);
    }
  }

  goToStep(index) {
    if (index < 0) index = 0;
    if (index >= this.steps.length) index = this.steps.length - 1;
    this.currentStepIndex = index;
    const step = this.steps[index];

    const target = document.querySelector(step.targetSelector);
    if (!target) return;

    // Reset user manual position on step transition so DaLa travels with content
    this.positioner.isUserCustomPositioned = false;

    if (this.titleEl) this.titleEl.textContent = step.title;
    if (this.msgEl) this.msgEl.textContent = step.message;
    if (this.promptEl) this.promptEl.textContent = step.prompt;
    if (this.moodBadge) this.moodBadge.textContent = step.mood;
    if (this.stepBadge) this.stepBadge.textContent = `${index + 1} / ${this.steps.length}`;

    if (this.btnPrev) {
      this.btnPrev.style.display = index === 0 ? 'none' : 'inline-block';
    }
    if (this.btnNext) {
      if (step.btnText) {
        this.btnNext.innerHTML = `<span>${step.btnText}</span>`;
      } else {
        this.btnNext.innerHTML = '<span>Next</span> ▶';
      }
    }

    this.setExpression(step.expression || 'dala-explaining');
    this.host.classList.remove('hidden');
    this.positioner.moveTo(target, step.side, step.id === 'welcome-hero');
  }

  next() {
    if (this.currentStepIndex < this.steps.length - 1) {
      this.goToStep(this.currentStepIndex + 1);
    } else {
      this.completeTour();
    }
  }

  prev() {
    if (this.currentStepIndex > 0) {
      this.goToStep(this.currentStepIndex - 1);
    }
  }

  skip() {
    this.isTourActive = false;
    this.host.classList.add('hidden');
    document.querySelectorAll('.dala-spotlight-active').forEach(el => {
      el.classList.remove('dala-spotlight-active');
    });
    if (this.reopenPill) {
      this.reopenPill.style.display = 'flex';
    }
  }

  completeTour() {
    this.setExpression('dala-celebrating');
    if (this.msgEl) {
      this.msgEl.textContent = this.mode === 'story'
        ? "You've mastered the 3 evidence planes and end-to-end data lineage! Feel free to explore freely or rerun scenarios."
        : "You've completed the Detailed Architecture Blueprint overview!";
    }
    if (this.promptEl) {
      this.promptEl.textContent = "Tour completed successfully. 🌟";
    }
    this.showFinishModal();
  }

  showFinishModal() {
    if (!this.finishModal) return;

    if (this.mode === 'story') {
      if (this.finishBadge) this.finishBadge.textContent = '🏆 MISSION ACCOMPLISHED • CASE CLOSED';
      if (this.finishTitle) this.finishTitle.textContent = 'Investigation Successfully Solved!';
      if (this.finishSubtitle) this.finishSubtitle.textContent = '98% Confidence End-to-End Lineage Fabric Established Across All 20 Enterprise Systems';
      if (this.finishStatsGrid) {
        this.finishStatsGrid.innerHTML = `
          <div class="dala-stat-card">
            <div class="dala-stat-num">3 / 3</div>
            <div class="dala-stat-label">Evidence Planes Intercepted</div>
            <div class="dala-stat-sub">Static AST • Catalog • Runtime</div>
          </div>
          <div class="dala-stat-card">
            <div class="dala-stat-num">5 Hops</div>
            <div class="dala-stat-label">Domino Outage Trail Mapped</div>
            <div class="dala-stat-sub">Java API ➔ Snowflake ➔ Power BI</div>
          </div>
          <div class="dala-stat-card">
            <div class="dala-stat-num">100%</div>
            <div class="dala-stat-label">Blast Radius Contained</div>
            <div class="dala-stat-sub">Automated CI/CD PR Gate Protected</div>
          </div>
        `;
      }
      if (this.finishCalloutText) {
        this.finishCalloutText.innerHTML = '<strong>Next Step:</strong> Explore the complete <strong>Detailed Technical Blueprint</strong> to inspect the 6-layer architecture, ERD schema, and 90-day implementation roadmap!';
      }
      if (this.finishToDetailedBtn) {
        this.finishToDetailedBtn.innerHTML = '<span>📐 Explore Detailed Blueprint</span>';
      }
      if (this.finishReplayBtn) {
        this.finishReplayBtn.innerHTML = '<span>↺ Replay Investigation</span>';
      }
    } else {
      if (this.finishBadge) this.finishBadge.textContent = '🎓 ARCHITECTURE MASTERED • BLUEPRINT COMPLETE';
      if (this.finishTitle) this.finishTitle.textContent = 'Enterprise Blueprint Mastered!';
      if (this.finishSubtitle) this.finishSubtitle.textContent = 'You now hold the complete production-grade blueprint to architect, deploy, and govern enterprise data lineage.';
      if (this.finishStatsGrid) {
        this.finishStatsGrid.innerHTML = `
          <div class="dala-stat-card">
            <div class="dala-stat-num">6 Layers</div>
            <div class="dala-stat-label">Engine Architecture</div>
            <div class="dala-stat-sub">Interception to Governance</div>
          </div>
          <div class="dala-stat-card">
            <div class="dala-stat-num">5 Levels</div>
            <div class="dala-stat-label">Lineage Granularity</div>
            <div class="dala-stat-sub">Table to Column & Cell</div>
          </div>
          <div class="dala-stat-card">
            <div class="dala-stat-num">90 Days</div>
            <div class="dala-stat-label">Enterprise Roadmap</div>
            <div class="dala-stat-sub">Phased Rollout Schedule</div>
          </div>
        `;
      }
      if (this.finishCalloutText) {
        this.finishCalloutText.innerHTML = '<strong>Great job!</strong> You can revisit the <strong>Story Mode Investigation</strong> to experience the interactive forensic clues simulation anytime!';
      }
      if (this.finishToDetailedBtn) {
        this.finishToDetailedBtn.innerHTML = '<span>🕵️‍♂️ Revisit Investigation Story</span>';
      }
      if (this.finishReplayBtn) {
        this.finishReplayBtn.innerHTML = '<span>↺ Replay Blueprint Tour</span>';
      }
    }

    this.finishModal.style.display = 'flex';
    this.finishModal.setAttribute('aria-hidden', 'false');
  }

  hideFinishModal() {
    if (this.finishModal) {
      this.finishModal.style.display = 'none';
      this.finishModal.setAttribute('aria-hidden', 'true');
    }
  }

  switchMode(newMode) {
    this.mode = newMode;
    this.currentStepIndex = 0;
    this.isTourActive = true;
    this.host.classList.remove('hidden', 'minimized');
    if (this.reopenPill) this.reopenPill.style.display = 'none';
    this.goToStep(0);
  }

  jumpToSection(sectionId) {
    if (this.mode !== 'detailed') return;
    const stepIdx = this.detailedSteps.findIndex(s => s.targetSelector === '#' + sectionId);
    if (stepIdx !== -1 && stepIdx !== this.currentStepIndex) {
      this.goToStep(stepIdx);
    }
  }

  toggleMinimize() {
    this.isMinimized = !this.isMinimized;
    this.host.classList.toggle('minimized', this.isMinimized);
    if (this.btnMin) {
      this.btnMin.textContent = this.isMinimized ? '□' : '─';
    }
  }

  resumeTour() {
    this.isTourActive = true;
    if (this.reopenPill) this.reopenPill.style.display = 'none';
    this.host.classList.remove('hidden', 'minimized');
    this.goToStep(this.currentStepIndex);
  }

  reactWithPraise(temporaryMessage, nextStepDelayMs = 1400) {
    if (this.msgEl) this.msgEl.textContent = temporaryMessage;
    this.setExpression('dala-celebrating');

    if (nextStepDelayMs > 0) {
      setTimeout(() => {
        this.next();
      }, nextStepDelayMs);
    }
  }

  onNodeSelected(node) {
    if (!node || !this.isTourActive) return;
    if (this.currentStepIndex === 10) {
      this.setExpression('dala-explaining');
      if (this.msgEl) {
        this.msgEl.textContent = `Inspecting ${node.icon} ${node.title} (${node.tech}). Provenance and downstream blast radius updated in the drawer!`;
      }
      if (this.promptEl) {
        this.promptEl.textContent = `Owner: ${node.owner} • Confidence: ${node.confidence}`;
      }
    }
  }

  enableFreeHandDragging() {
    let isDragging = false;
    let startX = 0;
    let startY = 0;
    let initX = 0;
    let initY = 0;
    let hasMoved = false;
    this.justDragged = false;

    const dragElements = [this.unit, this.bubble].filter(Boolean);

    const onPointerDown = (e) => {
      // Don't drag if interacting with buttons, controls, or links
      if (e.target.closest('button') || e.target.closest('a') || e.target.closest('input')) return;

      isDragging = true;
      hasMoved = false;
      startX = e.clientX;
      startY = e.clientY;
      initX = this.positioner.currentPosX || 0;
      initY = this.positioner.currentPosY || 0;

      this.host.classList.add('is-dragging');
      document.body.style.userSelect = 'none';

      window.addEventListener('pointermove', onPointerMove, { passive: false });
      window.addEventListener('pointerup', onPointerUp, { passive: false });
    };

    const onPointerMove = (e) => {
      if (!isDragging) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;

      if (Math.abs(dx) > 4 || Math.abs(dy) > 4) {
        hasMoved = true;
      }

      const hostW = this.host.offsetWidth || 440;
      const hostH = this.host.offsetHeight || 260;
      const newX = Math.max(8, Math.min(initX + dx, window.innerWidth - hostW - 8));
      const newY = Math.max(8, Math.min(initY + dy, window.innerHeight - hostH - 8));

      this.positioner.currentPosX = newX;
      this.positioner.currentPosY = newY;
      this.host.style.transform = `translate3d(${Math.round(newX)}px, ${Math.round(newY)}px, 0)`;

      e.preventDefault();
    };

    const onPointerUp = () => {
      if (!isDragging) return;
      isDragging = false;
      this.host.classList.remove('is-dragging');
      document.body.style.userSelect = '';

      window.removeEventListener('pointermove', onPointerMove);
      window.removeEventListener('pointerup', onPointerUp);

      if (hasMoved) {
        this.positioner.isUserCustomPositioned = true;
        this.justDragged = true;
        setTimeout(() => { this.justDragged = false; }, 150);
      }
    };

    dragElements.forEach(el => {
      el.addEventListener('pointerdown', onPointerDown);
    });
  }

  bindEvents() {
    if (this.btnNext) this.btnNext.addEventListener('click', () => this.next());
    if (this.btnPrev) this.btnPrev.addEventListener('click', () => this.prev());
    if (this.btnSkip) this.btnSkip.addEventListener('click', () => this.skip());
    if (this.btnMin) this.btnMin.addEventListener('click', () => this.toggleMinimize());
    if (this.unit) this.unit.addEventListener('click', () => {
      if (this.justDragged) return;
      this.toggleMinimize();
    });
    if (this.reopenPill) this.reopenPill.addEventListener('click', () => this.resumeTour());

    // Finish Celebration Modal Listeners
    if (this.finishCloseBtn) {
      this.finishCloseBtn.addEventListener('click', () => this.hideFinishModal());
    }
    if (this.finishExploreFreeBtn) {
      this.finishExploreFreeBtn.addEventListener('click', () => this.hideFinishModal());
    }
    if (this.finishModal) {
      this.finishModal.addEventListener('click', (e) => {
        if (e.target === this.finishModal) this.hideFinishModal();
      });
    }
    if (this.finishToDetailedBtn) {
      this.finishToDetailedBtn.addEventListener('click', () => {
        this.hideFinishModal();
        if (this.mode === 'story') {
          const btnDetailed = document.getElementById('btn-mode-detailed');
          if (btnDetailed) btnDetailed.click();
        } else {
          const btnStory = document.getElementById('btn-mode-story');
          if (btnStory) btnStory.click();
        }
      });
    }
    if (this.finishReplayBtn) {
      this.finishReplayBtn.addEventListener('click', () => {
        this.hideFinishModal();
        this.goToStep(0);
      });
    }

    let resizeTimer = null;
    window.addEventListener('resize', () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        if (this.isTourActive && !this.positioner.isUserCustomPositioned) {
          const currentStep = this.steps[this.currentStepIndex];
          const currentTarget = document.querySelector(currentStep?.targetSelector);
          if (currentTarget) this.positioner.moveTo(currentTarget, currentStep.side, currentStep.id === 'welcome-hero');
        }
      }, 150);
    });
  }

  bindInteractiveStoryHooks() {
    const btnClue1 = document.getElementById('btn-clue-static');
    if (btnClue1) {
      btnClue1.addEventListener('click', () => {
        if (this.currentStepIndex === 4) {
          this.reactWithPraise("Nice work! Clue #1 collected — Tree-sitter AST extracted the SQL query!");
        }
      });
    }

    const btnClue2 = document.getElementById('btn-clue-metadata');
    if (btnClue2) {
      btnClue2.addEventListener('click', () => {
        if (this.currentStepIndex === 5) {
          this.reactWithPraise("Awesome! Clue #2 collected — Catalog fingerprint confirmed dbt dependency!");
        }
      });
    }

    const btnClue3 = document.getElementById('btn-clue-runtime');
    if (btnClue3) {
      btnClue3.addEventListener('click', () => {
        if (this.currentStepIndex === 6) {
          this.reactWithPraise("Great detective work! Clue #3 collected — OpenLineage verified live production execution!");
        }
      });
    }

    const btnSimAll = document.getElementById('btn-simulate-all-clues');
    if (btnSimAll) {
      btnSimAll.addEventListener('click', () => {
        if (this.currentStepIndex >= 4 && this.currentStepIndex <= 6) {
          this.reactWithPraise("All 3 planes running simultaneously! Advancing to Evidence Synthesis...", 1200);
          setTimeout(() => this.goToStep(7), 1300);
        }
      });
    }

    const btnResolve = document.getElementById('btn-resolve-identities');
    if (btnResolve) {
      btnResolve.addEventListener('click', () => {
        if (this.currentStepIndex === 8) {
          this.reactWithPraise("Brilliant! All 4 datasets disambiguated into canonical 6-tuple URIs!");
        }
      });
    }

    const btnSimulate = document.getElementById('btn-run-simulation');
    if (btnSimulate) {
      btnSimulate.addEventListener('click', () => {
        if (this.currentStepIndex === 11) {
          this.reactWithPraise("Simulation complete! 4 downstream assets flagged and PR Gate blocked! Advancing to Final Dossier...", 1500);
        }
      });
    }
  }
}

