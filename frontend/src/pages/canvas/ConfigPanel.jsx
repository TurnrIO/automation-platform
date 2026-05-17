import { useState, useEffect, useRef, useMemo, useCallback, useContext } from "react";
import { NODE_DEFS, GROUPS } from "./nodeDefs.js";
import { NOTE_COLORS } from "./StickyNote.jsx";
import { ValidationContext } from "./CanvasApp.jsx";

// ── JsonSchemaTree ─────────────────────────────────────────────────────────────
// Renders a JSON value as a collapsible schema tree with drag-to-insert support.
export function JsonSchemaTree({ data, depth, sourceNodeId, fieldPath }) {
  depth     = depth     || 0;
  fieldPath = fieldPath || null;
  const [collapsed, setCollapsed] = useState(depth > 1);

  const template = (sourceNodeId && fieldPath) ? `{{${sourceNodeId}.${fieldPath}}}` : null;

  function startDrag(e) {
    if (!template) return;
    e.dataTransfer.setData("text/plain", template);
    e.dataTransfer.effectAllowed = "copy";
    e.stopPropagation();
  }

  function Draggable({ children }) {
    if (!template) return children;
    return (
      <span className="nio-drag-row" draggable onDragStart={startDrag} title={`Drag to insert: ${template}`}>
        <span className="nio-drag-handle" title={template}>⠿</span>
        {children}
      </span>
    );
  }

  if (data === null || data === undefined)
    return <Draggable><span className="nio-tree-null">null</span></Draggable>;
  if (typeof data === "boolean")
    return <Draggable><span className="nio-tree-bool">{String(data)}</span></Draggable>;
  if (typeof data === "number")
    return <Draggable><span className="nio-tree-num">{String(data)}</span></Draggable>;
  if (typeof data === "string") {
    const display = data.length > 120 ? data.slice(0, 120) + "…" : data;
    return <Draggable><span className="nio-tree-str">"{display}"</span></Draggable>;
  }

  if (Array.isArray(data)) {
    if (data.length === 0)
      return <Draggable><span style={{ color: "#94a3b8" }}>[ ] (empty)</span></Draggable>;
    const preview = `[ ${data.length} item${data.length > 1 ? "s" : ""} ]`;
    return (
      <div>
        <span className="nio-drag-row" draggable={!!template} onDragStart={startDrag}>
          {template && <span className="nio-drag-handle" title={template}>⠿</span>}
          <span className="nio-tree-expand" onClick={() => setCollapsed(c => !c)}>
            {collapsed ? "▶" : "▼"} {preview}
          </span>
        </span>
        {!collapsed && (
          <div className="nio-tree-indent">
            {data.slice(0, 30).map((item, i) => {
              const childPath = fieldPath ? `${fieldPath}[${i}]` : `[${i}]`;
              return (
                <div key={i} style={{ marginBottom: 2 }}>
                  <span style={{ color: "#64748b", fontSize: 9 }}>[{i}]</span>{" "}
                  <JsonSchemaTree data={item} depth={depth + 1} sourceNodeId={sourceNodeId} fieldPath={childPath} />
                </div>
              );
            })}
            {data.length > 30 && (
              <div style={{ color: "#64748b", fontSize: 9, fontStyle: "italic" }}>
                …{data.length - 30} more
              </div>
            )}
          </div>
        )}
      </div>
    );
  }

  if (typeof data === "object") {
    const keys = Object.keys(data);
    if (keys.length === 0)
      return <Draggable><span style={{ color: "#94a3b8" }}>{"{ }"} (empty)</span></Draggable>;
    const preview = `{ ${keys.slice(0, 3).join(", ")}${keys.length > 3 ? "…" : ""} }`;
    return (
      <div>
        <span className="nio-drag-row" draggable={!!template} onDragStart={startDrag}>
          {template && <span className="nio-drag-handle" title={template}>⠿</span>}
          <span className="nio-tree-expand" onClick={() => setCollapsed(c => !c)}>
            {collapsed ? "▶" : "▼"} {preview}
          </span>
        </span>
        {!collapsed && (
          <div className="nio-tree-indent">
            {keys.map(k => {
              const childPath = fieldPath ? `${fieldPath}.${k}` : k;
              return (
                <div
                  key={k}
                  className="nio-drag-row"
                  draggable={!!sourceNodeId}
                  onDragStart={e => {
                    e.dataTransfer.setData("text/plain", `{{${sourceNodeId}.${childPath}}}`);
                    e.dataTransfer.effectAllowed = "copy";
                    e.stopPropagation();
                  }}
                  style={{ marginBottom: 2, lineHeight: 1.7 }}
                >
                  {sourceNodeId && (
                    <span className="nio-drag-handle" title={`{{${sourceNodeId}.${childPath}}}`}>⠿</span>
                  )}
                  <div style={{ flex: 1 }}>
                    <span className="nio-tree-key">{k}:{" "}</span>
                    <JsonSchemaTree data={data[k]} depth={depth + 1} sourceNodeId={sourceNodeId} fieldPath={childPath} />
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    );
  }
  return <Draggable><span style={{ color: "#94a3b8" }}>{String(data)}</span></Draggable>;
}

// ── NioBody ────────────────────────────────────────────────────────────────────
const NIO_DISPLAY_LINES = 500;

export function NioBody({ data, view, sourceNodeId, isTruncated }) {
  if (isTruncated) {
    const kb = data.__size ? Math.round(data.__size / 1024) : "?";
    return (
      <div style={{ color: "#64748b", fontSize: 11, padding: "6px 0" }}>
        <div style={{ color: "#f59e0b", marginBottom: 4 }}>⚠ Payload too large to display ({kb} KB)</div>
        <div style={{ fontSize: 10, color: "#475569" }}>
          Exceeds the 5 MB display limit. Use a Transform or Run Script node to extract only the fields you need.
        </div>
      </div>
    );
  }
  if (data === undefined)
    return (
      <div style={{ color: "#475569", fontSize: 11, fontStyle: "italic" }}>
        No data yet — run the flow first
      </div>
    );
  if (view === "schema")
    return (
      <div style={{ fontSize: 11, lineHeight: 1.8 }}>
        {sourceNodeId && (
          <div style={{ fontSize: 9, color: "#64748b", marginBottom: 6, fontStyle: "italic" }}>
            ⠿ drag any field into a config input to insert a template reference
          </div>
        )}
        <JsonSchemaTree data={data} depth={0} sourceNodeId={sourceNodeId} fieldPath={null} />
      </div>
    );

  const fullStr    = data === null ? "null" : (typeof data === "string" ? data : JSON.stringify(data, null, 2));
  const lines      = fullStr.split("\n");
  const capped     = lines.length > NIO_DISPLAY_LINES;
  const displayed  = capped ? lines.slice(0, NIO_DISPLAY_LINES).join("\n") : fullStr;
  function copyFull() { navigator.clipboard.writeText(fullStr).catch(() => {}); }

  return (
    <div>
      <pre className="nio-json">{displayed}</pre>
      {capped && (
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "5px 8px", background: "#0f1117", borderTop: "1px solid #2a2d3e",
          fontSize: 10, color: "#64748b",
        }}>
          <span>Showing {NIO_DISPLAY_LINES} of {lines.length} lines ({Math.round(fullStr.length / 1024)} KB)</span>
          <button
            onClick={copyFull}
            style={{ background: "#2a2d3e", border: "none", color: "#94a3b8", borderRadius: 4, padding: "2px 8px", cursor: "pointer", fontSize: 10 }}
          >
            ⎘ Copy all
          </button>
        </div>
      )}
    </div>
  );
}

// ── NodeIOPanel ────────────────────────────────────────────────────────────────
export function NodeIOPanel({ runInput, runOutput, runStatus, runDurationMs, runAttempts, nodeId, upstreamNodeId, nodeLabel }) {
  const [side, setSide]         = useState("output");
  const [view, setView]         = useState("schema");
  const [expanded, setExpanded] = useState(false);

  const data        = side === "input" ? runInput : runOutput;
  const sourceNodeId = side === "output" ? nodeId : upstreamNodeId;

  const isErr        = runStatus === "err";
  const isOk         = runStatus === "ok";
  const statusColor  = isOk ? "#4ade80" : isErr ? "#f87171" : "#94a3b8";
  const statusLabel  = isOk ? "Success"  : isErr ? "Error"   : "Skipped";
  const isTruncated  = !!(data && data.__truncated);

  function Tabs() {
    return (
      <>
        <div className="nio-side-tabs">
          <button className={`nio-tab${side === "input"  ? " nio-tab-active" : ""}`} onClick={() => setSide("input")}>INPUT</button>
          <button className={`nio-tab${side === "output" ? " nio-tab-active" : ""}`} onClick={() => setSide("output")}>OUTPUT</button>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {runDurationMs !== undefined && (
            <span style={{ fontSize: 9, color: "#64748b" }}>
              {runDurationMs}ms{runAttempts > 1 ? ` · ${runAttempts}×` : ""}
            </span>
          )}
          {runStatus && (
            <span style={{
              fontSize: 9, fontWeight: 700, color: statusColor,
              background: isOk ? "#14532d33" : isErr ? "#7f1d1d33" : "#1e293b",
              padding: "2px 7px", borderRadius: 4,
            }}>
              {statusLabel}
            </span>
          )}
          <button className="nio-expand-btn" onClick={() => setExpanded(true)} title="Expand to full view">⛶</button>
        </div>
      </>
    );
  }

  function ViewTabs() {
    return (
      <div className="nio-view-tabs">
        <button className={`nio-view-tab${view === "schema" ? " active" : ""}`} onClick={() => setView("schema")}>Schema</button>
        <button className={`nio-view-tab${view === "json"   ? " active" : ""}`} onClick={() => setView("json")}>JSON</button>
      </div>
    );
  }

  return (
    <>
      <div className="nio-panel">
        <div className="nio-header"><Tabs /></div>
        <ViewTabs />
        <div className="nio-content">
          <NioBody data={data} view={view} sourceNodeId={sourceNodeId} isTruncated={isTruncated} />
        </div>
      </div>

      {expanded && (
        <div className="nio-modal-overlay" aria-hidden="true" onClick={e => { if (e.target === e.currentTarget) setExpanded(false); }}>
          <div className="nio-modal" role="dialog" aria-modal="true" aria-label="I/O Inspector">
            <div className="nio-modal-hdr">
              <div className="nio-side-tabs"><Tabs /></div>
              <button className="nio-modal-close" aria-label="Close" onClick={() => setExpanded(false)}>✕</button>
            </div>
            <ViewTabs />
            <div className="nio-modal-body">
              <div className="nio-content" style={{ maxHeight: "calc(85vh - 90px)" }}>
                <NioBody data={data} view={view} sourceNodeId={sourceNodeId} isTruncated={isTruncated} />
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

// ── ConfigPanel ────────────────────────────────────────────────────────────────
export function ConfigPanel({ node, onChange, onDelete, edges }) {
  if (!node) {
    return (
      <div className="config-panel">
        <div className="panel-header"><h3>Node Config</h3></div>
        <div className="panel-body">
          <div className="config-empty">
            <div style={{ fontSize: 28, marginBottom: 10 }}>⬅</div>
            Click any node to configure it.<br />
            Drag from the palette to add.<br /><br />
            <span style={{ fontSize: 10, color: "#4b5563" }}>
              Use <code style={{ color: "#a78bfa" }}>{"{{node_id.field}}"}</code><br />
              to pass data between nodes.
            </span>
          </div>
        </div>
      </div>
    );
  }

  const validationMap = useContext(ValidationContext);
  const nodeIssueList = validationMap.get(node.id) || [];

  const isNote      = node.data.type === "note";
  const isTrigger   = node.data.type?.startsWith("trigger.");
  const def         = NODE_DEFS[node.data.type] || { label: node.data.type, icon: "?", color: "#475569", fields: [] };
  const isDisabled  = !!node.data.disabled;
  const runStatus   = node.data._runStatus;
  const runOutput   = node.data._runOutput;
  const runInput    = node.data._runInput;
  const runDurationMs = node.data._runDurationMs;
  const runAttempts = node.data._runAttempts;
  const retryMax    = node.data.retry_max  ?? 0;
  const retryDelay  = node.data.retry_delay ?? 5;
  const failMode    = node.data.fail_mode  || "abort";

  const upstreamNodeId = useMemo(() => {
    if (!edges || !node) return null;
    const inEdge = edges.find(e => e.target === node.id);
    return inEdge ? inEdge.source : null;
  }, [edges, node?.id]);

  // Drop handler factory for config input/textarea fields
  function dropProps(fieldKey, currentVal) {
    return {
      onDragOver:  e => { e.preventDefault(); e.currentTarget.classList.add("drag-over"); },
      onDragLeave: e => e.currentTarget.classList.remove("drag-over"),
      onDrop: e => {
        e.preventDefault();
        e.currentTarget.classList.remove("drag-over");
        const tmpl = e.dataTransfer.getData("text/plain");
        if (!tmpl) return;
        const el    = e.currentTarget;
        const start = el.selectionStart ?? (currentVal || "").length;
        const end   = el.selectionEnd   ?? start;
        const val   = currentVal || "";
        const newVal = val.slice(0, start) + tmpl + val.slice(end);
        update(fieldKey, newVal);
        setTimeout(() => { try { el.selectionStart = el.selectionEnd = start + tmpl.length; } catch (e) {} }, 0);
      },
    };
  }

  function update(key, val)    { onChange(node.id, { ...node.data, config: { ...node.data.config, [key]: val } }); }
  function updateLabel(val)    { onChange(node.id, { ...node.data, label: val }); }
  function updateRetryMax(val)  { onChange(node.id, { ...node.data, retry_max:   parseInt(val) || 0 }); }
  function updateRetryDelay(val){ onChange(node.id, { ...node.data, retry_delay: parseInt(val) || 5 }); }
  function updateFailMode(val)  { onChange(node.id, { ...node.data, fail_mode:   val }); }
  function copyId()             { navigator.clipboard.writeText(node.id).catch(() => {}); }
  function toggleDisabled()     { onChange(node.id, { ...node.data, disabled: !isDisabled }); }

  return (
    <div className="config-panel">
      <div className="panel-header">
        <h3>{def.icon} {def.label}</h3>
      </div>
      <div className="panel-body">

        {/* Node ID badge */}
        <div className="node-id-badge" onClick={copyId} title="Click to copy node ID">
          <div>
            <div style={{ fontSize: 10, color: "#64748b", marginBottom: 2 }}>NODE ID (for templates)</div>
            <code>{node.id}</code>
          </div>
          <span className="copy-hint">📋 copy</span>
        </div>

        {/* Validation issues */}
        {nodeIssueList.length > 0 && (
          <div style={{ margin: "6px 0 2px", display: "flex", flexDirection: "column", gap: 3 }}>
            {nodeIssueList.map((iss, i) => (
              <div key={i} style={{
                display: "flex", gap: 6, alignItems: "flex-start",
                background: "#1c1400", border: "1px solid #fbbf2433",
                borderRadius: 5, padding: "4px 8px", fontSize: 11,
              }}>
                <span style={{ color: "#fbbf24", flexShrink: 0 }}>⚠</span>
                <span style={{ color: "#fcd34d", lineHeight: 1.4 }}>{iss.msg}</span>
              </div>
            ))}
          </div>
        )}

        {/* Disable / Enable toggle */}
        {!isNote && (
          <div className="disable-toggle-row">
            <span className="disable-toggle-label">{isDisabled ? "⏸ Node disabled" : "▶ Node enabled"}</span>
            <label className="toggle-switch">
              <input type="checkbox" checked={!isDisabled} onChange={toggleDisabled} />
              <div className="toggle-track" />
              <div className="toggle-thumb" />
            </label>
          </div>
        )}

        {/* Label */}
        {!isNote && (
          <>
            <div className="section-divider">Label</div>
            <div className="field-group">
              <input
                className="field-input"
                placeholder={def.label}
                value={node.data.label || ""}
                onChange={e => updateLabel(e.target.value)}
              />
            </div>
          </>
        )}

        {/* Fields */}
        {def.fields.length > 0 && <div className="section-divider">Config</div>}
        {def.fields.map(f => {
          const cfgVal = (node.data.config || {})[f.k] || "";
          const dp = (!f.secret && f.type !== "select") ? dropProps(f.k, cfgVal) : {};
          return (
            <div className="field-group" key={f.k}>
              <div className="field-label">
                {f.l}
                {f.secret && <span className="field-hint">🔒 sensitive</span>}
              </div>
              {f.type === "select" ? (
                <select className="field-input" value={cfgVal || f.options[0]} onChange={e => update(f.k, e.target.value)}>
                  {f.options.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              ) : f.textarea ? (
                <textarea
                  className={`field-input${f.mono ? " mono" : ""}`}
                  placeholder={f.ph}
                  value={cfgVal}
                  onChange={e => update(f.k, e.target.value)}
                  rows={4}
                  {...dp}
                />
              ) : (
                <input
                  className={`field-input${f.mono ? " mono" : ""}`}
                  placeholder={f.ph}
                  type={f.secret ? "password" : "text"}
                  value={cfgVal}
                  onChange={e => update(f.k, e.target.value)}
                  {...dp}
                />
              )}
              {f.secret && (
                <span className="secret-hint">💡 Or use {"{{creds.your-credential-name}}"}</span>
              )}
            </div>
          );
        })}

        {/* ── Node-type-specific hint panels ── */}
        {node.data.type === "action.condition" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            Expression must evaluate to a Python bool.<br />
            <div style={{ color: "#059669", marginTop: 4 }}>● True handle → nodes that run when condition is met</div>
            <div style={{ color: "#dc2626" }}>● False handle → nodes that run when condition is not met</div>
            <div style={{ marginTop: 4 }}>
              Nodes reachable from the un-taken handle are <strong>skipped</strong> automatically.<br />
              Nodes after both branches converge always run.
            </div>
            <div style={{ marginTop: 4 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ result: bool, input: ... }"}</code></div>
          </div>
        )}
        {node.data.type === "action.loop" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6 }}>
            <div style={{ color: "#7e22ce", marginBottom: 3 }}>● Body → runs per item</div>
            <div style={{ color: "#64748b", marginBottom: 4 }}>● Done → after all iterations</div>
            <div>Each body node receives the current item as input.</div>
          </div>
        )}
        {node.data.type === "action.filter" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6 }}>
            Output: <code style={{ color: "#a78bfa" }}>{"{ items: [...], count: N }"}</code><br />
            Use <code style={{ color: "#a78bfa" }}>item</code> in the expression.
          </div>
        )}
        {node.data.type === "action.llm_call" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6 }}>
            Output: <code style={{ color: "#a78bfa" }}>{"{ response, model, tokens }"}</code><br />
            Set <code style={{ color: "#a78bfa" }}>api_base</code> for Groq, Together AI, Ollama etc.
          </div>
        )}
        {node.data.type === "action.slack" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>💡 Credential shortcut</div>
            Create a <strong>Slack Incoming Webhook</strong> credential, then set <strong>Slack Credential</strong> to its name — the webhook URL is filled automatically.<br />
            The Webhook URL field overrides the credential if both are set.<br />
            <div style={{ marginTop: 4 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ sent: true, message }"}</code></div>
          </div>
        )}
        {node.data.type === "action.call_graph" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6 }}>
            Runs another flow as a sub-routine.<br />
            Output: the sub-flow's final context dict.<br />
            Max nesting depth: <code style={{ color: "#a78bfa" }}>5</code> levels.
          </div>
        )}
        {node.data.type === "action.airtable" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#f59e0b", fontWeight: 600, marginBottom: 4 }}>🟧 Airtable</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>api_key</code> (personal access token, starts with <code style={{ color: "#a78bfa" }}>pat…</code>) and <code style={{ color: "#a78bfa" }}>base_id</code> (starts with <code style={{ color: "#a78bfa" }}>app…</code>).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>list_records</code><span>→ {"{ records[], count }"} — all pages fetched</span>
              <code style={{ color: "#60a5fa" }}>search</code><span>→ same with filterByFormula + sort</span>
              <code style={{ color: "#a78bfa" }}>create_record</code><span>→ {"{ id, fields, created: true }"}</span>
              <code style={{ color: "#f59e0b" }}>update_record</code><span>→ PATCH — set record_id + fields_json</span>
              <code style={{ color: "#94a3b8" }}>upsert_record</code><span>→ create or update on upsert_field match</span>
              <code style={{ color: "#f87171" }}>delete_record</code><span>→ {"{ id, deleted: true }"}</span>
            </div>
          </div>
        )}
        {node.data.type === "action.redis" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#dc2626", fontWeight: 600, marginBottom: 4 }}>🟥 Redis Operations</div>
            Create a <strong>Redis</strong> credential (url field = <code style={{ color: "#a78bfa" }}>redis://host:6379/0</code>) → set <strong>Redis Credential</strong> to its name.<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>get/set/del</code><span>→ string key ops</span>
              <code style={{ color: "#60a5fa" }}>incr/decr</code><span>→ atomic counters</span>
              <code style={{ color: "#a78bfa" }}>lpush/rpop</code><span>→ queue (list head/tail)</span>
              <code style={{ color: "#f59e0b" }}>sadd/smembers</code><span>→ set ops</span>
              <code style={{ color: "#94a3b8" }}>hset/hget/hgetall</code><span>→ hash ops</span>
              <code style={{ color: "#64748b" }}>expire/ttl/exists</code><span>→ key metadata</span>
            </div>
          </div>
        )}
        {node.data.type === "action.run_script" && (
          <div style={{ background: "#2d0a0a", border: "1px solid #7f1d1d", borderRadius: 6, padding: "8px 10px", fontSize: 10, marginTop: 6 }}>
            <div style={{ color: "#f87171", fontWeight: 700, marginBottom: 4 }}>⚠️ Danger — arbitrary code execution</div>
            <div style={{ color: "#fca5a5", lineHeight: 1.7 }}>
              This node runs Python via <code style={{ color: "#fca5a5" }}>exec()</code> with <code style={{ color: "#fca5a5" }}>os</code>, <code style={{ color: "#fca5a5" }}>json</code>, and <code style={{ color: "#fca5a5" }}>time</code> in scope.<br />
              It is <strong>disabled by default</strong> — set <code style={{ color: "#fca5a5" }}>ENABLE_RUN_SCRIPT=true</code> in <code style={{ color: "#fca5a5" }}>.env</code> to allow execution.<br />
              Every run is written to the audit log. Assign output to <code style={{ color: "#fca5a5" }}>result</code>.
            </div>
          </div>
        )}
        {node.data.type === "action.wait_for_approval" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#d97706", fontWeight: 600, marginBottom: 4 }}>🔔 Human-in-the-loop gate</div>
            Sends an email to <strong>Approver Email</strong> with <strong>Approve</strong> and <strong>Reject</strong> buttons.
            The flow pauses here until a decision is made or the timeout elapses.<br />
            <div style={{ marginTop: 6 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ approved, decision, token, approve_url, reject_url }"}</code></div>
            <div style={{ marginTop: 4, color: "#64748b" }}>
              Requires <strong>AGENTMAIL_API_KEY</strong> + <strong>AGENTMAIL_FROM</strong> to be set.
              Without email, approve/reject URLs are logged to the run output so you can trigger them manually.
            </div>
          </div>
        )}
        {node.data.type === "action.send_email" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>💡 Credential shortcut</div>
            Create an <strong>SMTP Server</strong> credential, then set <strong>SMTP Credential</strong> to its name — host, port, user and password are filled automatically.<br />
            The individual host/user/pass fields override the credential if both are set.
          </div>
        )}
        {node.data.type === "action.ssh" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>💡 Credential shortcut</div>
            Create an <strong>SSH Server</strong> credential in the Credentials page, then set <strong>SSH Credential</strong> to its name — host, port, username and password are filled automatically.<br />
            Individual fields override the credential if both are set.<br />
            <div style={{ marginTop: 6 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ stdout, stderr, exit_code, success }"}</code></div>
            <span style={{ color: "#64748b" }}>Host keys are auto-accepted (self-hosted use only).</span>
          </div>
        )}
        {node.data.type === "action.sftp" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>📁 SFTP / FTP Operations</div>
            Create an <strong>SFTP / FTP Server</strong> credential and set <strong>SFTP Credential</strong> to its name.<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#a78bfa" }}>list</code><span>→ {"{ files:[{name,path,size,is_dir,depth}], count }"} — set <em>recursive: true</em> to walk all subdirs</span>
              <code style={{ color: "#a78bfa" }}>upload</code><span>→ put <em>content</em> at remote_path</span>
              <code style={{ color: "#a78bfa" }}>download</code><span>→ {"{ content, size }"}</span>
              <code style={{ color: "#a78bfa" }}>delete</code><span>→ remove remote file</span>
              <code style={{ color: "#a78bfa" }}>mkdir</code><span>→ create remote directory</span>
              <code style={{ color: "#a78bfa" }}>rename</code><span>→ move/rename — set <em>new_path</em> as destination</span>
              <code style={{ color: "#a78bfa" }}>exists</code><span>→ {"{ exists, is_dir }"} — non-destructive check</span>
              <code style={{ color: "#a78bfa" }}>stat</code><span>→ {"{ size, is_dir, modified }"} — metadata for one path</span>
            </div>
          </div>
        )}
        {node.data.type === "action.github" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>🐙 GitHub Integration</div>
            <div style={{ color: "#94a3b8", marginBottom: 6 }}>
              Create a <strong>GitHub Token</strong> credential (type: API Key, paste your PAT in the <em>api_key</em> field), then set <strong>GitHub Credential</strong> to its name.
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px" }}>
              <code style={{ color: "#a78bfa" }}>get_repo</code><span>→ repo details (stars, forks, description…)</span>
              <code style={{ color: "#a78bfa" }}>list_issues</code><span>→ open issues list</span>
              <code style={{ color: "#a78bfa" }}>get_issue</code><span>→ single issue (needs issue_number)</span>
              <code style={{ color: "#a78bfa" }}>create_issue</code><span>→ new issue (title, body, labels)</span>
              <code style={{ color: "#a78bfa" }}>close_issue</code><span>→ close issue by number</span>
              <code style={{ color: "#a78bfa" }}>add_comment</code><span>→ comment on issue</span>
              <code style={{ color: "#a78bfa" }}>list_commits</code><span>→ recent commits</span>
              <code style={{ color: "#a78bfa" }}>list_prs</code><span>→ open PRs</span>
              <code style={{ color: "#a78bfa" }}>get_file</code><span>→ raw file content (needs path)</span>
              <code style={{ color: "#a78bfa" }}>create_release</code><span>→ new release (tag, title, body)</span>
            </div>
          </div>
        )}
        {node.data.type === "action.google_sheets" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>📊 Google Sheets Integration</div>
            <div style={{ color: "#94a3b8", marginBottom: 6 }}>
              Create a <strong>Google Service Account</strong> credential and paste the full SA JSON into the <em>value</em> field. Then set <strong>Google SA Credential</strong> to its name. Share the spreadsheet with the service account email.
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px" }}>
              <code style={{ color: "#a78bfa" }}>read_range</code><span>→ {"{ rows: [...], headers?, count }"}</span>
              <code style={{ color: "#a78bfa" }}>write_range</code><span>→ overwrite cells with rows_json 2D array</span>
              <code style={{ color: "#a78bfa" }}>append_rows</code><span>→ append rows_json below existing data</span>
              <code style={{ color: "#a78bfa" }}>clear_range</code><span>→ clear all values in range</span>
            </div>
            <div style={{ marginTop: 6, color: "#64748b" }}>Range format: <code>Sheet1!A1:D10</code> or just <code>Sheet1</code></div>
          </div>
        )}
        {node.data.type === "action.notion" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>🗒 Notion Integration</div>
            <div style={{ color: "#94a3b8", marginBottom: 6 }}>
              Create a <strong>Notion Integration Token</strong> credential (API Key type, paste <em>secret_…</em> token), then set <strong>Notion Credential</strong> to its name. Share the database or page with your integration in Notion.
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px" }}>
              <code style={{ color: "#a78bfa" }}>query_database</code><span>→ rows with flattened properties</span>
              <code style={{ color: "#a78bfa" }}>get_page</code><span>→ page with flattened properties</span>
              <code style={{ color: "#a78bfa" }}>create_page</code><span>→ new page in database (properties_json)</span>
              <code style={{ color: "#a78bfa" }}>update_page</code><span>→ update page properties</span>
              <code style={{ color: "#a78bfa" }}>search</code><span>→ workspace-wide search</span>
              <code style={{ color: "#a78bfa" }}>append_blocks</code><span>→ add paragraph to page</span>
            </div>
          </div>
        )}
        {node.data.type === "trigger.rss" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#ea580c", fontWeight: 600, marginBottom: 4 }}>📡 RSS / Atom Feed</div>
            Polls any RSS 2.0 or Atom 1.0 feed — no extra dependencies (stdlib only).<br />
            Pair with a <strong>Cron</strong> trigger to check for new items periodically.<br />
            <code style={{ color: "#a78bfa" }}>filter_expression</code> is a Python bool expression — use <code style={{ color: "#a78bfa" }}>entry</code> dict with keys <code style={{ color: "#a78bfa" }}>title</code>, <code style={{ color: "#a78bfa" }}>link</code>, <code style={{ color: "#a78bfa" }}>summary</code>, <code style={{ color: "#a78bfa" }}>author</code>.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ entries[], count, feed_title, title, link, summary, published }"}</code>
          </div>
        )}
        {node.data.type === "trigger.email" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#0891b2", fontWeight: 600, marginBottom: 4 }}>📧 Email (IMAP)</div>
            Create an <strong>IMAP</strong> credential with host, port, username, password, and use_ssl fields, then set <strong>IMAP Credential</strong> to its name.<br />
            <code style={{ color: "#a78bfa" }}>filter_expression</code> is a Python bool — use <code style={{ color: "#a78bfa" }}>email</code> dict and <code style={{ color: "#a78bfa" }}>re</code> module.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ emails[], count, subject, from, to, body, date }"}</code>
          </div>
        )}
        {node.data.type === "trigger.file_watch" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#7c3aed", fontWeight: 600, marginBottom: 4 }}>👁 File Watch</div>
            Polls a local or SFTP directory for recently-modified files matching the glob pattern.<br />
            Set <strong>SFTP Credential</strong> to watch a remote server; leave blank for local filesystem.<br />
            <code style={{ color: "#a78bfa" }}>min_age_seconds</code> acts as a write-guard — prevents picking up files still being written.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ files[], count, path, name, size, modified }"}</code>
          </div>
        )}
        {node.data.type === "action.postgres" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#336791", fontWeight: 600, marginBottom: 4 }}>🐘 SQL Query</div>
            Supports PostgreSQL (psycopg2), MySQL (pymysql — optional dep), and SQLite (stdlib).<br />
            Create a credential with a <code style={{ color: "#a78bfa" }}>dsn</code> field (connection string), or individual host/port/username/password/database fields. Set <strong>DB Credential</strong> to its name.<br />
            Use <code style={{ color: "#a78bfa" }}>%s</code> placeholders with the <strong>params</strong> JSON array for safe parameterised queries.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ rows[], count, row (first row), columns, affected }"}</code>
          </div>
        )}
        {node.data.type === "action.mysql" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#00758f", fontWeight: 600, marginBottom: 4 }}>🐬 MySQL / MariaDB</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>host</code>, <code style={{ color: "#a78bfa" }}>port</code>, <code style={{ color: "#a78bfa" }}>username</code>, <code style={{ color: "#a78bfa" }}>password</code>, <code style={{ color: "#a78bfa" }}>database</code> fields — or a single <code style={{ color: "#a78bfa" }}>dsn</code> string (e.g. <code>mysql://user:pass@host/db</code>).<br />
            Requires <code style={{ color: "#a78bfa" }}>pymysql</code> — install with <code>pip install pymysql</code>.<br />
            Use <code style={{ color: "#a78bfa" }}>%s</code> placeholders with the <strong>params</strong> JSON array for safe parameterised queries.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ rows[], count, row (first), columns, affected, last_insert_id }"}</code>
          </div>
        )}
        {node.data.type === "action.jira" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#0052cc", fontWeight: 600, marginBottom: 4 }}>🔵 Jira REST API v3</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>base_url</code> (e.g. <code>https://yourco.atlassian.net</code>), <code style={{ color: "#a78bfa" }}>email</code>, and <code style={{ color: "#a78bfa" }}>api_token</code> (from id.atlassian.com/manage/api-tokens).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>get-issue</code><span>→ {"{ issue{key,summary,status,…}, key, id }"}</span>
              <code style={{ color: "#60a5fa" }}>create-issue</code><span>→ requires project_key + summary; issue_type defaults to Task</span>
              <code style={{ color: "#a78bfa" }}>update-issue</code><span>→ pass fields as JSON {"{ summary, priority:{name}, … }"}</span>
              <code style={{ color: "#fbbf24" }}>add-comment</code><span>→ appends text comment; returns comment id</span>
              <code style={{ color: "#f87171" }}>search</code><span>→ JQL query → {"{ issues[], count, total }"}</span>
              <code style={{ color: "#94a3b8" }}>get-transitions</code><span>→ {"{ transitions[{id, name, to_status}] }"}</span>
              <code style={{ color: "#64748b" }}>transition-issue</code><span>→ move to status by transition_id</span>
            </div>
          </div>
        )}
        {node.data.type === "action.s3" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#FF9900", fontWeight: 600, marginBottom: 4 }}>🪣 S3 Storage</div>
            Create an S3 credential with <code style={{ color: "#a78bfa" }}>access_key</code>, <code style={{ color: "#a78bfa" }}>secret_key</code>, <code style={{ color: "#a78bfa" }}>region</code>, and optionally <code style={{ color: "#a78bfa" }}>endpoint_url</code> for S3-compatible services (MinIO, R2, B2).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>get</code><span>→ {"{ content, size, content_type, metadata }"}</span>
              <code style={{ color: "#60a5fa" }}>put</code><span>→ upload content to key</span>
              <code style={{ color: "#a78bfa" }}>list</code><span>→ {"{ objects[], count }"} — use prefix to filter</span>
              <code style={{ color: "#f87171" }}>delete</code><span>→ remove object by key</span>
              <code style={{ color: "#f59e0b" }}>presigned_url</code><span>→ {"{ url }"} — expires_in seconds</span>
              <code style={{ color: "#94a3b8" }}>head</code><span>→ metadata only (no download)</span>
              <code style={{ color: "#64748b" }}>copy</code><span>→ server-side copy (source_key → key)</span>
            </div>
          </div>
        )}
        {node.data.type === "action.pdf" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#b91c1c", fontWeight: 600, marginBottom: 4 }}>📋 PDF Generate</div>
            Renders HTML to PDF — full documents or bare fragments both work (a page wrapper is added automatically).<br />
            Use <code style={{ color: "#a78bfa" }}>{"{{template}}"}</code> syntax anywhere in the HTML.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ pdf_bytes (base64), size_bytes, filename, ok }"}</code><br />
            <div style={{ marginTop: 4, color: "#64748b" }}>
              💡 Pair with <strong>Send Email</strong> (base64 attachment) or <strong>S3</strong> (put operation) to deliver the file.
            </div>
          </div>
        )}
        {node.data.type === "action.graphql" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#e535ab", fontWeight: 600, marginBottom: 4 }}>◈ GraphQL</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>endpoint</code> + <code style={{ color: "#a78bfa" }}>token</code> fields → set <strong>GraphQL Credential</strong>.<br />
            Use <code style={{ color: "#a78bfa" }}>{"{{input.field}}"}</code> in the query and variables for template rendering.<br />
            Output: <code style={{ color: "#a78bfa" }}>{"{ data, errors[], has_errors, status_code, ok }"}</code>
          </div>
        )}
        {node.data.type === "action.hubspot" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#ff7a59", fontWeight: 600, marginBottom: 4 }}>🟠 HubSpot CRM v3</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>access_token</code> (Private App or OAuth token from HubSpot).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>get_contact</code><span>→ {"{ object{id,…props}, id, properties }"}</span>
              <code style={{ color: "#60a5fa" }}>create_contact</code><span>→ pass properties as JSON {"{ email, firstname, … }"}</span>
              <code style={{ color: "#a78bfa" }}>update_contact</code><span>→ object_id + properties JSON to patch</span>
              <code style={{ color: "#fbbf24" }}>search</code><span>→ filters array → {"{ results[], count, total, has_more }"}</span>
              <code style={{ color: "#f87171" }}>create_deal</code><span>→ properties {"{ dealname, amount, dealstage, … }"}</span>
              <code style={{ color: "#94a3b8" }}>associate</code><span>→ link from_id (contacts) ↔ to_id (deals)</span>
              <code style={{ color: "#64748b" }}>get_object / create_object / update_object</code><span>→ generic object_type</span>
            </div>
          </div>
        )}
        {node.data.type === "action.mongodb" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#00ed64", fontWeight: 600, marginBottom: 4 }}>🍃 MongoDB</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>uri</code> (e.g. <code>mongodb+srv://user:pass@cluster.mongodb.net/mydb</code>).<br />
            Requires <code style={{ color: "#fbbf24" }}>pymongo</code> — install with <code>pip install pymongo</code>.<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>find</code><span>→ {"{ documents[], count, document (first) }"} — supports filter, projection, sort, limit</span>
              <code style={{ color: "#60a5fa" }}>find_one</code><span>→ {"{ document, found }"}</span>
              <code style={{ color: "#a78bfa" }}>insert_one</code><span>→ {"{ inserted_id, ok }"}</span>
              <code style={{ color: "#fbbf24" }}>update_one</code><span>→ {"{ matched_count, modified_count, upserted_id }"}</span>
              <code style={{ color: "#f87171" }}>delete_one</code><span>→ {"{ deleted_count }"}</span>
              <code style={{ color: "#94a3b8" }}>aggregate</code><span>→ pipeline array → {"{ documents[], count }"}</span>
            </div>
            <div style={{ marginTop: 4, color: "#64748b" }}>All ObjectId and datetime fields are auto-converted to strings.</div>
          </div>
        )}
        {node.data.type === "action.twilio" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#f22f46", fontWeight: 600, marginBottom: 4 }}>🔴 Twilio SMS / WhatsApp / Voice</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>account_sid</code> + <code style={{ color: "#a78bfa" }}>auth_token</code> (from console.twilio.com).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>send_sms</code><span>→ {"{ sid, status, to, from, body }"}</span>
              <code style={{ color: "#60a5fa" }}>send_whatsapp</code><span>→ same as send_sms — prefixes <code>whatsapp:</code> automatically</span>
              <code style={{ color: "#a78bfa" }}>make_call</code><span>→ twiml_url or inline TwiML → {"{ sid, status }"}</span>
              <code style={{ color: "#fbbf24" }}>check_status</code><span>→ pass sid + resource_type (message/call) → current status</span>
              <code style={{ color: "#94a3b8" }}>list_messages</code><span>→ filter by to/from → {"{ messages[], count }"}</span>
            </div>
          </div>
        )}
        {node.data.type === "action.linear" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#5e6ad2", fontWeight: 600, marginBottom: 4 }}>🔺 Linear</div>
            Create a credential with <code style={{ color: "#a78bfa" }}>api_key</code> (from linear.app → Settings → API → Personal API keys).<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>get_issue</code><span>→ {"{ issue{id,identifier,title,state,…}, title, state }"}</span>
              <code style={{ color: "#60a5fa" }}>create_issue</code><span>→ requires team_id + title → {"{ issue{id,url,…} }"}</span>
              <code style={{ color: "#a78bfa" }}>update_issue</code><span>→ issue_id + updates JSON {"{ stateId, priority, title, … }"}</span>
              <code style={{ color: "#fbbf24" }}>search_issues</code><span>→ query string → {"{ issues[], count }"}</span>
            </div>
          </div>
        )}
        {node.data.type === "action.openai_assistant" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#10a37f", fontWeight: 600, marginBottom: 4 }}>🤖 OpenAI Assistants v2</div>
            Use your existing OpenAI API key. Create an assistant at platform.openai.com → Assistants.<br />
            <strong style={{ color: "#e2e8f0" }}>Typical flow:</strong> create_thread → add_message → run_thread (polls until done) → get reply text.<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>create_thread</code><span>→ {"{ thread_id }"}</span>
              <code style={{ color: "#60a5fa" }}>add_message</code><span>→ thread_id + content + role → {"{ message_id }"}</span>
              <code style={{ color: "#a78bfa" }}>run_thread</code><span>→ polls until complete → {"{ reply, run_id, messages[], status }"}</span>
              <code style={{ color: "#fbbf24" }}>get_run_status</code><span>→ thread_id + run_id → {"{ status, run }"}</span>
              <code style={{ color: "#94a3b8" }}>list_messages</code><span>→ thread_id → {"{ messages[{role,text}], count }"}</span>
            </div>
          </div>
        )}
        {node.data.type === "note" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6 }}>
            <div style={{ color: "#ca8a04", fontWeight: 600, marginBottom: 5 }}>📝 Sticky notes are skipped by the executor</div>
            <div style={{ marginBottom: 8 }}>Use them to label sections, leave reminders, or document your flow logic.</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {["amber", "blue", "green", "purple", "red", "slate"].map(c => {
                const p = NOTE_COLORS[c];
                return (
                  <span key={c} style={{
                    background: p.bg, border: `1.5px solid ${p.border}`,
                    borderRadius: 4, padding: "2px 8px", color: p.text, fontSize: 9, fontWeight: 600,
                  }}>{c}</span>
                );
              })}
            </div>
          </div>
        )}
        {node.data.type === "trigger.cron" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>⏰ Saved automatically as a schedule</div>
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px" }}>
              <span style={{ color: "#64748b" }}>Every minute</span><code>* * * * *</code>
              <span style={{ color: "#64748b" }}>Every hour</span><code>0 * * * *</code>
              <span style={{ color: "#64748b" }}>Daily at 9am</span><code>0 9 * * *</code>
              <span style={{ color: "#64748b" }}>Weekdays 9am</span><code>0 9 * * 1-5</code>
              <span style={{ color: "#64748b" }}>Every 15 min</span><code>*/15 * * * *</code>
              <span style={{ color: "#64748b" }}>1st of month</span><code>0 0 1 * *</code>
            </div>
          </div>
        )}

        {node.data.type === "trigger.webhook" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ color: "#0284c7", fontWeight: 600, marginBottom: 4 }}>🔗 Webhook Trigger</div>
            Your webhook URL is:<br />
            <code style={{ color: "#a78bfa", wordBreak: "break-all" }}>https://your-app.example.com/api/webhooks/trigger/{"{{graph_id}}"}/{"{{secret}}"}</code><br />
            Replace <strong>{"{{graph_id}}"}</strong> with this flow's ID and <strong>{"{{secret}}"}</strong> with your secret (or leave blank to accept any POST).<br />
            To fire manually, use the <strong>▶ Run</strong> button in the topbar.
          </div>
        )}

        {!runStatus && (node.data.type === "trigger.cron" || node.data.type === "trigger.webhook") && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#64748b", marginTop: 6, lineHeight: 1.7 }}>
            <span style={{ color: "#a78bfa" }}>💡</span> This flow has never run. Use the <strong>▶ Run</strong> button in the topbar to trigger it manually, or save and the schedule/webhook will activate automatically.
          </div>
        )}

        {/* Retry Policy (action nodes only) */}
        {!isNote && !isTrigger && (
          <>
            <div className="section-divider">Retry Policy</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
              <div className="field-group" style={{ marginBottom: 0 }}>
                <label style={{ fontSize: 11, color: "#94a3b8", fontWeight: 500, marginBottom: 4, display: "block" }}>Max retries</label>
                <input className="field-input" type="number" min="0" max="5" value={retryMax} onChange={e => updateRetryMax(e.target.value)} />
              </div>
              <div className="field-group" style={{ marginBottom: 0 }}>
                <label style={{ fontSize: 11, color: "#94a3b8", fontWeight: 500, marginBottom: 4, display: "block" }}>Retry delay (sec)</label>
                <input className="field-input" type="number" min="1" max="60" value={retryDelay} onChange={e => updateRetryDelay(e.target.value)} />
              </div>
            </div>
            <div className="field-group" style={{ marginTop: 10 }}>
              <label style={{ fontSize: 11, color: "#94a3b8", fontWeight: 500, marginBottom: 4, display: "block" }}>On failure</label>
              <select className="field-input" value={failMode} onChange={e => updateFailMode(e.target.value)}>
                <option value="abort">abort — stop the graph (default)</option>
                <option value="continue">continue — store error, keep going</option>
              </select>
              {failMode === "continue" && (
                <div style={{ fontSize: 10, color: "#f59e0b", marginTop: 4 }}>
                  ⚠ Downstream nodes receive {"{ __error, __node, __type }"} as input. Use a Condition node to check for errors.
                </div>
              )}
            </div>
          </>
        )}

        {/* Merge node hint */}
        {node.data.type === "action.merge" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>⇒ Merge / Join</div>
            Connect multiple upstream nodes to this node to combine their outputs.
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 6 }}>
              <code style={{ color: "#4ade80" }}>dict</code><span>→ merge all upstream dicts (last wins on collision)</span>
              <code style={{ color: "#60a5fa" }}>all</code><span>→ {"{ merged: [...], count: N }"}</span>
              <code style={{ color: "#94a3b8" }}>first</code><span>→ pass through the first upstream value</span>
            </div>
          </div>
        )}
        {node.data.type === "action.switch" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            Value expression evaluates to a string/number, then matched against cases in order — first match wins.<br />
            <div style={{ marginTop: 4 }}>Cases JSON: <code style={{ color: "#a78bfa" }}>{'[{"match":"ok","label":"success"},{"match":"error","label":"failure"}]'}</code></div>
            <div style={{ marginTop: 4 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ value, matched_case, matched_index, no_match }"}</code></div>
            <div style={{ color: "#64748b", marginTop: 4 }}>Use <code>matched_case</code> in downstream nodes to route behaviour. <code>no_match: true</code> if nothing matched.</div>
          </div>
        )}
        {node.data.type === "action.aggregate" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            Place after a Loop body to collect per-item results.<br />
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px", marginTop: 4 }}>
              <code style={{ color: "#4ade80" }}>list</code><span>→ <code>{"{ items:[...], count }"}</code></span>
              <code style={{ color: "#60a5fa" }}>dict</code><span>→ merge item dicts into one object</span>
              <code style={{ color: "#94a3b8" }}>concat</code><span>→ join items as strings with separator</span>
            </div>
          </div>
        )}
        {node.data.type === "action.date" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            Output always includes: <code style={{ color: "#a78bfa" }}>{"{ iso, unix, formatted, year, month, day, hour, minute, second, weekday }"}</code><br />
            <code>diff</code> returns: <code style={{ color: "#a78bfa" }}>{"{ seconds, minutes, hours, days, human, past }"}</code><br />
            <div style={{ marginTop: 4, color: "#64748b" }}>
              Date input accepts ISO 8601 strings or Unix timestamps. Leave blank to use current UTC time.
            </div>
          </div>
        )}
        {node.data.type === "action.discord" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>💡 Credential shortcut</div>
            Create a <strong>Discord Webhook</strong> credential, then set <strong>Discord Credential</strong> to its name.<br />
            The Webhook URL field overrides the credential if both are set.<br />
            <div style={{ marginTop: 4 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ sent: true, message }"}</code></div>
          </div>
        )}
        {node.data.type === "action.csv" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.8 }}>
            <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "0 10px" }}>
              <code style={{ color: "#4ade80" }}>parse</code><span>→ CSV string to list of dicts; output: <code>{"{ rows, count, headers }"}</code></span>
              <code style={{ color: "#60a5fa" }}>generate</code><span>→ list of dicts to CSV string; output: <code>{"{ csv, count }"}</code></span>
            </div>
            <div style={{ marginTop: 4, color: "#64748b" }}>Parse: leave content blank to use upstream input directly.</div>
          </div>
        )}
        {node.data.type === "action.http_request" && (
          <div style={{ background: "#0f1117", border: "1px solid #2a2d3e", borderRadius: 6, padding: "8px 10px", fontSize: 10, color: "#94a3b8", marginTop: 6, lineHeight: 1.9 }}>
            <div style={{ color: "#a78bfa", fontWeight: 600, marginBottom: 4 }}>💡 Credential shortcut</div>
            Create a credential with a <code>token</code> or <code>api_key</code> field; set <strong>Bearer Credential</strong> to its name — the Authorization header is set automatically.<br />
            <div style={{ marginTop: 4 }}>Output: <code style={{ color: "#a78bfa" }}>{"{ status, ok, body, headers }"}</code></div>
            <div style={{ marginTop: 4, color: "#64748b" }}>
              <code>ignore_errors: true</code> prevents raising on 4xx/5xx — check <code>ok</code> in downstream nodes.
            </div>
          </div>
        )}

        {/* Node I/O Panel */}
        {!isNote && (
          <NodeIOPanel
            runInput={runInput}
            runOutput={runOutput}
            runStatus={runStatus}
            runDurationMs={runDurationMs}
            runAttempts={runAttempts}
            nodeId={node.id}
            upstreamNodeId={upstreamNodeId}
            nodeLabel={node.data.label || def.label}
          />
        )}

        <button className="delete-node-btn" onClick={() => onDelete(node.id)}>🗑 Remove node</button>
      </div>
    </div>
  );
}
