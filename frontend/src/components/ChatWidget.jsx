
import { useState, useRef, useEffect } from 'react';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer, Legend,
} from 'recharts';

const COLORS = ['#1e40af', '#3b82f6', '#16a34a', '#d97706', '#dc2626', '#8b5cf6', '#0284c7', '#ea580c'];

// Use an explicit backend base URL instead of relying on Vite proxy.
// Prefer VITE_API_URL when provided; otherwise default to the same host (port 8005).
const DEFAULT_API_BASE = (typeof window !== 'undefined')
  ? `http://${window.location.hostname}:8005`
  : 'http://127.0.0.1:8005';
const API_BASE = import.meta.env.VITE_API_URL || DEFAULT_API_BASE;

/* ── Lightweight Markdown Renderer ──────────────────────────────── */
function renderMarkdown(text) {
  if (!text) return null;

  const lines = text.split('\n');
  const elements = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Headings
    if (line.startsWith('### ')) {
      elements.push(<h4 key={i} className="chat-md-h3">{inlineMd(line.slice(4))}</h4>);
      i++; continue;
    }
    if (line.startsWith('## ')) {
      elements.push(<h3 key={i} className="chat-md-h2">{inlineMd(line.slice(3))}</h3>);
      i++; continue;
    }
    if (line.startsWith('# ')) {
      elements.push(<h2 key={i} className="chat-md-h1">{inlineMd(line.slice(2))}</h2>);
      i++; continue;
    }

    // Table detection
    if (line.includes('|') && i + 1 < lines.length && /^\|[-\s|:]+\|$/.test(lines[i + 1]?.trim())) {
      const tableLines = [line];
      let j = i + 1;
      while (j < lines.length && lines[j].includes('|')) {
        tableLines.push(lines[j]);
        j++;
      }
      elements.push(<MarkdownTable key={i} lines={tableLines} />);
      i = j; continue;
    }

    // Bullet list
    if (/^[\-\*•]\s/.test(line)) {
      const items = [];
      let j = i;
      while (j < lines.length && /^[\-\*•]\s/.test(lines[j])) {
        items.push(lines[j].replace(/^[\-\*•]\s+/, ''));
        j++;
      }
      elements.push(
        <ul key={i} className="chat-md-list">
          {items.map((item, idx) => <li key={idx}>{inlineMd(item)}</li>)}
        </ul>
      );
      i = j; continue;
    }

    // Numbered list
    if (/^\d+[\.\)]\s/.test(line)) {
      const items = [];
      let j = i;
      while (j < lines.length && /^\d+[\.\)]\s/.test(lines[j])) {
        items.push(lines[j].replace(/^\d+[\.\)]\s+/, ''));
        j++;
      }
      elements.push(
        <ol key={i} className="chat-md-list">
          {items.map((item, idx) => <li key={idx}>{inlineMd(item)}</li>)}
        </ol>
      );
      i = j; continue;
    }

    // Empty line
    if (line.trim() === '') { i++; continue; }

    // Regular paragraph
    elements.push(<p key={i} className="chat-md-p">{inlineMd(line)}</p>);
    i++;
  }

  return <>{elements}</>;
}

function inlineMd(text) {
  // Bold + Italic
  const parts = [];
  const regex = /(\*\*\*(.+?)\*\*\*|\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`)/g;
  let last = 0;
  let match;
  while ((match = regex.exec(text)) !== null) {
    if (match.index > last) parts.push(text.slice(last, match.index));
    if (match[2]) parts.push(<strong key={match.index}><em>{match[2]}</em></strong>);
    else if (match[3]) parts.push(<strong key={match.index}>{match[3]}</strong>);
    else if (match[4]) parts.push(<em key={match.index}>{match[4]}</em>);
    else if (match[5]) parts.push(<code key={match.index} className="chat-md-code">{match[5]}</code>);
    last = match.index + match[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts.length ? parts : text;
}

function MarkdownTable({ lines }) {
  const parseRow = (line) => line.split('|').filter((_, i, arr) => i > 0 && i < arr.length - 1).map(c => c.trim());
  const headers = parseRow(lines[0]);
  const rows = lines.slice(2).map(parseRow);

  return (
    <div className="chat-md-table-wrap">
      <table className="chat-md-table">
        <thead><tr>{headers.map((h, i) => <th key={i}>{inlineMd(h)}</th>)}</tr></thead>
        <tbody>{rows.map((row, i) => (
          <tr key={i}>{row.map((cell, j) => <td key={j}>{inlineMd(cell)}</td>)}</tr>
        ))}</tbody>
      </table>
    </div>
  );
}

/* ── Mini Chart Component ───────────────────────────────────────── */
function MiniChart({ chart }) {
  if (!chart || !chart.data || chart.data.length === 0) return null;

  const { type, title, data, xKey, yKeys } = chart;

  return (
    <div className="chat-chart">
      {title && <div className="chat-chart-title">{title}</div>}
      <ResponsiveContainer width="100%" height={200}>
        {type === 'bar' ? (
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey={xKey} tick={{ fontSize: 10 }} />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip />
            {yKeys.map((k, i) => (
              <Bar key={k} dataKey={k} fill={COLORS[i % COLORS.length]} radius={[3, 3, 0, 0]} />
            ))}
            {yKeys.length > 1 && <Legend />}
          </BarChart>
        ) : type === 'line' ? (
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey={xKey} tick={{ fontSize: 10 }} />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip />
            {yKeys.map((k, i) => (
              <Line key={k} type="monotone" dataKey={k} stroke={COLORS[i % COLORS.length]} strokeWidth={2} dot={false} />
            ))}
            {yKeys.length > 1 && <Legend />}
          </LineChart>
        ) : type === 'area' ? (
          <AreaChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey={xKey} tick={{ fontSize: 10 }} />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip />
            {yKeys.map((k, i) => (
              <Area key={k} type="monotone" dataKey={k} fill={COLORS[i % COLORS.length]} fillOpacity={0.2} stroke={COLORS[i % COLORS.length]} strokeWidth={2} />
            ))}
            {yKeys.length > 1 && <Legend />}
          </AreaChart>
        ) : type === 'pie' ? (
          <PieChart>
            <Pie data={data} dataKey={yKeys[0]} nameKey={xKey} cx="50%" cy="50%" outerRadius={70} label={({ name, value }) => `${name}: ${value}`}>
              {data.map((_, i) => (
                <Cell key={i} fill={COLORS[i % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        ) : null}
      </ResponsiveContainer>
    </div>
  );
}

/* ── Mode-specific suggestions ──────────────────────────────────── */
const MODE_SUGGESTIONS = {
  fleet: [
    'What is the current fleet health status?',
    'Show top 10 DTC codes across the fleet',
    'Which vehicles need urgent maintenance?',
    'Show fleet health trend as a line chart',
  ],
  vehicle: [
    'What are the active faults on this vehicle?',
    'Show this vehicle\'s fault history',
    'Is this vehicle safe to operate?',
    'What maintenance does this vehicle need?',
  ],
  dtc: [
    'Explain this DTC code — causes and symptoms',
    'Which vehicles are affected by this DTC?',
    'What faults commonly occur with this DTC?',
    'What is the recommended repair action?',
  ],
  default: [
    'How many vehicles have active faults?',
    'Show top 10 DTC codes by occurrence',
    'What is the fleet health trend this month?',
    'Which vehicles need urgent maintenance?',
  ],
};

/* ── Main Chat Widget Component ─────────────────────────────────── */
export default function ChatWidget({ pageContext }) {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  const mode = pageContext?.mode || 'default';
  const suggestions = MODE_SUGGESTIONS[mode] || MODE_SUGGESTIONS.default;

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const sendMessage = async (overrideText) => {
    const text = (overrideText || input).trim();
    if (!text || loading) return;

    const userMsg = { role: 'user', content: text };
    const newMsgs = [...messages, userMsg];
    setMessages(newMsgs);
    setInput('');
    setLoading(true);

    try {
      const apiMessages = newMsgs.map(m => ({ role: m.role, content: m.content }));
      const body = { messages: apiMessages };
      // Attach page context if available
      if (pageContext) {
        body.context = pageContext;
      }
      const res = await fetch(`${API_BASE}/api/ai/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        throw new Error(`Server error: ${res.status}`);
      }

      const data = await res.json();
      const assistantMsg = {
        role: 'assistant',
        content: data.text || 'No response.',
        chart: data.chart || null,
      };
      setMessages(prev => [...prev, assistantMsg]);
    } catch (err) {
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: `Error: ${err.message}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const clearChat = () => setMessages([]);

  const modeLabel = mode === 'vehicle' ? '🚛 Vehicle Mode'
    : mode === 'dtc' ? '🔧 DTC Mode'
    : mode === 'fleet' ? '📊 Fleet Mode'
    : '✦ AI Analyst';

  return (
    <>
      {/* Floating Action Button */}
      <button
        className={`chat-fab ${open ? 'chat-fab-open' : ''}`}
        onClick={() => setOpen(!open)}
        title="AI Analyst"
      >
        {open ? '✕' : '✦'}
      </button>

      {/* Chat Panel */}
      {open && (
        <div className="chat-panel">
          <div className="chat-header">
            <div className="chat-header-left">
              <span className="chat-header-icon">✦</span>
              <span>{modeLabel}</span>
              {pageContext?.customer_name && (
                <span className="chat-header-ctx">• {pageContext.customer_name}</span>
              )}
            </div>
            <button className="chat-clear-btn" onClick={clearChat} title="Clear chat">
              Clear
            </button>
          </div>

          <div className="chat-messages">
            {messages.length === 0 && (
              <div className="chat-welcome">
                <div className="chat-welcome-icon">✦</div>
                <div className="chat-welcome-title">DTC Fleet AI Analyst</div>
                <div className="chat-welcome-sub">
                  {mode === 'vehicle' ? 'Ask me about this vehicle\'s health, faults, and maintenance needs.' :
                   mode === 'dtc' ? 'Ask me about this DTC code — causes, symptoms, affected vehicles, and repair actions.' :
                   mode === 'fleet' ? 'Ask me about fleet health, fault distribution, trends, and maintenance priorities.' :
                   'Ask me about fleet health, vehicle faults, DTC trends, or maintenance priorities.'}
                </div>
                <div className="chat-suggestions">
                  {suggestions.map((q) => (
                    <button
                      key={q}
                      className="chat-suggestion"
                      onClick={() => { sendMessage(q); }}
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`chat-msg ${msg.role === 'user' ? 'chat-msg-user' : 'chat-msg-ai'}`}>
                {msg.role === 'assistant' && <span className="chat-msg-avatar">✦</span>}
                <div className="chat-msg-bubble">
                  {msg.role === 'assistant' ? (
                    <div className="chat-msg-text">{renderMarkdown(msg.content)}</div>
                  ) : (
                    <div className="chat-msg-text">{msg.content}</div>
                  )}
                  {msg.chart && <MiniChart chart={msg.chart} />}
                </div>
              </div>
            ))}

            {loading && (
              <div className="chat-msg chat-msg-ai">
                <span className="chat-msg-avatar">✦</span>
                <div className="chat-msg-bubble">
                  <div className="chat-typing">
                    <span></span><span></span><span></span>
                  </div>
                </div>
              </div>
            )}

            <div ref={bottomRef} />
          </div>

          <div className="chat-input-bar">
            <input
              ref={inputRef}
              className="chat-input"
              type="text"
              placeholder={mode === 'vehicle' ? 'Ask about this vehicle...'
                : mode === 'dtc' ? 'Ask about this DTC code...'
                : 'Ask about fleet, vehicles, DTCs...'}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={loading}
            />
            <button className="chat-send-btn" onClick={() => sendMessage()} disabled={loading || !input.trim()}>
              ➤
            </button>
          </div>
        </div>
      )}
    </>
  );
}
