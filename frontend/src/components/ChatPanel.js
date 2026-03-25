import React, { useState, useRef, useEffect } from 'react';
import { Send, RotateCcw, Zap } from 'lucide-react';
import ResultTable from './ResultTable';
import './ChatPanel.css';

const QUICK_QUERIES = [
  "Which products have the highest number of billing documents?",
  "Show me orders with broken or incomplete flows",
  "Trace the full flow of the first billing document",
  "How many orders have been delivered but not invoiced?",
  "What is the total payment amount received?",
  "List top 5 customers by order value",
];

export default function ChatPanel({ onHighlight, apiBase }) {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: 'Hello! I can answer questions about your business data — orders, deliveries, invoices, payments, customers, and products.\n\nTry one of the quick queries below or ask your own question.',
      timestamp: new Date(),
    }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const send = async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;
    setInput('');

    const userMsg = { role: 'user', content: msg, timestamp: new Date() };
    setMessages(prev => [...prev, userMsg]);
    setLoading(true);

    try {
      const history = messages.map(m => ({ role: m.role, content: m.content }));
      const r = await fetch(`${apiBase}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, history }),
      });
      const data = await r.json();

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: data.response,
        sqlResult: data.sql_result,
        timestamp: new Date(),
      }]);

      // Highlight nodes if result has IDs
      if (data.sql_result?.rows?.length) {
        // Try to extract entity IDs from result rows for highlighting
        const ids = extractNodeIds(data.sql_result);
        if (ids.length) onHighlight(ids);
      }
    } catch (e) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: '⚠️ Failed to reach the server. Make sure the backend is running.',
        timestamp: new Date(),
        error: true,
      }]);
    } finally {
      setLoading(false);
    }
  };

  const extractNodeIds = (result) => {
    if (!result?.columns || !result?.rows) return [];
    const ids = [];
    const prefixMap = {
      sales_order_id: 'so_', delivery_id: 'del_', invoice_id: 'inv_',
      payment_id: 'pay_', customer_id: 'cust_', product_id: 'prod_',
    };
    result.columns.forEach((col, ci) => {
      const prefix = prefixMap[col];
      if (prefix) {
        result.rows.forEach(row => { if (row[ci]) ids.push(prefix + row[ci]); });
      }
    });
    return ids;
  };

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
  };

  const clearChat = () => {
    setMessages([{
      role: 'assistant',
      content: 'Chat cleared. Ask me anything about your business data.',
      timestamp: new Date(),
    }]);
    onHighlight([]);
  };

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <div className="chat-title">
          <Zap size={14} className="chat-icon" />
          <span>Natural Language Query</span>
        </div>
        <button className="icon-btn" onClick={clearChat} title="Clear chat">
          <RotateCcw size={13} />
        </button>
      </div>

      <div className="chat-messages">
        {messages.map((msg, i) => (
          <div key={i} className={`message message-${msg.role} ${msg.error ? 'message-error' : ''}`}>
            <div className="message-bubble">
              <pre className="message-text">{msg.content}</pre>
              {msg.sqlResult && !msg.sqlResult.error && msg.sqlResult.rows?.length > 0 && (
                <ResultTable result={msg.sqlResult} />
              )}
              {msg.sqlResult?.error && (
                <div className="sql-error">SQL Error: {msg.sqlResult.error}</div>
              )}
            </div>
            <div className="message-time">
              {msg.timestamp?.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </div>
          </div>
        ))}

        {loading && (
          <div className="message message-assistant">
            <div className="message-bubble">
              <div className="typing-indicator">
                <span /><span /><span />
              </div>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="quick-queries">
        <div className="quick-label">Quick queries</div>
        <div className="quick-list">
          {QUICK_QUERIES.map((q, i) => (
            <button key={i} className="quick-btn" onClick={() => send(q)} disabled={loading}>
              {q}
            </button>
          ))}
        </div>
      </div>

      <div className="chat-input-area">
        <textarea
          ref={inputRef}
          className="chat-input"
          placeholder="Ask about orders, deliveries, invoices..."
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKey}
          rows={2}
          disabled={loading}
        />
        <button
          className="send-btn"
          onClick={() => send()}
          disabled={!input.trim() || loading}
        >
          <Send size={15} />
        </button>
      </div>
    </div>
  );
}
