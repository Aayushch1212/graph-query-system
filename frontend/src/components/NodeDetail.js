import React from 'react';
import { X } from 'lucide-react';
import './NodeDetail.css';

const TYPE_COLORS = {
  Customer: '#3b82f6', SalesOrder: '#8b5cf6', Delivery: '#10b981',
  Invoice: '#f59e0b', Payment: '#ef4444', Product: '#06b6d4',
};

export default function NodeDetail({ node, detail, onClose }) {
  const color = TYPE_COLORS[node.type] || '#64748b';
  const entries = Object.entries(detail || {}).filter(([, v]) => v != null && v !== '');

  return (
    <div className="node-detail">
      <div className="nd-header" style={{ borderLeftColor: color }}>
        <div>
          <div className="nd-type" style={{ color }}>{node.type}</div>
          <div className="nd-label">{node.label}</div>
        </div>
        <button className="nd-close" onClick={onClose}><X size={14} /></button>
      </div>
      <div className="nd-body">
        {entries.map(([k, v]) => (
          <div key={k} className="nd-row">
            <span className="nd-key">{k.replace(/_/g, ' ')}</span>
            <span className="nd-val">{String(v)}</span>
          </div>
        ))}
        {!entries.length && <div className="nd-empty">No additional data</div>}
      </div>
    </div>
  );
}
