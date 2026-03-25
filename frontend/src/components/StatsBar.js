import React from 'react';
import './StatsBar.css';

const LABELS = {
  customers: 'Customers',
  sales_orders: 'Orders',
  deliveries: 'Deliveries',
  invoices: 'Invoices',
  payments: 'Payments',
  products: 'Products',
};

const COLORS = {
  customers: '#3b82f6',
  sales_orders: '#8b5cf6',
  deliveries: '#10b981',
  invoices: '#f59e0b',
  payments: '#ef4444',
  products: '#06b6d4',
};

export default function StatsBar({ stats }) {
  return (
    <div className="stats-bar">
      {Object.entries(LABELS).map(([key, label]) => (
        <div key={key} className="stat-item">
          <span className="stat-dot" style={{ background: COLORS[key] }} />
          <span className="stat-count">{(stats[key] || 0).toLocaleString()}</span>
          <span className="stat-label">{label}</span>
        </div>
      ))}
    </div>
  );
}
