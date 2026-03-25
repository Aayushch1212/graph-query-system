import React, { useState } from 'react';
import './ResultTable.css';

const MAX_ROWS_SHOWN = 8;

export default function ResultTable({ result }) {
  const [expanded, setExpanded] = useState(false);
  if (!result || !result.columns?.length) return null;

  const rows = expanded ? result.rows : result.rows.slice(0, MAX_ROWS_SHOWN);
  const hasMore = result.rows.length > MAX_ROWS_SHOWN;

  return (
    <div className="result-table-wrap">
      <div className="result-meta">
        {result.count} row{result.count !== 1 ? 's' : ''} returned
      </div>
      <div className="result-scroll">
        <table className="result-table">
          <thead>
            <tr>
              {result.columns.map(col => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i}>
                {row.map((cell, j) => (
                  <td key={j} title={String(cell ?? '')}>
                    {cell == null ? <span className="null-val">null</span> : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hasMore && (
        <button className="expand-btn" onClick={() => setExpanded(e => !e)}>
          {expanded ? `Show less` : `Show all ${result.count} rows`}
        </button>
      )}
    </div>
  );
}
