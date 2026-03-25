import React, { useState, useEffect } from 'react';
import GraphView from './components/GraphView';
import ChatPanel from './components/ChatPanel';
import StatsBar from './components/StatsBar';
import NodeDetail from './components/NodeDetail';
import './App.css';

const API = process.env.REACT_APP_API_URL || 'http://localhost:8000';

export default function App() {
  const [graphData, setGraphData] = useState({ nodes: [], edges: [] });
  const [stats, setStats] = useState({});
  const [selectedNode, setSelectedNode] = useState(null);
  const [nodeDetail, setNodeDetail] = useState(null);
  const [highlightedNodes, setHighlightedNodes] = useState(new Set());
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('graph'); // 'graph' | 'chat' on mobile

  useEffect(() => {
    Promise.all([
      fetch(`${API}/api/graph`).then(r => r.json()),
      fetch(`${API}/api/stats`).then(r => r.json()),
    ]).then(([graph, stats]) => {
      setGraphData(graph);
      setStats(stats);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const handleNodeClick = async (node) => {
    setSelectedNode(node);
    try {
      const r = await fetch(`${API}/api/node/${node.id}`);
      const data = await r.json();
      setNodeDetail(data);
    } catch {
      setNodeDetail(node.properties);
    }
  };

  const handleHighlight = (nodeIds) => {
    setHighlightedNodes(new Set(nodeIds));
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-left">
          <div className="logo">
            <span className="logo-icon">◈</span>
            <span className="logo-text">GraphQuery</span>
          </div>
          <StatsBar stats={stats} />
        </div>
        <div className="header-tabs">
          <button
            className={`tab-btn ${activeTab === 'graph' ? 'active' : ''}`}
            onClick={() => setActiveTab('graph')}
          >Graph</button>
          <button
            className={`tab-btn ${activeTab === 'chat' ? 'active' : ''}`}
            onClick={() => setActiveTab('chat')}
          >Query</button>
        </div>
      </header>

      <main className="app-body">
        {loading ? (
          <div className="loading-screen">
            <div className="loading-spinner" />
            <p>Loading graph data...</p>
          </div>
        ) : (
          <>
            <div className={`pane graph-pane ${activeTab === 'graph' ? 'visible' : ''}`}>
              <GraphView
                data={graphData}
                onNodeClick={handleNodeClick}
                selectedNode={selectedNode}
                highlightedNodes={highlightedNodes}
              />
              {nodeDetail && selectedNode && (
                <NodeDetail
                  node={selectedNode}
                  detail={nodeDetail}
                  onClose={() => { setSelectedNode(null); setNodeDetail(null); }}
                />
              )}
            </div>
            <div className={`pane chat-pane ${activeTab === 'chat' ? 'visible' : ''}`}>
              <ChatPanel onHighlight={handleHighlight} apiBase={API} />
            </div>
          </>
        )}
      </main>
    </div>
  );
}
