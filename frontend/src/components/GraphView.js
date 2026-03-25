import React, { useEffect, useRef, useCallback } from 'react';
import * as d3 from 'd3';
import './GraphView.css';

const NODE_COLORS = {
  Customer: '#3b82f6',
  SalesOrder: '#8b5cf6',
  Delivery: '#10b981',
  Invoice: '#f59e0b',
  Payment: '#ef4444',
  Product: '#06b6d4',
};

const NODE_RADIUS = {
  Customer: 14,
  SalesOrder: 12,
  Delivery: 12,
  Invoice: 11,
  Payment: 11,
  Product: 10,
};

const ICONS = {
  Customer: '👤',
  SalesOrder: '📋',
  Delivery: '🚚',
  Invoice: '🧾',
  Payment: '💳',
  Product: '📦',
};

export default function GraphView({ data, onNodeClick, selectedNode, highlightedNodes }) {
  const svgRef = useRef(null);
  const simRef = useRef(null);
  const zoomRef = useRef(null);

  const draw = useCallback(() => {
    if (!svgRef.current || !data.nodes.length) return;

    const container = svgRef.current.parentElement;
    const W = container.clientWidth;
    const H = container.clientHeight;

    d3.select(svgRef.current).selectAll('*').remove();

    const svg = d3.select(svgRef.current)
      .attr('width', W)
      .attr('height', H);

    // Defs: arrowhead marker
    const defs = svg.append('defs');
    defs.append('marker')
      .attr('id', 'arrow')
      .attr('viewBox', '0 -4 8 8')
      .attr('refX', 20)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,-4L8,0L0,4')
      .attr('fill', '#334155');

    const g = svg.append('g').attr('class', 'zoom-group');

    // Zoom
    const zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on('zoom', (e) => g.attr('transform', e.transform));
    svg.call(zoom);
    zoomRef.current = zoom;

    // Initial zoom to fit
    svg.call(zoom.transform, d3.zoomIdentity.translate(W / 2, H / 2).scale(0.7));

    // Copy nodes/links for simulation
    const nodes = data.nodes.map(n => ({ ...n }));
    const links = data.edges.map(e => ({ ...e }));

    // Force simulation
    const sim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(80).strength(0.4))
      .force('charge', d3.forceManyBody().strength(-200))
      .force('center', d3.forceCenter(0, 0))
      .force('collision', d3.forceCollide().radius(d => (NODE_RADIUS[d.type] || 12) + 8));
    simRef.current = sim;

    // Edges
    const link = g.append('g').attr('class', 'links')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('class', 'edge')
      .attr('stroke', '#1e2535')
      .attr('stroke-width', 1.2)
      .attr('marker-end', 'url(#arrow)');

    // Edge labels
    const edgeLabel = g.append('g').attr('class', 'edge-labels')
      .selectAll('text')
      .data(links)
      .join('text')
      .attr('class', 'edge-label')
      .attr('fill', '#334155')
      .attr('font-size', '8px')
      .attr('font-family', 'IBM Plex Mono, monospace')
      .attr('text-anchor', 'middle')
      .text(d => d.label);

    // Node groups
    const node = g.append('g').attr('class', 'nodes')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .attr('class', 'node-group')
      .call(d3.drag()
        .on('start', (e, d) => {
          if (!e.active) sim.alphaTarget(0.3).restart();
          d.fx = d.x; d.fy = d.y;
        })
        .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
        .on('end', (e, d) => {
          if (!e.active) sim.alphaTarget(0);
          d.fx = null; d.fy = null;
        })
      )
      .on('click', (e, d) => { e.stopPropagation(); onNodeClick(d); });

    // Node circles
    node.append('circle')
      .attr('r', d => NODE_RADIUS[d.type] || 12)
      .attr('fill', d => NODE_COLORS[d.type] || '#64748b')
      .attr('fill-opacity', 0.15)
      .attr('stroke', d => NODE_COLORS[d.type] || '#64748b')
      .attr('stroke-width', 1.5)
      .attr('class', 'node-circle');

    // Node labels
    node.append('text')
      .attr('y', d => (NODE_RADIUS[d.type] || 12) + 11)
      .attr('text-anchor', 'middle')
      .attr('font-size', '9px')
      .attr('fill', '#64748b')
      .attr('font-family', 'IBM Plex Mono, monospace')
      .text(d => d.label.length > 10 ? d.label.slice(0, 10) + '…' : d.label);

    // Type badge
    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'central')
      .attr('font-size', '8px')
      .attr('fill', d => NODE_COLORS[d.type] || '#64748b')
      .text(d => ICONS[d.type] || '●');

    // Tick
    sim.on('tick', () => {
      link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);

      edgeLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2);

      node.attr('transform', d => `translate(${d.x},${d.y})`);
    });

    // Clear selection on bg click
    svg.on('click', () => onNodeClick(null));

  }, [data, onNodeClick]);

  useEffect(() => { draw(); }, [draw]);

  // Update selection highlight
  useEffect(() => {
    if (!svgRef.current) return;
    d3.select(svgRef.current).selectAll('.node-circle')
      .attr('stroke-width', d => {
        if (selectedNode && d.id === selectedNode.id) return 3;
        if (highlightedNodes.size > 0 && highlightedNodes.has(d.id)) return 2.5;
        return 1.5;
      })
      .attr('fill-opacity', d => {
        if (highlightedNodes.size === 0) return 0.15;
        if (highlightedNodes.has(d.id)) return 0.35;
        return 0.05;
      })
      .attr('stroke-opacity', d => {
        if (highlightedNodes.size === 0) return 1;
        if (highlightedNodes.has(d.id)) return 1;
        return 0.2;
      });
  }, [selectedNode, highlightedNodes]);

  return (
    <div className="graph-container">
      <div className="graph-legend">
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} className="legend-item">
            <span className="legend-dot" style={{ background: color }} />
            <span>{type}</span>
          </div>
        ))}
      </div>
      <div className="graph-hint">Drag nodes · Scroll to zoom · Click to inspect</div>
      <svg ref={svgRef} className="graph-svg" />
    </div>
  );
}
