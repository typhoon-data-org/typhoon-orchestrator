<template>
  <svg id="svg" :height="height + 50" :width="width"></svg>
</template>

<script>
  import * as d3 from "d3";
  import * as dagreD3 from "dagre-d3";

  export default {
    name: "DAGDiagram",
    data: () => {
      return {
        height: 500,
        width: 1200,
        dag: [],
      }
    },
    mounted() {
      let g = new dagreD3.graphlib.Graph().setGraph({});
      let states = [ "CLOSED", "LISTEN", "SYN RCVD", "SYN SENT",
        "ESTAB", "FINWAIT-1", "CLOSE WAIT", "FINWAIT-2",
        "CLOSING", "LAST-ACK", "TIME WAIT" ];

// Automatically label each of the nodes
      states.forEach(function(state) { g.setNode(state, { label: state }); });

// Set up the edges
      g.setEdge("CLOSED",     "LISTEN",     { label: "open" });
      g.setEdge("LISTEN",     "SYN RCVD",   { label: "rcv SYN" });
      g.setEdge("LISTEN",     "SYN SENT",   { label: "send" });
      g.setEdge("LISTEN",     "CLOSED",     { label: "close" });
      g.setEdge("SYN RCVD",   "FINWAIT-1",  { label: "close" });
      g.setEdge("SYN RCVD",   "ESTAB",      { label: "rcv ACK of SYN" });
      g.setEdge("SYN SENT",   "SYN RCVD",   { label: "rcv SYN" });
      g.setEdge("SYN SENT",   "ESTAB",      { label: "rcv SYN, ACK" });
      g.setEdge("SYN SENT",   "CLOSED",     { label: "close" });
      g.setEdge("ESTAB",      "FINWAIT-1",  { label: "close" });
      g.setEdge("ESTAB",      "CLOSE WAIT", { label: "rcv FIN" });
      g.setEdge("FINWAIT-1",  "FINWAIT-2",  { label: "rcv ACK of FIN" });
      g.setEdge("FINWAIT-1",  "CLOSING",    { label: "rcv FIN" });
      g.setEdge("CLOSE WAIT", "LAST-ACK",   { label: "close" });
      g.setEdge("FINWAIT-2",  "TIME WAIT",  { label: "rcv FIN" });
      g.setEdge("CLOSING",    "TIME WAIT",  { label: "rcv ACK of FIN" });
      g.setEdge("LAST-ACK",   "CLOSED",     { label: "rcv ACK of FIN" });
      g.setEdge("TIME WAIT",  "CLOSED",     { label: "timeout=2MSL" });

// Set some general styles
      g.nodes().forEach(function(v) {
        let node = g.node(v);
        node.rx = node.ry = 5;
      });

// Add some custom colors based on state
      g.node('CLOSED').style = "fill: #f77";
      g.node('ESTAB').style = "fill: #7f7";

      let svg = d3.select("svg"),
        inner = svg.append("g");

// Set up zoom support
      let zoom = d3.zoom().on("zoom", function() {
        inner.attr("transform", d3.event.transform);
      });
      svg.call(zoom);

// Create the renderer
      let render = new dagreD3.render();

// Run the renderer. This is what draws the final graph.
      render(inner, g);

// Center the graph
      let initialScale = 0.75;
      svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));

      svg.attr('height', g.graph().height * initialScale + 40);
    }
  }
</script>

<style scoped>

</style>