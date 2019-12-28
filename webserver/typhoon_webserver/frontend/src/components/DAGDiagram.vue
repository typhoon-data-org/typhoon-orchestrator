<template>
  <v-card class="svgContainer" flat>
    <h1 v-if="syntaxError" class="text-md-center"><v-icon x-large>error_outline</v-icon> Fix syntax errors to show DAG</h1>
    <h1 v-else-if="error" class="text-md-center"><v-icon x-large>error_outline</v-icon> {{ error }}</h1>
    <svg id="svgEl" :height="height + 50" width="100%"></svg>
  </v-card>
</template>

<script>
  import * as d3 from "d3";
  import * as dagreD3 from "dagre-d3";

  export default {
    name: "DAGDiagram",
    props: {
      nodes: Array,
      edges: Array,
    },
    data: () => {
      return {
        height: 500,
        width: 1000,
        error: false,
      }
    },
    computed: {
      // width() {
      //   return this.$refs.container.width();
      // }
    syntaxError () {
        return this.nodes.length === 0;
      }
    },
    watch: {
      nodes: {
        handler: function() {
          this.setError();
          this.drawDAG();
        },
        deep: true
      },
      edges: {
        handler: function() {
          this.setError();
          this.drawDAG();
        },
        deep: true
      },
    },
    // mounted() {
    // },
    methods: {
      setError: function () {
        let nodeLabels = this.nodes.map(x => x.label);
        if (new Set(nodeLabels).size !== this.nodes.length) this.error = "Nodes must have unique identifiers";
        else if (new Set(this.edges.map(x => x.label)).size !== this.edges.length) this.error = "Edges must have unique identifiers";
        else if (this.edges.filter(e => !nodeLabels.includes(e.source) || !nodeLabels.includes(e.destination)).length > 0)
          this.error = "Edges contain undeclared nodes";
        else this.error = false;
      },
      drawDAG: function () {
        d3.select("svg").selectAll("*").remove();

        if (this.error || this.nodes.length === 0) return;


        let svg = d3.select("svg"),
          inner = svg.append("g"),
          zoom = d3.zoom()
            .on("zoom", function () {
              inner.attr("transform", d3.event.transform);
          });
        svg.call(zoom);

        let render = new dagreD3.render();

        let g = new dagreD3.graphlib.Graph();
        g.setGraph({
          nodesep: 70,
          ranksep: 50,
          rankdir: "LR",
          marginx: 20,
          marginy: 20
        });

        this.nodes.forEach(node => g.setNode(node.label, {label: node.label}));
        this.edges.forEach(edge => g.setEdge(edge.source, edge.destination, {label: edge.label}));

        inner.call(render, g);

        let scalingMultiplier = 0.75,
          padding = 20,
          graphWidth = g.graph().width,
          graphHeight = g.graph().height,
          width = svg.style("width").replace("px", ""),
          hRatio = this.height / (graphHeight + padding),
          wRatio = width / (graphWidth + padding),
          scalingFactor = hRatio < wRatio ? hRatio * scalingMultiplier : wRatio * scalingMultiplier;
        let transform = d3.zoomIdentity
          .translate(
            (width.replace("px", "") - graphWidth * scalingFactor)/2,
            (this.height - g.graph().height * scalingFactor)/2
          )
          .scale(scalingFactor);
        inner
          .call(zoom.transform, transform)
      }
    }
  }
</script>

<style scoped>
.svgContainer >>> #svgEl rect{
  fill: #999;
  stroke: #000;
  stroke-width: 1.5px;
}
.svgContainer >>> #svgEl text {
  font-weight: 300;
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serf,serif;
  font-size: 14px;
}

.svgContainer >>> #svgEl path{
  stroke: #000;
  stroke-width: 1.5px;
}
.svgContainer >>> .tipsy div {
  font-size: 1.5em;
  font-weight: bold;
  color: #000000;
  margin: 0;
}
</style>