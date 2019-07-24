<template>
  <v-card class="svgContainer" flat>
    <svg v-if="nodes.length > 0" id="svgEl" :height="height + 50" :width="width"></svg>
    <h1 v-else>Errors!</h1>
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
      }
    },
    // computed: {
    //   width() {
    //     return this.$refs.container.width();
    //   }
    // },
    watch: {
      nodes: function() {
        this.drawDAG();
      },
      edges: function() {
        this.drawDAG();
      },
    },
    // mounted() {
    // },
    methods: {
      drawDAG: function () {
        d3.select("svg").selectAll("*").remove();
        let svg = d3.select("svg"),
          inner = svg.append("g"),
          zoom = d3.zoom().on("zoom", function () {
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