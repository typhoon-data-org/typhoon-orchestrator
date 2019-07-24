<template>
  <v-card ref="container" flat>
    <div class ="svgContainer">
      <svg id="svgEl" :height="height + 50" :width="width"></svg>
    </div>
  </v-card>
</template>

<script>
  import * as d3 from "d3";
  import * as dagreD3 from "dagre-d3";

  export default {
    name: "DAGDiagram",
    // props: {
    //   nodes: Array,
    //   edges: Array,
    // },
    data: () => {
      return {
        nodes: [
          {label: 'send_small_tables'},
          {label: 'send_medium_tables'},
          {label: 'send_large_tables'},
          {label: 'extract_data'},
          {label: 'write_data'},
          {label: 'notify'},
        ],
        edges: [
          {source: 'send_small_tables', destination: 'extract_data', label: 'e1'},
          {source: 'send_medium_tables', destination: 'extract_data', label: 'e2'},
          {source: 'send_large_tables', destination: 'extract_data', label: 'e3'},
          {source: 'extract_data', destination: 'write_data', label: 'write_snowflake'},
          {source: 'extract_data', destination: 'notify', label: 'slack'},
        ],
        height: 500,
        width: 1000,
        dag: [],
      }
    },
    // computed: {
    //   width() {
    //     return this.$refs.container.width();
    //   }
    // },
    mounted() {
      let svg = d3.select("svg"),
        inner = svg.append("g"),
        zoom = d3.zoom().on("zoom", function() {
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

      // g.setNode("A", {label: "A"});
      // g.setNode("B", {label: "B"});
      // g.setNode("C", {label: "C"});
      // g.setEdge("A", "B", {label: "label"});
      // g.setEdge("A", "C", {label: "label"});
      this.nodes.forEach(node => g.setNode(node.label, {label: node.label}));
      this.edges.forEach(edge => g.setEdge(edge.source, edge.destination, {label: edge.label}));

      inner.call(render, g);
    }
  }
</script>

<style scoped>
*{
  margin: 0;
  padding: 0;
}
/*body{*/
  /*height: 100%;*/
/*}*/
.svgContainer{
  position: absolute;
  /*width: 100%;*/
  /*height: 100%;*/
}
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