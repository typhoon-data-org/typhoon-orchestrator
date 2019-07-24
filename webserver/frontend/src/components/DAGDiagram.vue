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
    data: () => {
      return {
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
      let workers = {
        "identifier": {
          "consumers": 2,
          "count": 20
        },
        "lost-and-found": {
          "consumers": 1,
          "count": 1,
          "inputQueue": "identifier",
          "inputThroughput": 50
        },
        "monitor": {
          "consumers": 1,
          "count": 0,
          "inputQueue": "identifier",
          "inputThroughput": 50
        },
        "meta-enricher": {
          "consumers": 4,
          "count": 9900,
          "inputQueue": "identifier",
          "inputThroughput": 50
        },
        "geo-enricher": {
          "consumers": 2,
          "count": 1,
          "inputQueue": "meta-enricher",
          "inputThroughput": 50
        },
        "elasticsearch-writer": {
          "consumers": 0,
          "count": 9900,
          "inputQueue": "geo-enricher",
          "inputThroughput": 50
        }
      };

      // let g = new dagreD3.graphlib.Graph().setGraph({});
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

      g.setNode("A", {label: "A"});
      g.setNode("B", {label: "B"});
      g.setNode("C", {label: "C"});
      g.setEdge("A", "B", {label: "label"});
      g.setEdge("A", "C", {label: "label"});
      inner.call(render, g);
      return;

      for (var id in workers) {
        var worker = workers[id];
        var className = worker.consumers ? "running" : "stopped";
        if (worker.count > 10000) {
          className += " warn";
        }
        var html = "<div>";
        html += "<span class=status></span>";
        html += "<span class=consumers>"+worker.consumers+"</span>";
        html += "<span class=name>"+id+"</span>";
        html += "<span class=queue><span class=counter>"+worker.count+"</span></span>";
        html += "</div>";
        g.setNode(id, {
          labelType: "html",
          label: html,
          rx: 5,
          ry: 5,
          padding: 0,
          class: className
        });
        if (worker.inputQueue) {
          g.setEdge(worker.inputQueue, id, {
            label: worker.inputThroughput + "/s",
            width: 40
          });
        }
      }
      inner.call(render, g);
      // Zoom and scale to fit
      let graphWidth = g.graph().width + 80;
      let graphHeight = g.graph().height + 40;
      let width = parseInt(svg.style("width").replace(/px/, ""));
      let height = parseInt(svg.style("height").replace(/px/, ""));
      let zoomScale = Math.min(width / graphWidth, height / graphHeight);
      let translateX = (width / 2) - ((graphWidth * zoomScale) / 2)
      let translateY = (height / 2) - ((graphHeight * zoomScale) / 2);
      // let svgZoom = isUpdate ? svg.transition().duration(500) : svg;
      svg.call(zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(zoomScale));

    }
  }
</script>

<style scoped>
*{
  margin: 0;
  padding: 0;
}
body{
  height: 100%;
}
.svgContainer{
  position: absolute;
  width: 100%;
  height: 100%;
}
.svgContainer >>> #svgEl rect{
  fill: #999;
  stroke: #000;
  stroke-width: 1.5px;
}
.svgContainer >>> #svgEl text {
  font-weight: 300;
  font-family: "Helvetica Neue", Helvetica, Arial, sans-serf;
  font-size: 14px;
}

.svgContainer >>> #svgEl path{
  stroke: #000;
  stroke-width: 1.5px;
}
.svgContainer >>> .tipsy div {
  font-size: 1.5em;
  font-weight: bold;
  color: #60b1fc;
  margin: 0;
}
</style>