<template>
  <v-container  class="pt-0 pb-1">
    <v-expansion-panel>
    <v-expansion-panel-content>
      <template v-slot:header>
        <div>{{param_name}}</div>
      </template>
      <v-card>
        <v-divider class="pb-2"></v-divider>
        <v-layout v-if="param.apply" row wrap>
          <v-flex offset-md2 md10>
            <v-textarea
                outline
                name="input-7-4"
                label="Outline textarea"
                v-bind:value="transformations_text"
                readonly
            ></v-textarea>
          </v-flex>
        </v-layout>
        <v-layout row wrap>
          <v-flex md2>
            <v-subheader>Output</v-subheader>
          </v-flex>
          <v-flex md6>
            <v-text-field
                label="Output"
                v-bind:value="param_output"
                readonly
            ></v-text-field>
          </v-flex>
        </v-layout>
        <v-divider></v-divider>
      </v-card>
    </v-expansion-panel-content>
  </v-expansion-panel>
  </v-container>
</template>
<script>
  export default {
    name: 'ParamView',
    props: {
      edge_name: String,
      param_name: String,
      param: Object,
    },
    computed: {
      transformations_text() {
        if (!this.param.apply) {
          return null;
        }
        return this.param.contents.map(line => '- ' + line).join('\n');
      },
      param_output() {
        if (!this.param.apply) {
          return this.param.contents;
        }
        return this.$store.state.edges[this.edge_name][this.param_name].transformation_result;
      }
    }
  }
</script>
