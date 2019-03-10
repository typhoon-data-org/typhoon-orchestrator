<template>
  <v-container>
    <!--<v-layout row wrap>-->
    <!--<v-divider></v-divider>-->
    <!--</v-layout>-->
    <v-card elevation="8">
      <v-layout row wrap class="pt-2">
        <v-flex md2>
          <v-subheader>Edge</v-subheader>
        </v-flex>
        <v-flex md7>
          <v-text-field
              label="Name"
              v-bind:value="edge_name"
              readonly
              disabled
          ></v-text-field>
        </v-flex>
      </v-layout>
      <v-layout row wrap>
        <v-flex md2>
          <v-subheader>$SOURCE</v-subheader>
        </v-flex>
        <v-flex md7>
          <v-text-field
              label="Data"
              v-model="source_data"
          ></v-text-field>
        </v-flex>
        <v-flex offset-md1 md2>
            <v-btn-toggle v-model="source_value_type" mandatory>
              <v-btn flat value="str">
                Str
              </v-btn>
              <v-btn flat value="eval">
                Eval Exp
              </v-btn>
            </v-btn-toggle>
          </v-flex>
      </v-layout>

      <v-container v-for="(param, param_name) in edge" v-bind:key="param_name" class="pt-0 pb-0">
        <ParamView v-bind:edge_name="edge_name" v-bind:param_name="param_name" v-bind:param="param"/>
      </v-container>
      <!--<ParamView param_name="query"/>-->
      <v-layout  row wrap class="pb-2">
        <v-flex>
          <v-btn @click="getRunTransformationResults" color="success">Test</v-btn>
        </v-flex>
      </v-layout>
      <!--<v-layout row wrap>-->
      <!--<v-divider></v-divider>-->
      <!--</v-layout>-->
    </v-card>
  </v-container>
</template>

<script>
  import ParamView from "./ParamView";

  export default {
    name: "EdgeTester",
    components: {ParamView},
    data: () => ({
      source_data: '',
      source_value_type: 'str',
    }),
    props: {
      edge_name: String,
      edge: Object,
    },
    methods: {
      getRunTransformationResults: function() {
        const baseURI = 'http://localhost:5000/';
        let body = {
          edge: this.edge,
          source: this.source_data,
          eval_source: this.source_value_type === 'eval',
          dag_config: {
            execution_date: this.$store.getters.executionDatetime,
            dag_name: this.$store.state.dag_name,
          },
        };
        this.$http.post(baseURI + 'run-transformations', body)
          .then((result) => {
            Object.keys(result.data).forEach(key => {
              this.$store.commit('setTransformationResult',
                {
                  edge_name: this.edge_name,
                  param_name: key,
                  result: result.data[key],
                });
            });
          });
      }
    }
  }
</script>

