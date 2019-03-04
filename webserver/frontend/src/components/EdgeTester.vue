<template>
  <v-container>
    <!--<v-layout row wrap>-->
    <!--<v-divider></v-divider>-->
    <!--</v-layout>-->
    <v-card elevation="8">
      <v-layout row wrap>
        <v-flex md2>
          <v-subheader>Edge</v-subheader>
        </v-flex>
        <v-flex md6>
          <v-text-field
              label="Name"
              v-bind:value="edge_name"
              readonly
          ></v-text-field>
        </v-flex>
      </v-layout>
      <v-layout row wrap>
        <v-flex md2>
          <v-subheader>$SOURCE</v-subheader>
        </v-flex>
        <v-flex md6>
          <v-text-field
              label="Data"
              value="None"
          ></v-text-field>
        </v-flex>
      </v-layout>
      <ParamView param_name="table_name"/>
      <ParamView param_name="query"/>
      <v-layout row wrap>
        <v-flex pb12>
          <v-btn color="success">Test</v-btn>
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
    props: {
      edge_name: String,
      adapter: Object,
    }
  }
</script>

