<template>
  <v-container>
    <v-divider class="pb-2"></v-divider>
    <v-layout row wrap>
      <v-flex offset-md2 md6>
        <v-textarea
            outline
            name="input-7-4"
            label="Outline textarea"
            value="The Woodman set to work at once, and so sharp was his axe that the tree was soon chopped nearly through."
            readonly
        ></v-textarea>
      </v-flex>
    </v-layout>
    <v-layout row wrap>
      <v-flex md2>
        <v-subheader>{{param_name}}</v-subheader>
      </v-flex>
      <v-flex md6>
        <v-text-field
            label="Output"
            value="None"
        ></v-text-field>
      </v-flex>
    </v-layout>
    <v-divider></v-divider>
  </v-container>
</template>
<script>
  export default {
    name: 'ParamView',
    props: {
      edge_name: String,
      param_name: String,
    }
  }
</script>
