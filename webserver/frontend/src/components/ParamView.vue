<template>
  <v-container>
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
        <v-subheader>{{param_name}}</v-subheader>
      </v-flex>
      <v-flex md6>
        <v-text-field
            label="Output"
            v-bind:value="param.apply ? `None` : param.contents"
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
      param_name: String,
      param: Object,
    },
    computed: {
      transformations_text() {
        if (!this.param.apply) {
          return null;
        }
        return this.param.contents.map(line => '- ' + line).join('\n');
      }
    }
  }
</script>
