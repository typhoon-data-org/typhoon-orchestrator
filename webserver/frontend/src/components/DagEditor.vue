<template>
  <v-container>
    <editor v-model="content" @init="editorInit" lang="yaml" theme="dracula" width="1000" height="400"></editor>
    <v-textarea
        name="input-7-1"
        label="Default style"
        v-model="tokens"
        hint="Hint text"
        rows="50"
    ></v-textarea>
  </v-container>
</template>

<script>

  import {syntactical_analysis} from "../scripts/ace_helper";

  export default {
    components: {
      editor: require('vue2-ace-editor'),
    },
    data: () => ({
      content: 'name: aaa',
      tokens: '[]'
    }),
    methods: {
      editorInit: function (editor) {
        require('brace/ext/language_tools'); //language extension prerequsite...
        require('brace/mode/html');
        require('brace/mode/yaml');    //language
        require('brace/mode/less');
        require('brace/theme/chrome');
        require('brace/theme/dracula');
        require('brace/snippets/javascript'); //snippet

        editor.on('change',() => {
          let a = syntactical_analysis(editor);
          this.tokens = JSON.stringify(a, null, 4);
        });
      }
    }
  }
</script>

<style>

</style>
