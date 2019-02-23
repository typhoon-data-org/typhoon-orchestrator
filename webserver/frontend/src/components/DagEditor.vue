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
  import {get_completions} from "../scripts/completer";

  export default {
    components: {
      editor: require('vue2-ace-editor'),
    },
    data: () => ({
      content: 'name: aaa\nschedule-interval: "* * * * * *"',
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

        let ace = require('brace');
        require("brace/ext/language_tools");
        let langTools = ace.acequire("ace/ext/language_tools");
        editor.setOptions({enableBasicAutocompletion: true, enableSnippets: true, enableLiveAutocompletion: true});
        let customCompleter = {
          getCompletions: function (editor, session, pos, prefix, callback) {
            let wordList = get_completions(editor, session, pos, prefix);
            callback(null,
              // [{name: 'TODO', value: 'TODO', meta: 'TODO'}]
              wordList.map(word => ({name: word, value: word, meta: 'static'}))
            );
          }
        };
        langTools.setCompleters([customCompleter]);
        // langTools.addCompleter(customCompleter);

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
