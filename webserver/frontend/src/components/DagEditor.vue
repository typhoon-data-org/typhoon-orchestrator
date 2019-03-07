<template>
  <v-container grid-list-md>
    <v-layout row wrap>
      <v-flex xs10>
        <v-alert
            :value="true"
            type="warning"
            v-if="disable_syntax_checking"
        >
          Syntax checking disabled
        </v-alert>
        <v-alert
            :value="true"
            type="error"
            v-else-if="errors"
        >
          Syntax errors
        </v-alert>
        <v-alert
            :value="true"
            type="success"
            v-else
        >
          All good
        </v-alert>
      </v-flex>
    </v-layout>

    <v-layout row wrap>
      <v-flex xs12>
        <editor ref="dag_editor" v-model="content" @init="editorInit" lang="yaml" theme="dracula" width="1000" height="400"></editor>
      </v-flex>
    </v-layout>

    <v-layout row wrap>
      <v-btn v-on:click="copyEditorContentsToClipboard" outline fab>
        <v-icon color="white">assignment</v-icon>
      </v-btn>
      <v-checkbox v-model="disable_syntax_checking" label="Disable syntax checks"></v-checkbox>
    </v-layout>

    <v-layout row wrap>
      <v-flex offset-md1 md5>
        <v-text-field
            label="Filter edges by name"
            v-model="filter_exp"
            prepend-icon="search"
        ></v-text-field>
      </v-flex>
      <v-flex offset-md2>
        <DagConfigDialog></DagConfigDialog>
      </v-flex>
    </v-layout>

    <div v-if="Object.entries(edges).length > 0 && edges.constructor === Object">
      <v-container v-for="(edge, edge_name) in edges" v-bind:key="edge_name">
        <EdgeTester v-bind:edge_name="edge_name" v-bind:edge="edge"></EdgeTester>
      </v-container>
    </div>
    <v-container v-else>
      <h1 class="text-md-center">No edges matching filter criteria</h1>
    </v-container>

    <!--<v-textarea-->
    <!--name="input-7-1"-->
    <!--label="Default style"-->
    <!--v-model="tokens"-->
    <!--hint="Hint text"-->
          <!--rows="50"-->
      <!--&gt;</v-textarea>-->
  </v-container>
</template>

<script>

  import {copyToClipboard, syntactical_analysis} from "../scripts/ace_helper";
  import {get_completions} from "../scripts/completer";
  import EdgeTester from "./EdgeTester";
  import DagConfigDialog from "./DagConfigDialog";

  export default {
    components: {
      DagConfigDialog,
      EdgeTester,
      editor: require('vue2-ace-editor'),
    },
    data: () => ({
      content: 'name: example_dag\nschedule-interval: "* * * * * *"\nnodes:\n  e1:\n    function: typhoon.aa.bb' +
        '\n    config:\n' +
        '      table_name => APPLY: $SOURCE\n' +
        '      query => APPLY:\n' +
        '        - str("SELECT * FROM {{ table_name }} WHERE creation_date=\'{{ date_string }}\'")\n' +
        '        - typhoon.templates.render(template=$1, table_name=$SOURCE, date_string=$DAG_CONFIG.ds)\n' +
        '      batch_size: 2\n\n' +
        '  ',
      tokens: '[]',
      disable_syntax_checking: false,
      errors: false,
      filter_exp: '',
    }),
    computed: {
      typhoonModules() {
        return this.$store.state.typhoonModules;
      },
      typhoonFunctions() {
        return this.$store.state.typhoonFunctions;
      },
      userDefinedModules() {
        return this.$store.state.userDefinedModules;
      },
      userDefinedFunctions() {
        return this.$store.state.userDefinedFunctions;
      },
      edges() {
        if (self.filter_exp === '') {
          return this.$store.state.edges;
        } else {
          let result = {};
          for (let key in this.$store.state.edges) {
            if (this.$store.state.edges.hasOwnProperty(key) && key.includes(this.filter_exp)) {
              result[key] = this.$store.state.edges[key];
            }
          }
          return result;
        }
      }
    },
    methods: {
      editorInit: function (editor) {
        this.fetchTyphoonPackageInfo();

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
        editor.setOptions({
          enableBasicAutocompletion: true,
          enableSnippets: true,
          enableLiveAutocompletion: true,
          tabSize: 2,
        });
        let parent = this;
        let customCompleter = {
          getCompletions: (editor, session, pos, prefix, callback) => {
            let wordList = get_completions(editor, session, pos, prefix, parent.typhoonModules, parent.typhoonFunctions, parent.userDefinedModules, parent.userDefinedFunctions);
            callback(null,
              wordList.map(word => ({name: word, value: word, meta: 'static'}))
            );
          }
        };
        langTools.setCompleters([customCompleter]);
        // langTools.addCompleter(customCompleter);

        editor.on('change',() => {
          if (this.disable_syntax_checking) {
            editor.session.setAnnotations([]);
          } else {
            // let a = syntactical_analysis(editor);
            // this.tokens = JSON.stringify(a, null, 4);
            this.errors = !syntactical_analysis(editor);
          }
        });
      },
      copyEditorContentsToClipboard: function (event) {
        let code = this.$refs.dag_editor.editor.getValue();
        copyToClipboard(code);
      },
      fetchTyphoonPackageInfo: function () {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'typhoon-modules')
          .then((result) => {
            this.$store.commit('setTyphoonModules', result.data['functions']);
          });

        this.$http.get(baseURI + 'typhoon-package-trees')
          .then((result) => {
            this.$store.commit('setTyphoonFunctions', result.data['functions']);
          });

        this.$http.get(baseURI + 'typhoon-user-defined-modules')
          .then((result) => {
            this.$store.commit('setUserDefinedModules', result.data['functions']);
          });

        this.$http.get(baseURI + 'typhoon-user-defined-package-trees')
          .then((result) => {
            this.$store.commit('setUserDefinedFunctions', result.data['functions']);
          });
      }
    }
  }
</script>

<style>

</style>
