<template>
  <v-container grid-list-md>
    <v-layout row wrap>
      <v-flex xs9>
        <v-alert
            v-model="userPackagesError"
            dismissible
            type="error"
        >
          Error refreshing backend
        </v-alert>
      </v-flex>
    </v-layout>
    <v-layout row wrap>
      <v-flex>
        <v-btn color="success" v-bind:disabled="!editingExistingDAG" v-on:click="saveCode">
          <v-progress-circular v-if="savingCode" :size="25" indeterminate></v-progress-circular>
          <v-icon v-else left>save</v-icon>
          <u>s</u>ave
        </v-btn>
        <v-btn color="info" v-on:click="reloadBackend">
          <v-progress-circular v-if="loadingCode" :size="25" indeterminate></v-progress-circular>
          <v-icon v-else left>refresh</v-icon>
          <u>r</u>eload
        </v-btn>
      </v-flex>
    </v-layout>
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
          Syntax errors: {{ first_error }}
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
        <editor ref="dag_editor" v-model="content" @init="editorInit" lang="yaml" theme="tomorrow_night" width="1000" height="550"></editor>
      </v-flex>
    </v-layout>

    <v-layout row wrap>
      <v-btn v-on:click="copyEditorContentsToClipboard" outline fab>
        <v-icon color="white">assignment</v-icon>
      </v-btn>
      <v-checkbox v-model="disable_syntax_checking" label="Disable syntax checks (alt+D)"></v-checkbox>
    </v-layout>

    <v-layout row wrap>
      <v-flex offset-md1 md5>
        <v-text-field
            label="Filter edges by name (alt+F)"
            ref="filterEdgesTextField"
            v-model="filter_exp"
            prepend-icon="search"
        ></v-text-field>
      </v-flex>
      <v-flex offset-md2>
        <DagConfigDialog></DagConfigDialog>
      </v-flex>
    </v-layout>

    <div>
      <v-tabs fixed-tabs>
        <v-tab ripple>
          Docs
        </v-tab>
        <v-tab-item>
          <DocsView/>
        </v-tab-item>

        <v-tab ripple>
          Edges
        </v-tab>
        <v-tab-item>
          <v-card flat>
            <div v-if="Object.entries(edges).length > 0 && edges.constructor === Object">
              <v-container v-for="(edge, edge_name) in edges" v-bind:key="edge_name">
                <EdgeTester v-bind:edge_name="edge_name" v-bind:edge="edge"></EdgeTester>
              </v-container>
            </div>
            <v-container v-else-if="errors">
              <h1 class="text-md-center"><v-icon x-large>error_outline</v-icon> Fix syntax errors to show edges</h1>
            </v-container>
            <v-container v-else>
              <h1 class="text-md-center">No edges matching filter criteria</h1>
            </v-container>
          </v-card>
        </v-tab-item>
      </v-tabs>
    </div>

    <v-snackbar
        v-model="snackbar_clipboard"
      :timeout="1500"
      :top="true"
    >
      Copied code to clipboard
      <v-btn
        flat
        @click="snackbar_clipboard = false"
      >
        Close
      </v-btn>
    </v-snackbar>
    <v-snackbar
        v-model="snackbar_code"
        :timeout="1500"
        :top="true"
    >
      Reloaded backend
      <v-btn
          flat
          @click="snackbar_code = false"
      >
        Close
      </v-btn>
    </v-snackbar>
    <v-snackbar
        v-model="snackbar_loaded_dag"
        :timeout="1500"
        :top="true"
    >
      Loaded DAG {{ currentDAGFilename }}
      <v-btn
          flat
          @click="snackbar_loaded_dag = false"
      >
        Close
      </v-btn>
    </v-snackbar>
    <SidebarDags v-on:dag-file-selected="setDagFile"></SidebarDags>
  </v-container>
</template>

<script>

  import {copyToClipboard, firstSyntaxError, syntactical_analysis} from "../scripts/ace_helper";
  import {get_completions} from "../scripts/completer";
  import EdgeTester from "./EdgeTester";
  import DagConfigDialog from "./DagConfigDialog";
  import {EDGE_CONFIGS} from "../scripts/analize_dag";
  import DocsView from "./DocsView";
  import SidebarDags from "./SidebarDags";
  import {get_docobject} from "../scripts/doc_helper";

  export default {
    components: {
      SidebarDags,
      DocsView,
      DagConfigDialog,
      EdgeTester,
      editor: require('vue2-ace-editor'),
    },
    data: () => ({
      content: 'name: example_dag\nschedule_interval: rate(5 minutes)\nnodes:\n\n',
      tokens: '[]',
      disable_syntax_checking: false,
      errors: false,
      filter_exp: '',
      snackbar_clipboard: false,
      snackbar_code: false,
      snackbar_loaded_dag: false,
      userPackagesError: false,
      loadingCode: false,
      savingCode: false,
    }),
    computed: {
      typhoonFunctionModules() {
        return this.$store.state.dagEditor.typhoonFunctionModules;
      },
      typhoonFunctions() {
        return this.$store.state.dagEditor.typhoonFunctions;
      },
      userDefinedFunctionModules() {
        return this.$store.state.dagEditor.userDefinedFunctionModules;
      },
      userDefinedFunctions() {
        return this.$store.state.dagEditor.userDefinedFunctions;
      },
      edges() {
        if (self.filter_exp === '') {
          return this.$store.state.dagEditor.edges;
        } else {
          let result = {};
          for (let key in this.$store.state.dagEditor.edges) {
            if (this.$store.state.dagEditor.edges.hasOwnProperty(key) && key.includes(this.filter_exp)) {
              result[key] = this.$store.state.dagEditor.edges[key];
            }
          }
          return result;
        }
      },
      first_error() {
        return firstSyntaxError();
      },
      connection_ids () {
        return this.$store.state.connections.items.map(x => x.conn_id);
      },
      variable_ids () {
        return this.$store.state.variables.items.map(x => x.id);
      },
      currentDAGFilename () {
        return this.$store.state.dagEditor.currentDAGFilename;
      },
      savedCode () {
        return this.$store.state.dagEditor.savedCode
      },
      editingExistingDAG () {
        return this.currentDAGFilename !== null;
      }
    },
    methods: {
      editorInit: function (editor) {
        this.fetchTyphoonPackageInfo();
        this.getConnections();
        this.getVariables();

        require('brace/ext/language_tools'); //language extension prerequsite...
        require('brace/mode/html');
        require('brace/mode/yaml');    //language
        require('brace/mode/less');
        require('brace/theme/chrome');
        require('brace/theme/tomorrow_night');
        require('brace/snippets/javascript'); //snippet

        let ace = require('brace');
        require("brace/ext/language_tools");
        let langTools = ace.acequire("ace/ext/language_tools");
        editor.setOptions({
          enableBasicAutocompletion: true,
          enableSnippets: true,
          enableLiveAutocompletion: true,
          tabSize: 2,
          showPrintMargin: false,
        });

        if (this.savedCode) {
          this.content = this.savedCode;
        }

        let parent = this;
        let customCompleter = {
          getCompletions: (editor, session, pos, prefix, callback) => {
            let wordList = get_completions(
              editor, session, pos, prefix,
              parent.typhoonFunctionModules,
              parent.$store.state.dagEditor.typhoonTransformationModules,
              parent.$store.getters.typhoonFunctionNames,
              parent.$store.getters.typhoonTransformationNames,
              parent.userDefinedFunctionModules,
              parent.$store.state.dagEditor.userDefinedTransformationModules,
              parent.$store.getters.userDefinedFunctionNames,
              parent.$store.getters.userDefinedTransformationNames,
              parent.connection_ids,
              parent.variable_ids,
            );
            wordList = wordList || [];
            callback(null,
              wordList.map(word => ({name: word, value: word, meta: 'static'}))
            );
          }
        };
        langTools.setCompleters([customCompleter]);
        // langTools.addCompleter(customCompleter);

        editor.on('change', () => {
          if (this.disable_syntax_checking) {
            editor.session.setAnnotations([]);
          } else {
            // let a = syntactical_analysis(editor);
            // this.tokens = JSON.stringify(a, null, 4);
            this.errors = !syntactical_analysis(editor);
            if (!this.errors) {
              this.$store.commit('setEdges', EDGE_CONFIGS);
            } else {
              this.$store.commit('setEdges', {});
            }
          }
        });
        editor.session.selection.on('changeCursor', () => {
          let docObj = get_docobject(editor.session.getLine(editor.getCursorPosition().row), editor.session, editor.getCursorPosition());
          this.$store.commit('setCurrentDocObject', docObj);
        });
      },
      copyEditorContentsToClipboard: function (event) {
        let code = this.$refs.dag_editor.editor.getValue();
        copyToClipboard(code);
        this.snackbar_clipboard = true;
      },
      reloadBackend: function (event) {
        this.fetchTyphoonPackageInfo();
        this.snackbar_code = true;
      },
      fetchTyphoonPackageInfo: function () {
        const baseURI = 'http://localhost:5000/';
        this.loadingCode = true;
        this.$http.get(baseURI + 'typhoon-modules')
          .then((result) => {
            this.$store.commit('setTyphoonFunctionModules', result.data['functions']);
            this.$store.commit('setTyphoonTransformationModules', result.data['transformations']);
          });

        this.$http.get(baseURI + 'typhoon-package-trees')
          .then((result) => {
            this.$store.commit('setTyphoonFunctions', result.data['functions']);
            this.$store.commit('setTyphoonTransformations', result.data['transformations']);
          });

        this.$http.get(baseURI + 'typhoon-user-defined-modules')
          .then((result) => {
            this.$store.commit('setUserDefinedFunctionModules', result.data['functions']);
            this.$store.commit('setUserDefinedTransformationModules', result.data['transformations']);
          })
          .catch((error) => {
            this.userPackagesError = true;
          });

        this.$http.get(baseURI + 'typhoon-user-defined-package-trees')
          .then((result) => {
            this.$store.commit('setUserDefinedFunctions', result.data['functions']);
            this.$store.commit('setUserDefinedTransformations', result.data['transformations']);
            this.userPackagesError = false;
            this.loadingCode = false;
          })
          .catch((error) => {
            this.userPackagesError = true;
            this.loadingCode = false;
          });
      },

      getConnections: function () {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'connections', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.$store.commit('setConnections', result.data);
          });
      },

      getVariables: function () {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'variables', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.$store.commit('setVariables', result.data);
          });
      },

      setDagFile: function (evt) {
        this.content = evt.contents;
        this.snackbar_loaded_dag = true;
      },

      saveCode: function () {
        this.savingCode = true;
        this.$api.saveDAGCode({code: this.content}, {filename: this.currentDAGFilename})
          .then(() => this.savingCode = false)
      },

      setFocusFilterEdgesTextField: function() {
        this.$refs.filterEdgesTextField.focus();
      }
    },

    mounted: function () {
      window.addEventListener('keyup', (evt) => {
        if (evt.altKey && evt.code === 'KeyS' && this.currentDAGFilename !== null) {
          this.saveCode();
        } else if (evt.altKey && evt.code === 'KeyR' && this.currentDAGFilename !== null) {
          this.reloadBackend()
        } else if (evt.altKey && evt.code === 'KeyC' && this.currentDAGFilename !== null) {
          this.copyEditorContentsToClipboard()
        } else if (evt.altKey && evt.code === 'KeyF' && this.currentDAGFilename !== null) {
          this.setFocusFilterEdgesTextField();
        } else if (evt.altKey && evt.code === 'Digit1') {
          let show = this.$store.state.dagEditor.showSidebarDAGs;
          this.$store.commit('setShowSidebarDAGs', !show);
        }
        evt.preventDefault();
      });
    },

    beforeDestroy() {
      this.$store.commit('setSavedCode', this.$refs.dag_editor.editor.getValue());
    }
  }
</script>

