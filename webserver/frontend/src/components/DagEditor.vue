<template>
  <v-container>
    <!--<v-layout-->
      <!--text-xs-center-->
      <!--wrap-->
    <!--&gt;-->
      <!--<v-flex xs12>-->
        <!--<v-img-->
          <!--:src="require('../assets/logo.svg')"-->
          <!--class="my-3"-->
          <!--contain-->
          <!--height="200"-->
        <!--&gt;</v-img>-->
      <!--</v-flex>-->

      <!--<v-flex mb-4>-->
        <!--<h1 class="display-2 font-weight-bold mb-3">-->
          <!--welcome to vuetify-->
        <!--</h1>-->
        <!--<p class="subheading font-weight-regular">-->
          <!--for help and collaboration with other vuetify developers,-->
          <!--<br>please join our online-->
          <!--<a href="https://community.vuetifyjs.com" target="_blank">discord community</a>-->
        <!--</p>-->
      <!--</v-flex>-->

      <!--<v-flex-->
        <!--mb-5-->
        <!--xs12-->
      <!--&gt;-->
        <!--<h2 class="headline font-weight-bold mb-3">what's next?</h2>-->

        <!--<v-layout justify-center>-->
          <!--<a-->
            <!--v-for="(next, i) in whatsnext"-->
            <!--:key="i"-->
            <!--:href="next.href"-->
            <!--class="subheading mx-3"-->
            <!--target="_blank"-->
          <!--&gt;-->
            <!--{{ next.text }}-->
          <!--</a>-->
        <!--</v-layout>-->
      <!--</v-flex>-->

      <!--<v-flex-->
        <!--xs12-->
        <!--mb-5-->
      <!--&gt;-->
        <!--<h2 class="headline font-weight-bold mb-3">important links</h2>-->

        <!--<v-layout justify-center>-->
          <!--<a-->
            <!--v-for="(link, i) in importantlinks"-->
            <!--:key="i"-->
            <!--:href="link.href"-->
            <!--class="subheading mx-3"-->
            <!--target="_blank"-->
          <!--&gt;-->
            <!--{{ link.text }}-->
          <!--</a>-->
        <!--</v-layout>-->
      <!--</v-flex>-->

      <!--<v-flex-->
        <!--xs12-->
        <!--mb-5-->
      <!--&gt;-->
        <!--<h2 class="headline font-weight-bold mb-3">ecosystem</h2>-->

        <!--<v-layout justify-center>-->
          <!--<a-->
            <!--v-for="(eco, i) in ecosystem"-->
            <!--:key="i"-->
            <!--:href="eco.href"-->
            <!--class="subheading mx-3"-->
            <!--target="_blank"-->
          <!--&gt;-->
            <!--{{ eco.text }}-->
          <!--</a>-->
        <!--</v-layout>-->
      <!--</v-flex>
    </v-layout>-->
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

  // import syntactical_analysis from "../scripts/ace_helper";

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
          // let ace_helper = require('../scripts/ace_helper');
          let a = syntactical_analysis(editor);
          this.tokens = JSON.stringify(a, null, 4);
        });
      }
    }
  }
</script>

<style>

</style>
