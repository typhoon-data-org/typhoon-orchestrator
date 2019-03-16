<template>
  <v-app dark>
    <v-toolbar app>
      <v-toolbar-title class="headline text-uppercase">
        <span>TYPHOON</span>
        <span class="font-weight-light">PROJECT</span>
      </v-toolbar-title>
      <v-toolbar-items class="hidden-sm-and-down">
        <v-btn @click="$router.push('connections')" flat>
          Connections
        </v-btn>
        <v-btn  @click="$router.push('dag-editor')" flat>DAG Editor</v-btn>
      </v-toolbar-items>
      <v-spacer></v-spacer>
      <v-btn
          flat
          href="https://github.com/vuetifyjs/vuetify/releases/latest"
          target="_blank"
      >
        <span class="mr-2">Latest Release</span>
      </v-btn>
    </v-toolbar>

    <v-content>
      <!--<HelloWorld/>-->
      <router-view></router-view>
      <!--<DagEditor></DagEditor>-->
    </v-content>

    <v-footer
        dark
        height="auto"
    >
      <v-card
          flat
          tile
          class="lighten-1 white--text text-xs-center"
      >
        <v-card-text>
          <!--<v-btn-->
              <!--v-for="icon in icons"-->
              <!--:key="icon"-->
              <!--class="mx-3 white&#45;&#45;text"-->
              <!--icon-->
          <!--&gt;-->
            <!--<v-icon size="24px">{{ icon }}</v-icon>-->
          <!--</v-btn>-->
        </v-card-text>

        <v-card-text class="white--text pt-0">
          <div>Typhoon Orchestrator: DEV MODE</div>
          Typhoon is a task orchestrator and workflow manager used to create asynchronous data pipelines that can be deployed to AWS Lambda/Fargate to be completely serverless.
          Development mode active.
        </v-card-text>

        <v-divider></v-divider>

        <v-card-text class="white--text">
          &copy;2018 — <strong>Vuetify</strong>
        </v-card-text>
      </v-card>
    </v-footer>
  </v-app>
</template>

<script>
  // import HelloWorld from './components/HelloWorld'
  import DagEditor from "./components/DagEditor";


  export default {
    name: 'App',
    components: {
      // HelloWorld,
      DagEditor,
    },
    data () {
      return {
        //
      }
    },
  }
</script>
