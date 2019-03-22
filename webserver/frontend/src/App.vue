<template>
  <v-app dark>
    <v-snackbar
        v-model="snackbar"
        :timeout="1500"
        :top="true"
    >
      {{ snackbar_message }}
      <v-btn
          flat
          @click="snackbar_clipboard = false"
      >
        Close
      </v-btn>
    </v-snackbar>
    <v-toolbar app>
      <v-toolbar-title class="headline text-uppercase">
        <span>TYPHOON</span>
        <span class="font-weight-light">PROJECT</span>
      </v-toolbar-title>
      <v-toolbar-items class="hidden-sm-and-down">
        <v-btn @click="$router.push('connections')" flat>Connections</v-btn>
        <v-btn @click="$router.push('variables')" flat>Variables</v-btn>
        <v-btn  @click="$router.push('dag-editor')" flat>DAG Editor</v-btn>
      </v-toolbar-items>
      <v-menu>
        <template v-slot:activator="{ on }">
          <v-toolbar-title v-on="on">
            <v-btn  flat>
              Deploy
              <v-icon dark>arrow_drop_down</v-icon>
            </v-btn>
            <!--<span class="text-uppercase">Deploy</span>-->
            <!--<v-icon dark>arrow_drop_down</v-icon>-->
          </v-toolbar-title>
        </template>

        <v-list>
          <v-list-tile @click="">
            <v-list-tile-title v-text="'Build DAGs'" @click="buildDags"></v-list-tile-title>
          </v-list-tile>
        </v-list>
      </v-menu>
      <v-spacer></v-spacer>
      <v-btn
          flat
          href="https://github.com/biellls/typhoon-orchestrator"
          target="_blank"
      >
        <span class="mr-2">Pre-Alpha</span>
      </v-btn>
    </v-toolbar>

    <v-content>
      <router-view></router-view>
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
  export default {
    name: 'App',
    components: {
    },
    data () {
      return {
        snackbar: false,
        snackbar_message: '',
      }
    },
    methods: {
      buildDags() {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'build-dags', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.snackbar_message = 'Finished building DAGs';
            this.snackbar = true;
          });
      }
    }
  }
</script>
