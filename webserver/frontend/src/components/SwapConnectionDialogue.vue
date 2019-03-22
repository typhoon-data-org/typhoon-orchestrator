<template>
  <v-dialog v-model="dialog" max-width="300px">
    <template v-slot:activator="{ on }">
      <v-icon
          smallction HideAllWindows
          v-on="on"
      >
        swap_horiz
      </v-icon>
    </template>
    <v-card>
      <v-card-title>
        <span class="headline">Swap Connection</span>
      </v-card-title>

      <v-card-text>
        <v-form ref="connection_swap_form">
          <v-container grid-list-md>
            <v-layout wrap>
              <v-flex md12>
                <v-radio-group v-model="radios" :mandatory="true">
                  <v-radio v-for="item in connections" :key="item.conn_env"
                           v-bind:label="item.conn_env + ': ' + item.conn_type"
                           v-bind:value="item.conn_env"
                  >
                  </v-radio>
                </v-radio-group>
              </v-flex>
            </v-layout>
          </v-container>
        </v-form>
      </v-card-text>

      <v-card-actions>
        <v-spacer></v-spacer>
        <v-btn color="blue darken-1" flat @click="close">Cancel</v-btn>
        <v-btn color="blue darken-1" flat @click="save">Save</v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script>
  export default {
    name: "SwitchConnectionDialogue",
    props: {
      dialog: Boolean,
      conn_id: String
    },
    data: () => ({
      radios: null,
      connections: [],
    }),
    watch: {
      dialog: function (val) {
        if (val) {
          this.get_connection_envs(this.conn_id);
        }
      }
    },
    methods: {
      get_connection_envs(conn_id) {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'connection-envs', {
          params: {
            conn_id: conn_id
          }
        })
          .then((result) => {
            this.connections = result.data;
          });
      },
      swapConnection: function (env) {
        const baseURI = 'http://localhost:5000/';
        this.$http.put(baseURI + 'swap-connection', {}, {
          params: {
            env: 'dev',
            conn_id: this.conn_id,
            conn_env: env
          }
        })
          .then((result) => {
            this.$emit('update-connections', '');
          });
      },
      close: function() {
        this.dialog = false;
      },
      save: function () {
        this.swapConnection(this.radios);
        this.dialog = false;
      }
    },
  }
</script>

<style scoped>

</style>