<template>
  <v-layout row justify-center>
    <v-dialog v-model="dialog" persistent max-width="600px">
      <template v-slot:activator="{ on }">
        <v-btn color="brown" v-on="on">
          <v-icon left>extension</v-icon>
          DAG config
        </v-btn>
      </template>
      <v-card>
        <v-card-title>
          <span class="headline">DAG Configuration</span>
        </v-card-title>
        <v-card-text>

          <v-layout row wrap>
            <v-flex md4>
              <v-menu
                  ref="menu"
                  v-model="menu"
                  :nudge-right="40"
                  lazy
                  transition="scale-transition"
                  offset-y
                  full-width
                  min-width="290px"
              >
                <template v-slot:activator="{ on }">
                  <v-text-field
                      v-model="execution_date"
                      label="Pick execution date"
                      prepend-icon="event"
                      readonly
                      v-on="on"
                  ></v-text-field>
                </template>
                <v-date-picker v-model="execution_date" no-title scrollable>
                </v-date-picker>
              </v-menu>
            </v-flex>
            <v-flex md4>
              <v-menu
                  ref="menu"
                  v-model="menu2"
                  :close-on-content-click="false"
                  :nudge-right="40"
                  :return-value.sync="execution_time"
                  lazy
                  transition="scale-transition"
                  offset-y
                  full-width
                  max-width="290px"
                  min-width="290px"
              >
                <template v-slot:activator="{ on }">
                  <v-text-field
                      v-model="execution_time"
                      label="Picker in menu"
                      prepend-icon="access_time"
                      readonly
                      v-on="on"
                  ></v-text-field>
                </template>
                <v-time-picker
                    v-if="menu2"
                    v-model="execution_time"
                    full-width
                    @click:minute="$refs.menu.save(execution_time)"
                ></v-time-picker>
              </v-menu>
            </v-flex>
            <v-spacer></v-spacer>
          </v-layout>
          <v-container grid-list-md>
            <v-layout wrap>
              <v-flex xs12 sm6 md4>
                <v-text-field v-model="execution_datetime" label="Execution Date" readonly deactivated></v-text-field>
              </v-flex>
              <v-flex xs12 sm6 md4>
                <v-text-field
                    label="ds"
                    hint="Calculated from execution date"
                    v-model="execution_date"
                    readonly deactivated>
                </v-text-field>
              </v-flex>
              <v-flex xs12 sm6 md4>
                <v-text-field
                    label="ds_nodash"
                    hint="Calculated from execution date"
                    v-model="ds_nodash"
                    required
                    readonly
                    deactivated
                ></v-text-field>
              </v-flex>
              <v-flex xs12>
                <v-text-field label="DAG name" v-model="dag_name" required></v-text-field>
              </v-flex>
            </v-layout>
          </v-container>
        </v-card-text>
        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn flat @click="dialog = false">Close</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-layout>
</template>

<script>
  export default {
    name: "DagConfigDialog",
    data: () => ({
      dialog: false,
      // execution_date: new Date().toISOString().substr(0, 10),
      // execution_time: "00:00",
      menu: false,
      menu2: false,
    }),
    computed: {
      execution_date: {
        get: function () {
          return this.$store.state.dagEditor.execution_date;
        },
        set: function (value) {
          this.$store.commit('setExecutionDate', value);
        }
      },
      execution_time: {
        get: function () {
          return this.$store.state.dagEditor.execution_time;
        },
        set: function (value) {
          this.$store.commit('setExecutionTime', value);
        }
      },
      execution_datetime() {
        return this.$store.getters.executionDatetime;
      },
      ds_nodash() {
        return this.execution_date.replace('-', '').replace('-', '');
      },
      dag_name: {
        get: function () {
          return this.$store.state.dagEditor.dag_name;
        },
        set: function (dag_name) {
          this.$store.commit('setDagName', dag_name);
        }
      }
    },
  }
</script>

<style scoped>

</style>