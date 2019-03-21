<template>
  <v-container grid-list-md>
    <v-card>
      <v-card-title>
        <v-icon large left>power</v-icon>
        <h2>Connections</h2>
        <v-dialog v-model="dialog" max-width="500px">
          <template v-slot:activator="{ on }">
            <v-btn dark class="mb-2" v-on="on">
              <v-icon left>add_circle_outline</v-icon>
              New Connection
            </v-btn>
          </template>
          <v-card>
            <v-card-title>
              <span class="headline">{{ formTitle }}</span>
            </v-card-title>

            <v-card-text>
              <v-form ref="connection_form" v-model="valid">
                <v-container grid-list-md>
                  <v-layout wrap>
                    <v-flex md6>
                      <v-text-field
                          v-model="editedItem.conn_id"
                          label="Connection ID"
                          :rules="[rules.required]"
                          :disabled="isNewConnection"
                          :readonly="isNewConnection">
                      </v-text-field>
                    </v-flex>
                    <!--<v-flex xs12 sm6 md4>-->
                      <!--<v-text-field v-model="editedItem.conn_type" label="connection_type"></v-text-field>-->
                    <!--</v-flex>-->
                    <v-flex md6>
                      <v-autocomplete
                          v-model="editedItem.conn_type"
                          label="connection_type"
                          :rules="[rules.required]"
                          :items="connection_types">
                      </v-autocomplete>
                    </v-flex>
                    <v-flex md8>
                      <v-text-field v-model="editedItem.host" label="Host"></v-text-field>
                    </v-flex>
                    <v-flex md4>
                      <v-text-field v-model="editedItem.port" label="Port" :rules="[rules.numeric]"></v-text-field>
                    </v-flex>
                    <v-flex md6>
                      <v-text-field v-model="editedItem.login" label="Login"></v-text-field>
                    </v-flex>
                    <v-flex md6>
                      <v-text-field
                          v-model="editedItem.password"
                          label="Password"
                          type="password"
                          append-icon="visibility_off">
                      </v-text-field>
                    </v-flex>
                    <v-flex md12>
                      <v-textarea v-model="editedItem.extra" label="Extra"></v-textarea>
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
        <v-spacer></v-spacer>
        <v-text-field
            v-model="search"
            append-icon="search"
            label="Search"
            single-line
            hide-details
        ></v-text-field>
      </v-card-title>
      <v-data-table
          :headers="headers"
          :items="connections"
          :search="search"
          :loading="loading"
          item-key="conn_id"
          :rows-per-page-items="[25,50,100,{'text':'$vuetify.dataIterator.rowsPerPageAll','value':-1}]"
      >
        <template v-slot:items="props">
          <td><b>{{ props.item.conn_id }}</b></td>
          <td>{{ props.item.conn_type }}</td>
          <td>{{ props.item.host }}</td>
          <td>{{ props.item.port }}</td>
          <td>{{ props.item.login }}</td>
          <!--<td>{{ props.item.schema }}</td>-->
          <td class="justify-center layout px-0">
            <v-icon
                small
                class="mr-2"
                @click="editItem(props.item)"
            >
              edit
            </v-icon>
            <v-icon
                small
                @click="deleteItem(props.item)"
            >
              delete
            </v-icon>
          </td>
        </template>
        <v-alert v-slot:no-results :value="true" color="error" icon="warning">
          Your search for "{{ search }}" found no results.
        </v-alert>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  export default {
    name: "Connections",
    data: () => ({
      valid: false,
      search: '',
      headers: [
        { text: 'Connection ID', align: 'left', value: 'conn_id' },
        { text: 'Type', value: 'conn_type' },
        { text: 'Host', value: 'host' },
        { text: 'Port', value: 'port' },
        { text: 'Login', value: 'login' },
        // { text: 'Schema', value: 'schema' },
        { text: 'Actions', value: 'conn_id', sortable: false },
      ],
      connection_types: [],
      dialog: false,
      loading: false,
      editedIndex: -1,
      editedItem: {
        conn_id: null,
        conn_type: null,
        schema: null,
        host: null,
        port: null,
        login: null,
        password: null,
        extra: '',
      },
      defaultItem: {
        conn_id: null,
        conn_type: null,
        schema: null,
        host: null,
        port: null,
        login: null,
        password: null,
        extra: '',
      },
      rules: {
        required: value => !!value || 'Required.',
        numeric: value => (!value || /^\d*$/.test(value)) || 'Invalid port number',
      }
    }),
    computed: {
      connections () {
        return this.$store.state.connections.items;
      },
      formTitle () {
        return this.editedIndex === -1 ? 'New Connection' : 'Edit Connection';
      },
      isNewConnection () {
        return this.editedIndex !== -1;
      },
      connection_ids () {
        return this.$store.state.connections.items.map(x => x.conn_id);
      }
    },
    methods: {
      getConnections: function () {
        this.loading = true;
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'connections', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.$store.commit('setConnections', result.data);
            this.loading = false;
          });
      },

      getConnectionTypes: function () {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'connection-types')
          .then((result) => {
            this.connection_types = result.data;
          });
      },

      setConnection: function (conn) {
        const baseURI = 'http://localhost:5000/';
        this.$http.put(baseURI + 'connection', conn, {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.getConnections();
          });
      },


      deleteConnection: function (conn) {
        const baseURI = 'http://localhost:5000/';
        this.$http.delete(baseURI + 'connection', {
          params: {
            env: 'dev',
            conn_id: conn.conn_id
          }
        })
          .then((result) => {
            this.getConnections();
          });
      },

      editItem: function (item) {
        this.editedIndex = this.connections.indexOf(item);
        this.editedItem = Object.assign({}, item);
        this.editedItem.extra = (this.editedItem.extra && JSON.stringify(this.editedItem.extra, null, 2)) || '';
        this.dialog = true;
      },

      deleteItem: function (item) {
        const index = this.connections.indexOf(item);
        let confirmed = confirm('Are you sure you want to delete this connection?')
        if (confirmed) {
          this.deleteConnection(item)
        }
      },

      close: function () {
        this.dialog = false;
        setTimeout(() => {
          this.editedItem = Object.assign({}, this.defaultItem);
          this.editedIndex = -1
        }, 300)
      },

      save: function () {
        let conn = Object.assign({}, this.editedItem);
        if (this.$refs.connection_form.validate()) {
          conn.extra = (conn.extra && JSON.parse(conn.extra)) || null;
          // if (this.editedIndex > -1) {  // Editing existing object
          this.setConnection(conn);
          this.close()
        }
      }
    },
    created: function () {
      this.getConnections();
      this.getConnectionTypes();
    },
  }
</script>

<style scoped>

</style>