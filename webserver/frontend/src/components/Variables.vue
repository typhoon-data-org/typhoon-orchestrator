<template>
  <v-container grid-list-md>
    <v-card>
      <v-card-title>
        <v-icon large left>assignment_ind</v-icon>
        <h2>Variables</h2>
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
          :items="variables"
          :search="search"
          item-key="id"
          :rows-per-page-items="[25,50,100,{'text':'$vuetify.dataIterator.rowsPerPageAll','value':-1}]"
      >
        <template v-slot:items="props">
          <td><b>{{ props.item.id }}</b></td>
          <td>{{ props.item.type }}</td>
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
    name: "Variables",
    data: () => ({
      valid: false,
      search: '',
      headers: [
        { text: 'Variable ID', align: 'left', value: 'id' },
        { text: 'Type', value: 'type' },
        { text: 'Actions', value: 'id', sortable: false },
      ],
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
    }),
    computed: {
      variables() {
        return this.$store.state.variables.items;
      },
    },
    methods: {
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
      setVariable: function (variable) {
        const baseURI = 'http://localhost:5000/';
        this.$http.put(baseURI + 'variable', variable, {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.getConnections();
          });
      },
      editItem: function (item) {
        this.editedIndex = this.variables.indexOf(item);
        this.editedItem = Object.assign({}, item);
        // this.dialog = true;
      },
      deleteItem: function (item) {
        const index = this.variables.indexOf(item);
        let confirmed = confirm('Are you sure you want to delete this variable?')
        if (confirmed) {
          // this.deleteVariable(item)
        }
      },
    },
    created: function () {
      // this.setVariable({'id': 'num_b', 'type': 'number', 'contents': '3'});
      this.getVariables();
    },
  }
</script>

<style scoped>

</style>