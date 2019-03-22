<template>
  <v-container grid-list-md>
    <v-card>
      <v-card-title>
        <v-icon large left>assignment_ind</v-icon>
        <h2>Variables</h2>
        <v-dialog v-model="dialog" max-width="500px">
          <template v-slot:activator="{ on }">
            <v-btn dark class="mb-2" v-on="on">
              <v-icon left>add_circle_outline</v-icon>
              New Variable
            </v-btn>
          </template>
          <v-card>
            <v-card-title>
              <span class="headline">{{ formTitle }}</span>
            </v-card-title>

            <v-card-text>
              <v-form ref="variable_form" v-model="valid">
                <v-container grid-list-md>
                  <v-layout wrap>
                    <v-flex md6>
                      <v-text-field
                          v-model="editedItem.id"
                          label="Variable ID"
                          :rules="[rules.required]"
                          :disabled="isNewVariable"
                          :readonly="isNewVariable">
                      </v-text-field>
                    </v-flex>
                    <v-flex md6>
                      <v-autocomplete
                          v-model="editedItem.type"
                          label="variable_type"
                          :rules="[rules.required]"
                          :items="variable_types">
                      </v-autocomplete>
                    </v-flex>
                    <v-flex md12>
                      <v-textarea v-model="editedItem.contents" label="Contents" :rules="[rules.required]">
                      </v-textarea>
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
          :items="variables"
          :search="search"
          :loading="loading"
          item-key="id"
          :rows-per-page-items="[25,50,100,{'text':'$vuetify.dataIterator.rowsPerPageAll','value':-1}]"
      >
        <template v-slot:items="props">
          <td><b>{{ props.item.id }}</b></td>
          <td>{{ props.item.type }}</td>
          <td>
            <v-icon
                small
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
      dialog: false,
      loading: false,
      variable_types: [],
      editedIndex: -1,
      editedItem: {
        id: null,
        type: null,
        contents: null,
      },
      defaultItem: {
        id: null,
        type: null,
        contents: null,
      },
      rules: {
        required: value => !!value || 'Required.',
      }
    }),
    computed: {
      variables() {
        return this.$store.state.variables.items;
      },
      isNewVariable () {
        return this.editedIndex !== -1;
      },
      formTitle () {
        return this.editedIndex === -1 ? 'New Variable' : 'Edit Variable';
      },
    },
    methods: {
      getVariables: function () {
        this.loading = true;
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'variables', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.$store.commit('setVariables', result.data);
            this.loading = false;
          });
      },
      getVariableTypes: function () {
        const baseURI = 'http://localhost:5000/';
        this.$http.get(baseURI + 'variable-types', {
          params: {
            env: 'dev'
          }
        })
          .then((result) => {
            this.variable_types = result.data;
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
            this.getVariables();
          });
      },
      deleteVariable: function (variable) {
        const baseURI = 'http://localhost:5000/';
        this.$http.delete(baseURI + 'variable', {
          params: {
            env: 'dev',
            id: variable.id
          }
        })
          .then((result) => {
            this.getVariables();
          });
      },
      editItem: function (item) {
        this.editedIndex = this.variables.indexOf(item);
        this.editedItem = Object.assign({}, item);
        this.dialog = true;
      },
      deleteItem: function (item) {
        const index = this.variables.indexOf(item);
        let confirmed = confirm('Are you sure you want to delete this variable?')
        if (confirmed) {
          this.deleteVariable(item)
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
        let variable = Object.assign({}, this.editedItem);
        if (this.$refs.variable_form.validate()) {
          this.setVariable(variable);
          this.close()
        }
      }
    },
    created: function () {
      this.getVariableTypes();
      this.getVariables();
    },
  }
</script>

<style scoped>

</style>