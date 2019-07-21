<template>
  <v-card v-if="hasDocObj" flat>
    <v-card-title>
      <v-icon
        large
        left
      >
        description
      </v-icon>
      <span v-if="return_type" class="title">
        {{ title }} -> {{ return_type }}
      </span>
      <span v-else class="title">
        {{ title }}
      </span>
    </v-card-title>
    <v-card-text>
      <h3>Arguments:</h3>
      <v-data-table
          :items="func_args"
          class="elevation-8 mb-4 ma-2"
          hide-actions
          hide-headers
      >
        <template v-slot:items="props">
          <td>{{ props.item.name }}</td>
          <td class="text-xs-right">{{ props.item.type }}</td>
        </template>
      </v-data-table>
      <div v-if="description">
        <h3>Description:</h3>
        <v-textarea v-model="description" readonly disabled></v-textarea>
      </div>
    </v-card-text>
  </v-card>
  <v-card v-else flat>
    <v-card-text>
      <h1 class="text-md-center">No Docs</h1>
    </v-card-text>
  </v-card>
</template>
<script>
  export default {
    name: 'DocsView',
    data: () => ({
    }),
    computed: {
      hasDocObj() {
        return this.$store.state.dagEditor.currentDocObject !== null;
      },
      title() {
        let docObj = this.$store.state.dagEditor.currentDocObject;
        if (docObj) {
          return docObj.name;
        } else {
          return null;
        }
      },
      description() {
        let docObj = this.$store.state.dagEditor.currentDocObject;
        if (docObj.type === 'user_function') {
          return this.$store.state.dagEditor.userDefinedFunctions[docObj.module][docObj.name].docstring;
        } else if (docObj.type === 'user_transformation') {
          return this.$store.state.dagEditor.userDefinedTransformations[docObj.module][docObj.name].docstring;
        } else if (docObj.type === 'typhoon_function') {
          return this.$store.state.dagEditor.typhoonFunctions[docObj.module][docObj.name].docstring;
        } else if (docObj.type === 'typhoon_transformation') {
          return this.$store.state.dagEditor.typhoonTransformations[docObj.module][docObj.name].docstring;
        }
        return null
      },
      return_type() {
        let docObj = this.$store.state.dagEditor.currentDocObject;
        if (docObj.type === 'user_function') {
          return this.$store.state.dagEditor.userDefinedFunctions[docObj.module][docObj.name].return_type;
        } else if (docObj.type === 'user_transformation') {
          return this.$store.state.dagEditor.userDefinedTransformations[docObj.module][docObj.name].return_type;
        } else if (docObj.type === 'typhoon_function') {
          return this.$store.state.dagEditor.typhoonFunctions[docObj.module][docObj.name].return_type;
        } else if (docObj.type === 'typhoon_transformation') {
          return this.$store.state.dagEditor.typhoonTransformations[docObj.module][docObj.name].return_type;
        }
        return null;
      },
      func_args() {
        let docObj = this.$store.state.dagEditor.currentDocObject;
        let args = [];
        if (docObj.type === 'user_function') {
          args = this.$store.state.dagEditor.userDefinedFunctions[docObj.module][docObj.name].args;
        } else if (docObj.type === 'user_transformation') {
          args = this.$store.state.dagEditor.userDefinedTransformations[docObj.module][docObj.name].args;
        } else if (docObj.type === 'typhoon_function') {
          args = this.$store.state.dagEditor.typhoonFunctions[docObj.module][docObj.name].args;
        } else if (docObj.type === 'typhoon_transformation') {
          args = this.$store.state.dagEditor.typhoonTransformations[docObj.module][docObj.name].args;
        }
        return args || [];
      }
    }
  }
</script>
