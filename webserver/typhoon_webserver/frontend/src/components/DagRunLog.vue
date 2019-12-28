<template>
  <v-layout justify-center>
    <v-dialog v-model="dialog" fullscreen hide-overlay transition="dialog-bottom-transition">
      <v-card>
        <v-toolbar>
          <v-btn icon @click="$emit('closed')">
            <v-icon>close</v-icon>
          </v-btn>
          <v-toolbar-title>{{title}}</v-toolbar-title>
          <v-spacer></v-spacer>
        </v-toolbar>
        <v-card-text><pre>{{logText}}</pre></v-card-text>
      </v-card>
    </v-dialog>
  </v-layout>
</template>

<script>
  export default {
    name: "DagRunLog",
    props: {
      dialog: Boolean,
      logText: String,
      title: String,
    },
    data: () => ({
    }),
  }
</script>

<style scoped>

</style>