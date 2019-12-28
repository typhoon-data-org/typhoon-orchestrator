<template>
  <v-navigation-drawer
      v-model="drawer"
      :mini-variant="mini"
      absolute
      dark
      temporary
  >
    <v-list class="pa-1">
      <v-list-tile v-if="mini" @click.stop="mini = !mini">
        <v-list-tile-action>
          <v-icon>chevron_right</v-icon>
        </v-list-tile-action>
      </v-list-tile>

      <v-list-tile avatar tag="div">
        <v-list-tile-avatar>
          <v-icon>code</v-icon>
          <!--<img src="https://randomuser.me/api/portraits/men/85.jpg">-->
        </v-list-tile-avatar>

        <v-list-tile-content>
          <v-list-tile-title>DAGs</v-list-tile-title>
        </v-list-tile-content>

        <v-list-tile-action>
          <v-btn icon @click.stop="mini = !mini">
            <v-icon>chevron_left</v-icon>
          </v-btn>
        </v-list-tile-action>
      </v-list-tile>
    </v-list>

    <v-list class="pt-0" dense>
      <v-divider light></v-divider>

      <v-list-tile
          v-for="filename in dagFiles"
          :key="filename"
          @click="loadDAGFile(filename)"
          :value="filename === activeFilename"
      >
        <v-list-tile-action>
          <v-icon>insert_drive_file</v-icon>
        </v-list-tile-action>

        <v-list-tile-content>
          <v-list-tile-title>{{ filename }}</v-list-tile-title>
        </v-list-tile-content>
      </v-list-tile>
    </v-list>
  </v-navigation-drawer>
</template>

<script>
  export default {
    name: "SidebarDags",
    data () {
      return {
        mini: false,
      }
    },
    computed: {
      dagFiles () {
        return this.$store.state.dagEditor.dagFiles;
      },
      activeFilename () {
        return this.$store.state.dagEditor.currentDAGFilename;
      },
      drawer: {
        get () {
          return this.$store.state.dagEditor.showSidebarDAGs;
        },
        set (show) {
          this.$store.commit('setShowSidebarDAGs', show);
        }
      }
    },
    methods: {
      loadDAGFile: function(filename) {
        this.$store.commit('setCurrentDAGFilename', filename);
        this.$api.getDAGContents({filename: filename})
          .then((result) => {
            this.$emit('dag-file-selected', {
              filename: filename,
              contents: result.data.contents,
            });
            setTimeout(() => {
              this.drawer = false;
            }, 300);
          });
      }
    },
    created: function () {
      if (this.$store.state.dagEditor.savedCode !== '') {
        this.drawer = false;
      }
      // window.addEventListener('keyup', (evt) => {
      //   if (evt.altKey && evt.code === 'Digit1') {
      //     this.drawer = !this.drawer;
      //   }
      //   evt.preventDefault();
      // });
      this.$api.getDagFilenames()
        .then((result) => {
          this.$store.commit('setDagFiles', result.data);
        });
    },
  }
</script>

<style scoped>

</style>