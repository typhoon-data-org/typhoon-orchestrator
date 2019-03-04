import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  state: {
    typhoonModules: [],
    typhoonFunctions: {},
    userDefinedModules: [],
    userDefinedFunctions: {},
    edges: {
      e1: {
        table_name: {
          apply: true,
          contents: ['$SOURCE']
        },
        query: {
          apply: true,
          contents: [
            'str("SELECT * FROM {{ table_name }} WHERE creation_date=\'{{ date_string }}\'")',
            'typhoon.templates.render(template=$1, table_name=$SOURCE, date_string=$DAG_CONFIG.ds)'
          ]
        },
        batch_size: {
          apply: false,
          contents: 2
        }
      }
    }
  },
  mutations: {
    setTyphoonModules(state, modules) {
      state.typhoonModules = modules;
    },
    setTyphoonFunctions(state, modules) {
      state.typhoonFunctions = modules;
    },
    setUserDefinedModules(state, modules) {
      state.userDefinedModules = modules;
    },
    setUserDefinedFunctions(state, modules) {
      state.userDefinedFunctions = modules;
    },
  },
  actions: {

  }
})
