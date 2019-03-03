import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

export default new Vuex.Store({
  state: {
    typhoonModules: [],
    typhoonFunctions: {},
    userDefinedModules: [],
    userDefinedFunctions: {},
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
