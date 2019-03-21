import Vue from 'vue'
import Vuex from 'vuex'
import dagEditor from "./modules/dagEditor";
import connections from "./modules/connections";
import variables from "./modules/variables";

Vue.use(Vuex);

export default new Vuex.Store({
  modules: {
    dagEditor,
    connections,
    variables
  },
  state: {
  },
  getters: {
  },
  mutations: {
  },
  actions: {

  }
})
