import Vue from 'vue'
import Vuex from 'vuex'
import dagEditor from "./modules/dagEditor";
import connections from "./modules/connections";

Vue.use(Vuex);

export default new Vuex.Store({
  modules: {
    dagEditor,
    connections
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
