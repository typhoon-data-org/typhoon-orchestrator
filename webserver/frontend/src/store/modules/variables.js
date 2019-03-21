import Vue from 'vue'

export default {
  state: {
    items: [],
  },
  mutations: {
    setVariables(state, variables) {
      Vue.set(state, 'items', variables);
    },
  }
}
