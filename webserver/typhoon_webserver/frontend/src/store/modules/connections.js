import Vue from 'vue'

export default {
  state: {
    items: [],
  },
  mutations: {
    setConnections(state, connections) {
      Vue.set(state, 'items', connections);
      // state.items = connections;
    },
  }
}
