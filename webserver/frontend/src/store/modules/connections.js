export default {
  state: {
    items: [],
  },
  mutations: {
    setConnections(state, connections) {
      state.items = connections;
    },
  }
}
