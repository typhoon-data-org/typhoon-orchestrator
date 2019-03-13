import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex);

export default new Vuex.Store({
  state: {
    typhoonFunctionModules: [],
    typhoonFunctions: {},
    userDefinedModules: [],
    userDefinedFunctions: {},
    dag_name: 'example_dag',
    execution_date: new Date().toISOString().substr(0, 10),
    execution_time: '00:00',
    edges: {},
  },
  getters: {
    executionDatetime(state) {
      return state.execution_date + 'T' + state.execution_time;
    }
  },
  mutations: {
    setTyphoonFunctionModules(state, modules) {
      state.typhoonFunctionModules = modules;
    },
    setTyphoonFunctions(state, functions) {
      state.typhoonFunctions = functions;
    },
    setUserDefinedModules(state, modules) {
      state.userDefinedModules = modules;
    },
    setUserDefinedFunctions(state, functions) {
      state.userDefinedFunctions = functions;
    },
    setEdges(state, edges) {
      state.edges = edges;
    },
    setTransformationResult(state, payload) {
      Vue.set(state.edges[payload.edge_name][payload.param_name], 'transformation_result', payload.result);
    },
    setDagName(state, name) {
      state.dag_name = name;
    },
    setExecutionDate(state, date) {
      Vue.set(state, 'execution_date',  date);
    },
    setExecutionTime(state, time) {
      Vue.set(state, 'execution_time',  time);
    },
  },
  actions: {

  }
})
