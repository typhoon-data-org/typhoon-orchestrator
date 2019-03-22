import Vue from 'vue'

export default {
  state: {
    typhoonFunctionModules: [],
    typhoonTransformationModules: [],
    typhoonFunctions: {},
    typhoonTransformations: {},
    userDefinedFunctionModules: [],
    userDefinedTransformationModules: [],
    userDefinedFunctions: {},
    userDefinedTransformations: {},
    dag_name: 'example_dag',
    execution_date: new Date().toISOString().substr(0, 10),
    execution_time: '00:00',
    edges: {},
    savedCode: '',
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
    setTyphoonTransformationModules(state, modules) {
      state.typhoonTransformationModules = modules;
    },
    setTyphoonFunctions(state, functions) {
      state.typhoonFunctions = functions;
    },
    setTyphoonTransformations(state, transformations) {
      state.typhoonTransformations = transformations;
    },
    setUserDefinedFunctionModules(state, modules) {
      state.userDefinedFunctionModules = modules;
    },
    setUserDefinedTransformationModules(state, transformations) {
      state.userDefinedTransformationModules = transformations;
    },
    setUserDefinedFunctions(state, functions) {
      state.userDefinedFunctions = functions;
    },
    setUserDefinedTransformations(state, transformations) {
      state.userDefinedTransformations = transformations;
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
    setSavedCode(state, code) {
      state.savedCode = code;
    },
  }
}
