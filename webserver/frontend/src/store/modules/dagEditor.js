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
    currentDocObject: null,
    dagFiles: [],
    currentDAGFilename: null,
    showSidebarDAGs: true,
    showDAGConfigDialog: false,
  },
  getters: {
    executionDatetime(state) {
      return state.execution_date + 'T' + state.execution_time;
    },
    typhoonFunctionNames(state) {
      let result = {};
      Object.keys(state.typhoonFunctions).forEach(k => {
        if (!result.hasOwnProperty(k)) {
          result[k] = [];
        }
        Object.keys(state.typhoonFunctions[k]).forEach(k2 => {
          result[k].push(state.typhoonFunctions[k][k2].name);
        });
      });
      return result;
    },
    typhoonTransformationNames(state) {
      let result = {};
      Object.keys(state.typhoonTransformations).forEach(k => {
        if (!result.hasOwnProperty(k)) {
          result[k] = [];
        }
        Object.keys(state.typhoonTransformations[k]).forEach(k2 => {
          result[k].push(state.typhoonTransformations[k][k2].name);
        });
      });
      return result;
    },
    userDefinedFunctionNames(state) {
      let result = {};
      Object.keys(state.userDefinedFunctions).forEach(k => {
        if (!result.hasOwnProperty(k)) {
          result[k] = [];
        }
        Object.keys(state.userDefinedFunctions[k]).forEach(k2 => {
          result[k].push(state.userDefinedFunctions[k][k2].name);
        });
      });
      return result;
    },
    userDefinedTransformationNames(state) {
      let result = {};
      Object.keys(state.userDefinedTransformations).forEach(k => {
        if (!result.hasOwnProperty(k)) {
          result[k] = [];
        }
        Object.keys(state.userDefinedTransformations[k]).forEach(k2 => {
          result[k].push(state.userDefinedTransformations[k][k2].name);
        });
      });
      return result;
    },
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
      let result;
      if (typeof payload.result === 'object' && '__error__' in payload.result) {
        result = payload.result;
      } else {
        result = JSON.stringify(payload.result)
      }
      Vue.set(state.edges[payload.edge_name][payload.param_name], 'transformation_result', result);
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
    setCurrentDocObject(state, docObj) {
      state.currentDocObject = docObj;
    },
    setDagFiles(state, dagFiles) {
      state.dagFiles = dagFiles;
    },
    setCurrentDAGFilename(state, filename) {
      state.currentDAGFilename = filename;
    },
    setShowSidebarDAGs(state, show) {
      state.showSidebarDAGs = show;
    },
    setShowDAGConfigDialog(state, show) {
      state.showDAGConfigDialog = show;
    },
    toggleDAGConfigDialog(state) {
      state.showDAGConfigDialog = !this.setShowDAGConfigDialog;
    },
  }
}
