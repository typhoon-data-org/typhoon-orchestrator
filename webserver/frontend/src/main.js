import Vue from 'vue'
import './plugins/vuetify'
import App from './App.vue'
import axios from "axios";
import store from './store/store'
import router from './router'
import api from './scripts/api'

Vue.config.productionTip = false;

Vue.prototype.$http = axios;
Vue.prototype.$api = api;

new Vue({
  store,
  router,
  render: h => h(App)
}).$mount('#app')
