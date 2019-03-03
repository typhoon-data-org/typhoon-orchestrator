import Vue from 'vue'
import './plugins/vuetify'
import App from './App.vue'
import axios from "axios";

Vue.config.productionTip = false;

Vue.prototype.$http = axios;

new Vue({
  render: h => h(App),
}).$mount('#app')
