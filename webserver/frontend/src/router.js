import Vue from 'vue'
import Router from 'vue-router'
import Home from './views/Home.vue'
import Connections from "./components/Connections";
import DagEditor from "./components/DagEditor";

Vue.use(Router)

export default new Router({
  mode: 'history',
  base: process.env.BASE_URL,
  routes: [
    {
      path: '/',
      name: 'home',
      component: Home
    },
    {
      path: '/connections',
      name: 'connections',
      component: Connections
    },
    {
      path: '/dag-editor',
      name: 'DAG editor',
      component: DagEditor
    },
    {
      path: '/about',
      name: 'about',
      // route level code-splitting
      // this generates a separate chunk (about.[hash].js) for this route
      // which is lazy-loaded when the route is visited.
      component: () => import(/* webpackChunkName: "about" */ './views/About.vue')
    }
  ]
})
