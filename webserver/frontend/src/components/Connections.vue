<template>
  <!--<v-card>-->
    <!--<v-card-title>-->
      <!--Connections-->
      <!--<v-spacer></v-spacer>-->
      <!--<v-text-field-->
        <!--v-model="search"-->
        <!--append-icon="search"-->
        <!--label="Search"-->
        <!--single-line-->
        <!--hide-details-->
      <!--&gt;</v-text-field>-->
    <!--</v-card-title>-->
    <!--<v-data-table-->
      <!--:headers="headers"-->
      <!--:items="desserts"-->
      <!--:search="search"-->
    <!--&gt;-->
      <!--<template v-slot:items="props">-->
        <!--<td>{{ props.item.name }}</td>-->
        <!--<td class="text-xs-right">{{ props.item.calories }}</td>-->
        <!--<td class="text-xs-right">{{ props.item.fat }}</td>-->
        <!--<td class="text-xs-right">{{ props.item.carbs }}</td>-->
        <!--<td class="text-xs-right">{{ props.item.protein }}</td>-->
        <!--<td class="text-xs-right">{{ props.item.iron }}</td>-->
      <!--</template>-->
      <!--<v-alert v-slot:no-results :value="true" color="error" icon="warning">-->
        <!--Your search for "{{ search }}" found no results.-->
      <!--</v-alert>-->
    <!--</v-data-table>-->
  <!--</v-card>-->
  <h1>Connections</h1>
</template>

<script>
  export default {
    name: "Connections"
  }
</script>

<style scoped>

</style>