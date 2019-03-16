<template>
  <v-container grid-list-md>
    <v-card>
      <v-card-title>
        <v-icon large left>power</v-icon>
        <h2>Connections</h2>
        <v-spacer></v-spacer>
        <v-text-field
            v-model="search"
            append-icon="search"
            label="Search"
            single-line
            hide-details
        ></v-text-field>
      </v-card-title>
      <v-data-table
          :headers="headers"
          :items="connections"
          :search="search"
      >
        <template v-slot:items="props">
          <td>{{ props.item.conn_id }}</td>
          <td>{{ props.item.conn_type }}</td>
          <td>{{ props.item.host }}</td>
          <td>{{ props.item.port }}</td>
          <td>{{ props.item.login }}</td>
          <td>{{ props.item.schema }}</td>
        </template>
        <v-alert v-slot:no-results :value="true" color="error" icon="warning">
          Your search for "{{ search }}" found no results.
        </v-alert>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  export default {
    name: "Connections",
    data: () => ({
      search: '',
      headers: [
        { text: 'Connection ID', align: 'left', value: 'conn_id' },
        { text: 'Type', value: 'conn_type' },
        { text: 'Host', value: 'host' },
        { text: 'Port', value: 'port' },
        { text: 'Login', value: 'login' },
        { text: 'Schema', value: 'schema' },
      ],
      connections: [
        {
          conn_id: 'reminder_bot',
          conn_type: 'telegram',
          host: 'telegram.com',
          port: '1234',
          login: 'jkdfnkjabs36814qwhasndjn',
          schema: 'None'
        }
      ],
    }),
  }
</script>

<style scoped>

</style>