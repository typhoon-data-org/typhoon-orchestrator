<template>
  <v-card flat>
    <v-card-title>
      <v-icon
        large
        left
      >
        description
      </v-icon>
      <span v-if="output_type" class="title">
        {{ title }} -> {{ output_type }}
      </span>
      <span v-else class="title">
        {{ title }}
      </span>
    </v-card-title>
    <v-card-text>
      <h3>Parameters:</h3>
      <v-data-table
          :items="input_parameters"
          class="elevation-8 mb-4"
          hide-actions
          hide-headers
      >
        <template v-slot:items="props">
          <td>{{ props.item.name }}</td>
          <td class="text-xs-right">{{ props.item.type }}</td>
        </template>
      </v-data-table>
      <h3>Description:</h3>
      {{ description }}
    </v-card-text>
  </v-card>
</template>
<script>
  export default {
    name: 'DocsView',
    data: () => ({
      title: 'to_csv',
      description: 'Converts a DataFrame parameter to a csv buffer',
      input_parameters: [
        {'name': 'aa', 'type': 'int'},
        {'name': 'b', 'type': 'str'},
        {'name': 'data', 'type': 'DataFrame'},
      ],
      output_type: 'BytesIO',
    }),
  }
</script>
