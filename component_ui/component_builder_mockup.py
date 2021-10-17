import streamlit as st
from unittest.mock import Mock
import json
import urllib
import pydantic_component_mock as mock_comps

api_url = "http://localhost:8080/api"
api_version = "v1"
api_str = "/".join([api_url, api_version])

l = mock_comps.Component.schema()['properties']['component_name']['anyOf']
list_c = [x['$ref'].split('/')[2] for x in l]



def mock_get_components(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://localhost:8080/api/v1/components':
        return MockResponse({"components": list_c, "errors": None}, 200)
    else:
        return MockResponse(None, 404)

    return MockResponse(None, 404)


def mock_get_component_definition(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if 'http://localhost:8080/api/v1/component/' in args[0]:
        requested_component = args[0].rsplit('/', 1)[-1]
        if requested_component == 'Singer_Demultiplexer_Model':
            cd = mock_comps.Singer_Demultiplexer_Model.schema()
            return MockResponse({"definition": cd, "errors": None}, 200)
        elif requested_component == 'CSV_To_Snowflake_Model':
            cd = mock_comps.CSV_To_Snowflake_Model.schema()
            return MockResponse({"definition": cd, "errors": None}, 200)
        elif requested_component == 'Glob_Compress_Model':
            cd = mock_comps.Glob_Compress_Model.schema()
            return MockResponse({"definition": cd, "errors": None}, 200)
        else:
            return MockResponse({"definition": None, "errors": 'No component found'}, 200)
    else:
        return MockResponse(None, 404)


def mock_get_connections(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    connections = []
    connections.append({'name': 'transaction_sys', 'class': 'LocalStorageHook'})
    connections.append({'name': 'pricing_prod', 'class': 'LocalStorageHook'})
    connections.append({'name': 'data_lake', 'class': 'S3Hook'})
    connections.append({'name': 'external_client_share_2133', 'class': 'S3Hook'})
    connections.append({'name': 'snowflake', 'class': 'Snowflake'})
    connections.append({'name': 'snowflake_legacy_co', 'class': 'Snowflake'})
    o = urllib.parse.urlparse(args[0])
    if o.path == '/api/v1/connections':
        if o.query == '':
            return MockResponse({"connections": connections, "errors": None}, 200)
        else:
            hook_list_params = o.query.split('&')
            hook_list = [x.split('=')[1] for x in hook_list_params]
            connection_filtered = list(filter(lambda x: x['class'] in hook_list, connections))
            return MockResponse({"connections": connection_filtered, "errors": None}, 200)
    else:
        return MockResponse(None, 404)

return_vals = {}


st.sidebar.header('DAG configuration')
component_lst = mock_get_components("/".join([api_str,'components'])).json()
component = st.sidebar.selectbox(
    label='Select Component',
    options=component_lst['components'],
    help='You can follow docs here to create a component and use this UI to build a DAG from it.'
)
dag_name = st.sidebar.text_input(
    label='DAG Name',
    value='your_dag_name_here',
    key='input_dag_name',
    help='Any lowercase name for your Typhoon DAG. Try to avoid clashes. Must be without spaces or special characters'
)
schedule_type = st.sidebar.radio(
    label='Schedule type',
    options=['Rate', 'Cron'],
    index=0,
    key='input_schedule_type',
    help='Typhoon dags can accept Cron or a Rate.'
)
schedule_value = st.sidebar.text_input(
    label=schedule_type,
    value='1 hour' if schedule_type == 'Rate' else '0 0 * ? * *',
    key='schedule_value',
    help='Any Rate or Cron.'
)

return_vals['base_component'] = component
return_vals['dag_name'] = dag_name
return_vals['schedule_type'] = schedule_type
return_vals['schedule_value'] = schedule_value
return_vals['component_arguments'] = {}

st.header('Component config')
component_definition = mock_get_component_definition("/".join([api_str,'component',component])).json()
st.subheader('Args:')
props = component_definition['definition']['properties']
for arg in props.keys():
    if '$ref' in props[arg].keys():
        ref_path = props[arg]['$ref'].split('/')

        # Do params here for now hard code - use URLlib parser - not manual
        param = component_definition['definition'][ref_path[1]][ref_path[2]]
        hook_list_raw = param['properties']['hook_class']['anyOf']

        hook_list = [x['const'] for x in hook_list_raw]
        uri = "/".join([api_str, 'connections'])
        if len(hook_list) >0:
            uri = uri + '?' + urllib.parse.urlencode({'type': hook_list}, True)
        connections_list = mock_get_connections(uri).json()
        connection_keys = [d['name'] for d in connections_list['connections'] if 'name' in d]

        return_vals['component_arguments'][arg] = st.selectbox(
            label=arg,
            options=connection_keys,
            help='help here (docstring hint?)'
        )
    else:
        if 'type' in props[arg].keys():
            if props[arg]['type'] == 'string':
                return_vals['component_arguments'][arg] = st.text_input(
                    label=arg,
                    value='example here',
                    key=arg,
                    help='help here (docstring hint?)'
                )
            elif props[arg]['type'] == 'array':
                return_vals['component_arguments'][arg] = st.radio(
                    label=arg,
                    options=props[arg]['default'],
                    index=0,
                    key=arg,
                    help='help here (docstring hint?)'
                )
        else :
            st.write('Type unspecified (Use string as default).')


if st.button('Create DAG'):
    # send the request to create a DaG
    st.write('Success!')


expander = st.beta_expander("Component Raw Definition")
expander.write(component_definition)

return_val_expander = st.beta_expander("DAG return Values")
return_val_expander.write(return_vals)


