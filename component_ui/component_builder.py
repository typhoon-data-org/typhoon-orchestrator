import streamlit as st
import json
import pydantic_component_mock as mock_comps
import requests
import yaml

api_url = "http://localhost:8000/api"
api_version = "v1"
api_str = "/".join([api_url, api_version])

l = mock_comps.Component.schema()['properties']['component_name']['anyOf']
list_c = [x['$ref'].split('/')[2] for x in l]


def make_request(url, method, query_params=None, body=None):
    response = requests.request(method, url, params=query_params, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return "Error: " + str(e)
    return response.json()

@st.cache
def get_components():
    endpoint = "/".join([api_str, 'components'])
    components = make_request(endpoint, 'get')
    return components
@st.cache
def get_component_definition(component_name):
    endpoint = "/".join([api_str, 'component'])
    component_def = make_request(endpoint + '/' + component_name, 'get')
    print(component_def)
    return component_def

@st.cache
def get_connections():
    endpoint = "/".join([api_str, 'connections'])
    conns_list = make_request(endpoint, 'get')
    return [ c['conn_id'] for c in conns_list['connections'] ]


def post_create_dag_from_component(dag__from_component_json):
    endpoint = "/".join([api_str, 'dag-from-component'])
    response = make_request(endpoint, 'post', body=dag__from_component_json)
    return response

def put_create_dag(dag_json):
    endpoint = "/".join([api_str, 'dag'])
    response = make_request(endpoint, 'put', body=dag_json)
    return response


st.sidebar.header('DAG configuration')

component_lst = get_components()

component = st.sidebar.selectbox(
    label='Select Component',
    options=list(component_lst['components'].keys()),
    help='You can follow docs here to create a component and use this UI to build a DAG from it.'
)


component_definition = get_component_definition(component)


st.header('Component config:  ' + component_definition['name'])

c1, c2, c3 = st.beta_columns((2, 1, 1))
with c1:
    dag_name = st.text_input(
        label='DAG Name',
        value='your_dag_name_here',
        key='input_dag_name',
        help='Any lowercase name for your Typhoon DAG. Try to avoid clashes. Must be without spaces or special characters'
    )
with c2:
    schedule_granularity = st.selectbox(
        label='Schedule type',
        options=["year", "month", "day", "hour",  "minute",  "second"],
        index=3,
        key='input_schedule_type',
        help='Rate interval type'
    )
with c3:
    schedule_value = st.number_input(
        label='Schedule interval',
        value=1,
        key='schedule_value',
        help='Rate interval'
    )

return_vals = {}
return_vals['base_component'] = component_lst['components'][component] + '.' + component
return_vals['name'] = dag_name
return_vals['schedule_interval'] = schedule_value
return_vals['granularity'] = schedule_granularity
return_vals['component_arguments'] = {}

st.subheader('Args:')

hook_list = {}

for arg in component_definition['args']:
    if component_definition['args'][arg] == 'str':
        return_vals['component_arguments'][arg] = st.text_input(
            label=arg,
            value='<my_value>',
            key=arg,
            help='Your environment name - underscore_lowercase'
        )
    elif 'Hook' in component_definition['args'][arg]:
        c1_h, c2_h, = st.beta_columns((1, 1))
        with c1_h:
            if get_connections() == []:
                st.error("Please add hooks using typhoon cli - typhoon connections add.")
                hook_list[arg] = ' ... '
            else:
                hook_list[arg] = st.selectbox(
                    label=arg,
                    options=get_connections(),
                    help='Choose from available connection of Type ' + (component_definition['args'][arg])
                )

        with c2_h:
            return_vals['component_arguments'][arg] = st.text_input(
                label=arg,
                value='!Hook ' + str(hook_list[arg] or ''),
                key=arg,
                help='''Example:
                          !Hook my_env_db_hook'
                '''
            )
    elif component_definition['args'][arg] == 'List[str]':
         list_input = st.text_area(
            label=arg,
            value='- table_1 \n- table_2 \n- table_3',
            key=arg,
            help='Type ' + (component_definition['args'][arg]) + ' \n Example \n- <my_value_1> \n- <my_value_2> \n- <my_value_3>'
         )
         try:
             parsed_list = yaml.safe_load(list_input)
         except:
             st.error("Please add hooks using typhoon cli - typhoon connections add.")
             parsed_list = None
         return_vals['component_arguments'][arg] = parsed_list
    elif type(component_definition['args'][arg]) == dict:
        return_vals['component_arguments'][arg] = st.text_input(
            label=arg,
            value='<my_value> - (hint: ' + str((component_definition['args'][arg])) +')',
            key=arg,
            help='Type ' + str((component_definition['args'][arg]))
        )
    elif component_definition['args'][arg] == 'dict':
        return_vals['component_arguments'][arg] = st.text_input(
            label=arg,
            value='<my_value> - (hint: ' + str((component_definition['args'][arg])) +')',
            key=arg,
            help='Type ' + str((component_definition['args'][arg]))
        )
    else:
        return_vals['component_arguments'][arg] = st.text_input(
            label=arg,
            value='<my_value>',
            key=str(arg),
            help='Your environment name - underscore_lowercase'
        )

dag_from_component = json.loads("{}")
dag_success = json.loads("{}")

if st.button('Create DAG'):
    # First create dag from component
    dag_from_component = post_create_dag_from_component(return_vals)
    # st.write(dag_from_component)
    # Create the Dag
    dag_success = put_create_dag(dag_from_component)
    st.write(dag_success)

#
# expander = st.beta_expander("Component Raw Definition")
# expander.write(component_definition)

# return_val_expander = st.beta_expander("DAG return Values")
# return_val_expander.json(json.dumps(return_vals))
#
# expander = st.beta_expander("Resulting DAG from API")
# expander.write(dag_from_component)
#
# expander = st.beta_expander("Result of DAG Creation")
# expander.write(dag_success)
#



