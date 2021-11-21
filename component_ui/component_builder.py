import json
import re
from time import sleep

import graphviz
import requests
import streamlit as st
import yaml

api_url = "http://localhost:8000/api"
api_version = "v1"
api_str = "/".join([api_url, api_version])

if 'initialised' not in st.session_state:
    with st.spinner('Waiting for API...'):
        retry_seconds = 1
        healthy = False
        while not healthy:
            try:
                requests.get(f'{api_str}/health-check').raise_for_status()
                healthy = True
            except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError):
                print(f'Failed health check, retry in {retry_seconds} seconds')
                sleep(retry_seconds)


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
components = {}
print(component_lst)
for component in component_lst['components']:
    c_definition = get_component_definition(component)
    show = True
    for key in c_definition:
        if 'COMPONENT_INPUT' in str(c_definition[key]):
            show = False
            break
    if show:
        components[component] = c_definition

component = st.sidebar.selectbox(
    label='Select Component',
    options=list(components.keys()),
    help='You can follow docs here to create a component and use this UI to build a DAG from it.'
)
# Select the component we will work with on main panel.
component_definition = components[component]

st.sidebar.subheader("Tasks:")
# Create a graphlib graph object
graph = graphviz.Digraph(engine='dot')

tasks = list(component_definition['tasks'].keys())
for i in range(1, len(tasks)):
    graph.edge(tasks[i - 1], tasks[i])

st.sidebar.graphviz_chart(graph)

st.sidebar.subheader("Args:")
st.sidebar.write(component_definition['args'])



st.header('Component config:  ' + component_definition['name'])
# @TODO Replace this with the YAML from API
expander = st.expander("Component Raw Definition")
expander.write(component_definition)
st.markdown("""---""")

if "initialized" not in st.session_state:
    st.session_state.initialized = True
    st.session_state.valid_schedule_interval = True


def validate_schedule_interval():
    print(st.session_state.schedule_value)
    global valid_schedule_interval
    schedule_interval_regex = (
            '(' + '@hourly|@daily|@weekly|@monthly|@yearly|' +
            r'((\*|\?|\d+((\/|\-){0,1}(\d+))*)\s*){5,6}' + '|' +
            r'rate\(\s*1\s+minute\s*\)' + '|' +
            r'rate\(\s*\d+\s+minutes\s*\)' + '|' +
            r'rate\(\s*1\s+hour\s*\)' + '|' +
            r'rate\(\s*\d+\s+hours\s*\)' + '|' +
            r'rate\(\s*1\s+day\s*\)' + '|' +
            r'rate\(\s*\d+\s+days\s*\)' +
            ')'
    )
    st.session_state.valid_schedule_interval = re.match(schedule_interval_regex, st.session_state.schedule_value) is not None
    print('*** match', bool(re.match(schedule_interval_regex, st.session_state.schedule_value)))
    print('*** valid_sched_interval', st.session_state.valid_schedule_interval)


with st.container():
    c1, c2, c3 = st.columns((4, 3, 3))
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
        schedule_value = st.text_input(
            label='Schedule interval',
            value='rate(1 day)',
            key='schedule_value',
            help='Rate interval',
            on_change=validate_schedule_interval,
        )
    if not st.session_state.valid_schedule_interval:
        st.warning('Schedule interval is not valid')



return_vals = {}
return_vals['base_component'] = component_lst['components'][component] + '.' + component
return_vals['name'] = dag_name
return_vals['schedule_interval'] = schedule_value
return_vals['granularity'] = schedule_granularity
return_vals['component_arguments'] = {}

st.markdown("""---""")
st.subheader('Args:')


hook_list = {}





for arg in component_definition['args']:
    if component_definition['args'][arg] == 'str' and not component_definition['args'][arg].startswith('Literal'):
        c_field, c_side = st.columns((4, 6))
        with st.container():
            with c_field:
                return_vals['component_arguments'][arg] = st.text_input(
                    label=arg,
                    value='my_value',
                    key=arg,
                    help='String type'
                )
    elif type(component_definition['args'][arg]) == dict and component_definition['args'][arg]['type'] == 'bool':
        c_field, c_side = st.columns((4, 6))
        with st.container():
            with c_field:
                bool_input = st.selectbox(
                    label=arg,
                    options=['True','False'],
                    index=['True','False'].index(str(component_definition['args'][arg]['default'])),
                    #value=component_definition['args'][arg]['default'],
                    key=arg,
                    help='True or False. Default is : ' + str(component_definition['args'][arg]['default'])
                )
                return_vals['component_arguments'][arg] = True if bool_input=="True" else False

    elif str(component_definition['args'][arg]).startswith('Literal') and '|' in component_definition['args'][arg]:

        regex = r"Literal\[\'(.*?)\'\]\s*\|*\s*"
        matches = re.findall(regex, component_definition['args'][arg], re.MULTILINE)

        c_field, c_side = st.columns((4, 6))
        with st.container():
            with c_field:
                return_vals['component_arguments'][arg]  = st.selectbox(
                    label=arg,
                    options=matches,
                    key=arg,
                    help='Choose one of the Literal options'
                )

    elif 'Hook' in component_definition['args'][arg]:
        c_field, c_side = st.columns((4, 6))
        with st.container():
            hooks_present = get_connections() != []
            with c_field:
                if hooks_present:
                    hook_user_value = st.selectbox(
                        label=arg,
                        key=arg,
                        options=get_connections(),
                        help='Choose from available connection of Type ' + (component_definition['args'][arg])
                    )
                else:
                    hook_user_value = st.text_input(
                        label=arg,
                        value="hook_name ** note: you must add it **",
                        key=arg,
                        help='''Example:
                                   my_env_db_hook
                        '''
                    )
            with c_side:
                if hooks_present:
                    st.write("")
                    st.info("To add other hooks use the [typhoon cli] (https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/connections.html#adding-the-connection).")
                    hook_list[arg] = 'my_hook_name'
                else:
                    st.write("")
                    st.error("Please add hooks using [typhoon cli] (https://typhoon-data-org.github.io/typhoon-orchestrator/getting-started/connections.html#adding-the-connection).")
                    hook_list[arg] = 'my_hook_name'

        return_vals['component_arguments'][arg] = {"__typhoon__": "Py", "value": "$HOOK." + hook_user_value}

    elif component_definition['args'][arg] == 'List[str]':
        c_field, c_side = st.columns((4, 6))
        with c_field:
             list_input = st.text_area(
                label=arg,
                value='- item 1 \n- item 2 \n- item 3',
                key=arg,
                help='Type ' + (component_definition['args'][arg]) + ' \n \n(hint :) YAML formatted list. e.g. \n- item 1 \n- item 2 \n- item 3',
                height= 200
             )
        with c_side:
             try:
                 parsed_list = yaml.safe_load(list_input)
                 st.info("Please input a YAML formatted list. \n- item 1 \n- item 2 \n- item 3")
             except:
                 st.error("Please input a [YAML formatted list] (https://docs.ansible.com/ansible/latest/reference_appendices/YAMLSyntax.html#yaml-basics). \n- table_1 \n- table_2 \n- table_3. ")
                 parsed_list = None
        return_vals['component_arguments'][arg] = parsed_list

    elif component_definition['args'][arg] == 'dict':
        c_field, c_side = st.columns((4, 6))
        with c_field:
            return_vals['component_arguments'][arg] = st.text_input(
                label=arg,
                value='<my_value> - (hint: ' + str((component_definition['args'][arg])) +')',
                key=arg,
                help='Type ' + str((component_definition['args'][arg]))
            )

    else:
        c_field, c_side = st.columns((4, 6))
        with c_field:

            return_vals['component_arguments'][arg] = st.text_input(
                label=arg,
                value='<my_value>',
                key=str(arg),
                help='String type'
            )

dag_from_component = json.loads("{}")
dag_success = json.loads("{}")

if st.button('Create DAG'):
    with st.spinner('Creating DAG from your Component...'):
        # First create dag from component
        dag_from_component = post_create_dag_from_component(return_vals)
        # st.write(dag_from_component)
        # Create the Dag
        dag_success = put_create_dag(dag_from_component)
    component_def = make_request(f'{api_str}/dags-build')

    st.success('Done! \n\n Created DAG path' + dag_success)
    st.balloons()

# return_val_expander = st.expander("DAG return Values")
# return_val_expander.json(json.dumps(return_vals))
#
# expander = st.beta_expander("Resulting DAG from API")
# expander.write(dag_from_component)
