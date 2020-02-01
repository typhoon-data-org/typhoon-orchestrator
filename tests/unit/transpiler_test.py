# import yaml
#
# from typhoon.core.transpiler import transpile
#
# DAG_CODE = """
# name: example_dag
# schedule_interval: rate(10 minutes)
# active: true
#
# nodes:
#   send_data:
#     function: typhoon.flow_control.branch
#     config:
#       branches:
#         - ['a.txt', 'Apples']
#         - ['b.txt', 'Mangoes']
#         - ['c.txt', 'Pears']
#
#   write_data:
#     function: typhoon.filesystem.write_data
#     config:
#       hook => APPLY: $HOOK.data_lake
#
# edges:
#   e1:
#     source: send_data
#     adapter:
#       data => APPLY: transformations.files.to_bytesIO($BATCH[1])
#       path => APPLY: f'store/{$BATCH[0]}'
#     destination: write_data
# """.strip()
#
#
# def test_transpile_dag():
#     dag = yaml.load(DAG_CODE)
#     assert transpile(dag=dag, debug_mode=False).strip() == """
# # DAG name: example_dag
# # Schedule interval = rate(10 minutes)
# import os
# from datetime import datetime
# from typing import Dict
#
# import typhoon.contrib.functions as typhoon_functions
# import typhoon.contrib.transformations as typhoon_transformations
# from typhoon.contrib.hooks.hook_factory import get_hook
# from typhoon.variables import get_variable_contents
# from typhoon.handler import handle
# from typhoon.core import SKIP_BATCH, task, DagContext, setup_logging
# from typhoon.models.task import task_logging_wrapper
#
# import transformations.files
#
# os.environ['TYPHOON_HOME'] = os.path.dirname(__file__)
# DAG_ID = 'example_dag'
#
#
# def example_dag_main(event, context):
#     setup_logging()
#     if event.get('type'):     # Async execution
#         return handle(event, context)
#
#     # Main execution
#     dag_context = DagContext.from_date_string(event['time'])
#
#     send_data_branches(dag_context=dag_context)
#
#
# # Branches
# @task(asynchronous=True, dag_name='example_dag')
# def send_data_branches(dag_context: DagContext, batch_num: int = 0):
#     data_generator = send_data_node(dag_context=dag_context, batch_num=batch_num)
#     for batch_num, data in enumerate(data_generator or [], start=1):
#         if data is SKIP_BATCH:
#             print(f'Skipping batch {batch_num} for send_data')
#             continue
#
#         config = {}
#         # Parameter data => APPLY
#         data_1 = transformations.files.to_bytesIO(data[1])
#         config['data'] = data_1
#
#         # Parameter path => APPLY
#         path_1 = f'store/{data[0]}'
#         config['path'] = path_1
#
#         write_data_branches(dag_context=dag_context, config=config, batch_num=batch_num)
#
#
# @task(asynchronous=True, dag_name='example_dag')
# def write_data_branches(dag_context: DagContext, config, batch_num: int = 0):
#     data_generator = write_data_node(dag_context=dag_context, config=config, batch_num=batch_num)
#     for batch_num, data in enumerate(data_generator or [], start=1):
#         if data is SKIP_BATCH:
#             print(f'Skipping batch {batch_num} for write_data')
#             continue
#
#         pass        # Necessary for the generator to be exhausted since it probably has side effects
#
#
# # Nodes
# def send_data_node(dag_context: DagContext, batch_num: int):
#     node_function = task_logging_wrapper(
#         dag_id=DAG_ID,
#         dag_context=dag_context,
#         task_id='send_data',
#         batch_num=batch_num,
#     )(typhoon_functions.flow_control.branch)       # Wrapper to add logging
#
#     config = {}
#
#     # Parameter branches
#     config['branches'] = [['a.txt', 'Apples'], ['b.txt', 'Mangoes'], ['c.txt', 'Pears']]
#
#     yield from node_function(
#         **config,
#     )
#
#
# def write_data_node(dag_context: DagContext, config: Dict, batch_num: int):
#     node_function = task_logging_wrapper(
#         dag_id=DAG_ID,
#         dag_context=dag_context,
#         task_id='write_data',
#         batch_num=batch_num,
#     )(typhoon_functions.filesystem.write_data)       # Wrapper to add logging
#
#     # Parameter hook => APPLY
#     hook_1 = get_hook("data_lake")
#     config['hook'] = hook_1
#
#     yield from node_function(
#         **config,
#     )
#     """.strip()
