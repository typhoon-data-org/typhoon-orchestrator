import time

from watchdog.events import PatternMatchingEventHandler, FileSystemEventHandler
from watchdog.observers import Observer

from typhoon.core import settings


def create_on_created(env: str):
    from typhoon.cli import build_all_dags

    def on_created(event):
        print(f"DAG {event.src_path} has been created")
        build_all_dags(target_env=env, debug=True)
    return on_created


def create_on_deleted(env: str):
    from typhoon.cli import build_all_dags

    def on_deleted(event):
        print(f"DAG {event.src_path} has been deleted")
        build_all_dags(target_env=env, debug=True)
    return on_deleted


def create_on_modified(env: str):
    from typhoon.cli import build_all_dags

    def on_modified(event):
        print(f"DAG {event.src_path} has been modified")
        build_all_dags(target_env=env, debug=True)
    return on_modified


def create_on_moved(env: str):
    from typhoon.cli import build_all_dags

    def on_moved(event):
        print(f"DAG {event.src_path} has been moved to {event.dest_path}")
        build_all_dags(target_env=env, debug=True)
    return on_moved


def setup_event_handler(env: str) -> FileSystemEventHandler:
    event_handler = PatternMatchingEventHandler(
        ignore_directories=True,
        patterns="*.yml",
        case_sensitive=True,
    )
    # event_handler.on_created = create_on_created(env)
    # event_handler.on_deleted = create_on_deleted(env)
    event_handler.on_modified = create_on_modified(env)
    # event_handler.on_moved = create_on_moved(env)
    return event_handler


def setup_observer(event_handler) -> Observer:
    path = settings.dags_directory()
    go_recursively = True
    observer = Observer()
    observer.schedule(event_handler, path, recursive=go_recursively)
    return observer


def start_observer(observer: Observer):
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()


def watch_changes(target_env: str):
    """This is only for development, therefore as a precondition target_env must be set to local development"""
    event_handler = setup_event_handler(target_env)
    observer = setup_observer(event_handler)
    start_observer(observer)
