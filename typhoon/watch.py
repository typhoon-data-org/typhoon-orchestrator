import time
from typing import Optional

from watchdog.events import PatternMatchingEventHandler, FileSystemEventHandler
from watchdog.observers import Observer

from typhoon.core.settings import Settings
from typhoon.deployment.packaging import build_all_dags


def on_created(event):
    print(f"DAG {event.src_path} has been created")
    build_all_dags(remote=None)


def on_deleted(event):
    print(f"DAG {event.src_path} has been deleted")
    build_all_dags(remote=None)


def on_modified(event):
    print(f"DAG {event.src_path} has been modified")
    build_all_dags(remote=None)


def on_moved(event):
    print(f"DAG {event.src_path} has been moved to {event.dest_path}")
    build_all_dags(remote=None)


def setup_event_handler(patterns: Optional[str] = None) -> FileSystemEventHandler:
    event_handler = PatternMatchingEventHandler(
        ignore_directories=True,
        patterns=patterns or "*.yml",
        case_sensitive=True,
    )
    # event_handler.on_created = on_created
    # event_handler.on_deleted = on_deletedenv)
    event_handler.on_modified = on_modified
    # event_handler.on_moved = on_moved
    return event_handler


def setup_observer(event_handler) -> Observer:
    path = Settings.dags_directory
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


def watch_changes(patterns: Optional[str] = None):
    event_handler = setup_event_handler(patterns)
    observer = setup_observer(event_handler)
    start_observer(observer)
