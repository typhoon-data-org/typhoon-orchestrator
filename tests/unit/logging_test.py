import logging.config

from typhoon.core.logger import setup_logging

config = """
version: 1
root:
  level: INFO
  handlers: [console]
handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    stream: ext://sys.stderr
"""


def test_logging_config(monkeypatch, tmp_path, capsys):
    (tmp_path / 'logger_config.yml').write_text(config)
    monkeypatch.setenv('TYPHOON_HOME', str(tmp_path))
    setup_logging()
    logging.info('Hello World!')
    captured = capsys.readouterr()
    assert captured.err == 'Hello World!\n'
