import logging
import contextvars
from logging.handlers import QueueHandler, QueueListener
import queue
import atexit

# The context var to hold our request ID
request_id_var = contextvars.ContextVar('request_id', default='-')
request_user_var = contextvars.ContextVar('request_user', default='-')

class RequestIDFilter(logging.Filter):
    """
    A logging filter that injects the current request ID into the log record.
    """
    def filter(self, record):
        record.request_id = request_id_var.get()
        record.request_user = request_user_var.get()
        return True

class MultiLineFormatter(logging.Formatter):
    """
    A formatter that ensures multi-line messages (like tracebacks or logs
    with newlines) get the timestamp and request ID prepended to EVERY line.
    """
    def format(self, record):
        # Format the record to get the initial formatted string (which may have \n)
        s = super().format(record)
        lines = s.split('\n')
        
        if len(lines) > 1:
            # We want each subsequent line to have the same prefix as the first line.
            # To do this safely, we construct the prefix manually using standard formats.
            # The format in settings.py is: %(asctime)s - %(levelname)s - [%(request_id)s] - ...
            # We can use formatTime and record data to reconstruct it.
            time_str = self.formatTime(record, self.datefmt)
            level = record.levelname
            req_id = getattr(record, 'request_id', '-')
            req_user = getattr(record, 'request_user', '-')
            
            prefix = f"{time_str} - {level} - [{req_id}|{req_user}] - "
            
            # The first line has it applied already by the super().format(). 
            # We apply it to the remaining lines.
            formatted_lines = [lines[0]] + [f"{prefix}{line}" for line in lines[1:]]
            s = '\n'.join(formatted_lines)
            
        return s

import concurrent.futures

class SimpleAsyncFileHandler(logging.FileHandler):
    """
    A file handler that delegates disk writes to a background thread.
    This guarantees non-blocking, asynchronous logging without complex QueueListeners.
    """
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        super().__init__(filename, mode, encoding, delay)
        # Background thread executor for log serialization and disk I/O
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="AsyncLogger")

    def emit(self, record):
        # Fire and forget into the background thread.
        # Since log records are simple data objects, they cross thread boundaries safely.
        self._executor.submit(self._async_emit, record)

    def _async_emit(self, record):
        super().emit(record)

    def close(self):
        self._executor.shutdown(wait=True)
        super().close()

