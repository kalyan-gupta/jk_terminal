from django.apps import AppConfig


class TradesConfig(AppConfig):
    name = 'trades'

    def ready(self):
        import os
        import sys
        
        # Avoid starting the order monitor during database migrations, tests, or static collection
        skip_commands = ['makemigrations', 'migrate', 'collectstatic', 'test', 'shell', 'showmigrations', 'check']
        if any(cmd in sys.argv for cmd in skip_commands):
            return

        # Only start the monitor in the main process (avoid starting twice during development reload)
        if os.environ.get('RUN_MAIN') == 'true' or os.environ.get('DEBUG') != 'True':
            from .order_monitor import start_order_monitor
            start_order_monitor()
