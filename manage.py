#!/usr/bin/env python
import os
import sys

from counters.lib import settings

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "hdpstat.settings")

    from django.core.management import execute_from_command_line

    settings.init ()

    execute_from_command_line(sys.argv)
