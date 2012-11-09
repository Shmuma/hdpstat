"""
Tasks classifier - reclassify all tasks from DB
"""
import os
import sys

from counters.lib import tasks
from counters.lib import settings

from counters.models import Task, TaskGroup

from django.core.management.base import BaseCommand, CommandError


class Command (BaseCommand):
    def handle (self, *args, **options):
        opts = settings.init ()

        os.chdir (opts['WorkDir'])
        tasks.init (opts['TaskClassesFile'])

        total = updated = 0

        for t in Task.objects.all ():
            groupName = tasks.classify (t.name)
            total += 1
            if groupName != t.taskGroup.name:
                group = TaskGroup.objects.get_or_create (name=groupName)[0]
                t.taskGroup = group
                t.save ()
                updated += 1
        print "Processed %d tasks, updated %d" % (total, updated)


