from django.db import models


class TaskGroup (models.Model):
    name = models.CharField (max_length=255)


class Task (models.Model):
    name = models.CharField (max_length=255)
    taskGroup = models.ForeignKey (TaskGroup)


class Pool (models.Model):
    name = models.CharField (max_length=100)


class User (models.Model):
    name = models.CharField (max_length=100)


class TaskInstance (models.Model):
    task = models.ForeignKey (Task)
    pool = models.ForeignKey (Pool)
    user = models.ForeignKey (User)

    submitted = models.DateTimeField ()
    started = models.DateTimeField ()
    finished = models.DateTimeField ()

    mappers = models.PositiveIntegerField ()
    reducers = models.PositiveIntegerField ()

    jobid = models.CharField (max_length=100)
    status = models.PositiveSmallIntegerField ()


class CounterGroup (models.Model):
    name = models.CharField (max_length=100)


class Counter (models.Model):
    name = models.CharField (max_length=100)
    tag = models.CharField (max_length=100)
    counterGroup = models.ForeignKey (CounterGroup)


class CounterValue (models.Model):
    taskInstance = models.ForeignKey (TaskInstance)
    counter = models.ForeignKey (Counter)
    value = models.BigIntegerField ()
