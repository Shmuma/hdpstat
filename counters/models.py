from django.db import models


class TaskGroup (models.Model):
    name = models.CharField (max_length=255, unique=True)

    def __unicode__ (self):
        return self.name


class Task (models.Model):
    name = models.CharField (max_length=255, unique=True)
    taskGroup = models.ForeignKey (TaskGroup)

    def __unicode__ (self):
        return self.name


class Pool (models.Model):
    name = models.CharField (max_length=100, unique=True)

    def __unicode__ (self):
        return self.name


class User (models.Model):
    name = models.CharField (max_length=100, unique=True)

    def __unicode__ (self):
        return self.name


class TaskInstance (models.Model):
    STATUSES = (
        (0, 'UNKNOWN'),
        (1, 'RUNNING'),
        (2, 'FAILED'),
        (3, 'KILLED'),
        (4, 'SUCCESS'),
        )


    task = models.ForeignKey (Task)
    pool = models.ForeignKey (Pool)
    user = models.ForeignKey (User)

    submitted = models.DateTimeField (null=True)
    started = models.DateTimeField (null=True, db_index=True)
    finished = models.DateTimeField (null=True, db_index=True)

    started_maps = models.DateTimeField (null=True)
    finished_maps = models.DateTimeField (null=True)
    started_reducers = models.DateTimeField (null=True)
    finished_reducers = models.DateTimeField (null=True)

    mappers = models.PositiveIntegerField ()
    reducers = models.PositiveIntegerField ()

    jobid = models.CharField (max_length=100, unique=True)
    status = models.PositiveSmallIntegerField (choices=STATUSES)

    def __unicode__ (self):
        return self.jobid

    @staticmethod
    def statusValue (status):
        for k, v in TaskInstance.STATUSES:
            if status == v:
                return k
        return 0



class CounterGroup (models.Model):
    name = models.CharField (max_length=100, unique=True)

    def __unicode__ (self):
        return self.name


class Counter (models.Model):
    name = models.CharField (max_length=100)
    tag = models.CharField (max_length=100, unique=True)
    counterGroup = models.ForeignKey (CounterGroup)

    def __unicode__ (self):
        return self.name


class CounterValue (models.Model):
    taskInstance = models.ForeignKey (TaskInstance)
    counter = models.ForeignKey (Counter)
    value = models.BigIntegerField ()
