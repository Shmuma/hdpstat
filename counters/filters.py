from django import forms

from counters.models import TaskInstance, User, Pool, CounterGroup, TaskGroup


def get_statuses ():
    """
    Return list of statuses to render
    """
    res = []
    for val, name in TaskInstance.STATUSES:
        if val != 0:
            res.append ((name, name))
    return res


def get_counters ():
    """
    Counter groups requires special handling
    """
    res = [('Time', 'Time')]
    for cg in CounterGroup.objects.all ():
        if cg.name != 'Time':
            res.append ((cg.name, cg.name))
    return res           


class FilterForm (forms.Form):
    DAYS_CHOICES = (
        (1, '1 day'),
        (7, '1 week'),
        (14, '2 weeks'),
        (30, '1 month'),
        (90, '3 months'),
        (3650, 'All time'))

    status = forms.MultipleChoiceField (required = False, choices = get_statuses (),
                                        widget = forms.CheckboxSelectMultiple)
    user = forms.ModelMultipleChoiceField (required = False, queryset = User.objects.order_by ('name'))
    pool = forms.ModelMultipleChoiceField (required = False, queryset = Pool.objects.order_by ('name'))
    job_name = forms.CharField (label = "Job name", required = False)
    task_group = forms.ModelMultipleChoiceField (label = "Group", required = False, queryset = TaskGroup.objects.order_by ('name'))
    days = forms.ChoiceField (required = False, choices = DAYS_CHOICES, initial=1)
    cgroup = forms.ChoiceField (label = "Show counters", required = False, choices = get_counters ())
    sort = forms.CharField (widget = forms.HiddenInput)
