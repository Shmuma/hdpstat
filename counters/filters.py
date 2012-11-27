from django import forms

from counters.models import TaskInstance, User, Pool, CounterGroup


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
    user = forms.MultipleChoiceField (required = False, choices = [(u.name, u.name) for u in User.objects.order_by ('name')])
    pool = forms.MultipleChoiceField (required = False, choices = [(p.name, p.name) for p in Pool.objects.order_by ('name')])
    days = forms.ChoiceField (required = False, choices = DAYS_CHOICES, initial=1)
    job_name = forms.CharField (label = "Job", required = False)
    cgroup = forms.ChoiceField (label = "Show counters", required = False, choices = get_counters ())
    sort = forms.CharField (widget = forms.HiddenInput)
