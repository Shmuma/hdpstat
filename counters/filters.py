from django import forms

from counters.models import TaskInstance, User, Pool


def get_statuses ():
    """
    Return list of statuses to render
    """
    res = []
    for val, name in TaskInstance.STATUSES:
        if val != 0:
            res.append ((name, name))
    return res


class FilterForm (forms.Form):
    DAYS_CHOICES = (
        (1, '1 day'),
        (7, '1 week'),
        (14, '2 weeks'),
        (30, '1 month'),
        (90, '3 months'))

    status = forms.MultipleChoiceField (required = False, choices = get_statuses (),
                                        widget = forms.CheckboxSelectMultiple)
    user = forms.MultipleChoiceField (required = False, choices = [(u.name, u.name) for u in User.objects.all ()])
    pool = forms.MultipleChoiceField (required = False, choices = [(p.name, p.name) for p in Pool.objects.all ()])
    days = forms.ChoiceField (required = False, choices = DAYS_CHOICES)
