from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
                       url (r'^$', 'counters.views.dashboard_view'),
#                       url (r'^pools_all_time', 'counters.views.overview_pools_all_time'), 
#                       url (r'^pools_interval', 'counters.views.overview_pools_interval'), 
                       url (r'^jobs', 'counters.views.jobs_view', name='jobs'), 
                       url (r'^job/(?P<jobid>\w+)', 'counters.views.job_detail_view', name='job_detail'),
                       url (r'^aggregate_pools', 'counters.views.not_implemented'),
                       url (r'^aggregate_users', 'counters.views.not_implemented'),
                       url (r'^aggregate_groups', 'counters.views.not_implemented'),
    # Examples:
    # url(r'^$', 'hdpstat.views.home', name='home'),
    # url(r'^hdpstat/', include('hdpstat.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)
