from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
                       url (r'^pools_all_time', 'counters.views.overview_pools_all_time'), 
                       url (r'^pools_interval', 'counters.views.overview_pools_interval'), 
    # Examples:
    # url(r'^$', 'hdpstat.views.home', name='home'),
    # url(r'^hdpstat/', include('hdpstat.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)
