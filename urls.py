"""
GAVIP Example AVIS: Gaia DR1 AVI

Django URLs and their corresponding views.
"""
from django.conf.urls import include, patterns, url
from avi import views

from plugins.urls import job_list_urls

urlpatterns = patterns(
    '',
    # TODO: renamed back to job_result for use with the framework job list panel plugin
#     url(r'^job/(?P<job_id>[0-9]+)/$', views.job_output, name='job_output'),
    url(r'^job/(?P<job_id>[0-9]+)/$', views.job_output, name='job_result'),
    
#     url(r'^simulation/synthetic/job/(?P<job_id>[0-9]+)/(?P<source_id>[A-Za-z0-9_-]+)/$', simviews.synthetic_job_source_output, name='syn_job_source'),

#     url(r'^help/$', views.help_documentation, name='help'),
    
    
    # HTML views
    url(r'^hr$', views.hr_generation, name='hr_generation'),
    url(r'^variable$', views.variable_visualisation, name='variable_visualisation'),
    
    # REST views
    url(r'^$', views.Index.as_view(), name='index'),

    url(r'^job_list/', include(job_list_urls, namespace='job_list')),

)
