"""
GAVIP Example AVIS: Gaia DR1 AVI

Django views for simulation pipelines defined in :mod:`avi.tasks`.
"""
import json
import logging

from django.shortcuts import redirect, resolve_url
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from rest_framework.views import APIView
from rest_framework.renderers import TemplateHTMLRenderer
from rest_framework.response import Response

from avi.forms import HrForm, VariableSourceForm

from pipeline.models import AviJobRequest

logger = logging.getLogger(__name__)

def get_default_context():
    return {
            'hr_form': HrForm(),
            'variable_form': VariableSourceForm(),
            }

class Index(APIView):
    """
    Index HTML view
    """
    renderer_classes = [TemplateHTMLRenderer]
    template_name = 'avi/index.html'

    def get(self, request):
        return Response(get_default_context())


@require_http_methods(["POST"])
def hr_generation(request):
    form = HrForm(request.POST)
        
    if not form.is_valid():
        context = get_default_context()
        context.update({'hr_invalid': True, 
                        'hr_form': form})
        return render(request, 'avi/index.html', context)
    form.save()
    return redirect('%s#job-tab' % resolve_url('avi:index'))


@require_http_methods(["POST"])
def variable_visualisation(request):
    form = VariableSourceForm(request.POST)
        
    if not form.is_valid():
        context = get_default_context()
        context.update({'variablesource_invalid': True, 
                        'variable_form': form})
        return render(request, 'avi/index.html', context)
    form.save()
    return redirect('%s#job-tab' % resolve_url('avi:index'))


def load_job_output(job_id, context):
    """
    Retrieves the JSON output generated by a particular pipeline job.
    
    Parameters
    ----------
    job_id : int
        Job identifier
    context : dict
        Context dictionary within which the job output JSON will be included.

    Returns
    -------
    dict
        Job output JSON
    """
    logger.info('At start of load_job_output(), context is %s', str(context).encode('utf-8'))
    file_path = AviJobRequest.objects.get(job_id=job_id).result_path
    context['job_id'] = job_id
    with open(file_path, 'r') as out_file:
        context.update(json.load(out_file))
    logger.info('Returning context: %s' % str(context).encode('utf-8'))
    return context


@require_http_methods(["GET"])
def job_output(request, job_id):
    """
    Retrieves the JSON output generated by a particular pipeline job.
    """
    return render(request, 'avi/index.html', context=load_job_output(job_id, context=get_default_context()))

