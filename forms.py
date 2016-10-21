"""
This module contains all ``django.forms.ModelForm`` implementations associated with AVI pipeline jobs (``gavip_avi.pipeline.models.AviJob``). 
The model subclasses of ``AviJob`` themselves are implemented in :mod:`avi.models`.
"""
from django.forms import ModelForm, Textarea

from avi.models import HrJob, VariableSourceJob

DEFAULT_EXCLUDED_MODEL_FIELDS = [
                                 'expected_runtime',
                                 'output_path',
                                 'request',
                                 'resources_ram_mb',
                                 'resources_cpu_cores'
                                 ]        


class HrForm(ModelForm):
    """
    ModelForm for HR plot generation
    """
    class Meta:
        model = HrJob
        exclude = DEFAULT_EXCLUDED_MODEL_FIELDS
        
        widgets = {
          'query': Textarea(attrs={'rows':15, 'cols':80}),
        }


class VariableSourceForm(ModelForm):
    """
    ModelForm for variable source visualisation
    """
    class Meta:
        model = VariableSourceJob
        exclude = DEFAULT_EXCLUDED_MODEL_FIELDS
        
#         widgets = {
#           'source_id': Textarea(attrs={'rows':1, 'cols':80}),
#         }
