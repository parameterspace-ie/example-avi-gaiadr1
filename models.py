"""
GAVIP Example AVIS: Gaia DR1 AVI

Django models used by the AVI pipeline
"""

from django.db import models
from pipeline.models import AviJob


class GaiaDr1Job(AviJob):
    class Meta:
        abstract = True
    
    output_path = models.CharField(max_length=100, null=True, blank=True)
    
    def get_absolute_url(self):
        return "%i/" % self.pk
    
    
class HrJob(GaiaDr1Job):
    """
    All parameters required to generate the HR diagrams in a pipeline.
    """
    
    query = models.CharField(max_length=2000, default="""SELECT gaia.source_id, gaia.hip,
                 gaia.phot_g_mean_mag+5*log10(gaia.parallax)-10 as g_mag_abs_gaia,
                 gaia.phot_g_mean_mag+5*log10(hip.plx)-10 as g_mag_abs_hip,
                 hip.b_v
          FROM gaiadr1.tgas_source AS gaia
          INNER JOIN public.hipparcos as hip
          ON gaia.hip = hip.HIP
          WHERE gaia.parallax/gaia.parallax_error >= 5 AND
          hip.plx/hip.e_plx >= 5 AND
          hip.e_b_v > 0.0 and hip.e_b_v <= 0.05 AND
          (2.5/log(10))*(gaia.phot_g_mean_flux_error/gaia.phot_g_mean_flux) <= 0.05
    """)
    """
    This is a modified version of the T. Boch query, which has been updated to use the GACS names.
    """

    pipeline_task = 'GenerateHrDiagrams'
    """
    The associated pipeline task name.
    """


class VariableSourceJob(GaiaDr1Job):
    """
    All parameters required to generate the variable source visualisations in a pipeline.
    """
    
    RANDOM_SOURCE_ID = "<select random source id>"
    
    source_id = models.CharField(max_length=2000, default=RANDOM_SOURCE_ID)
    """
    Id of source to be visualised.
    """

    pipeline_task = 'VisualiseVariableSource'
    """
    The associated pipeline task name.
    """

