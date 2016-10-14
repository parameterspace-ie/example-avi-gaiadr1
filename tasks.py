"""
GAVIP Example AVIS: Gaia DR1 AVI

Pipelines that will be executed by the GAVIP AVI Framework.
"""
from astropy.io.votable import parse_single_table
from bokeh.embed import components
from bokeh.models.ranges import Range1d
from bokeh.plotting import figure
import hashlib
import json
import logging
import matplotlib as mpl
# Run without UI
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pandas_profiling
from scipy.stats import gaussian_kde

from django.conf import settings
from django.utils import formats

from connectors.tapquery import AsyncJob
from pipeline.classes import AviTask, AviLocalTarget
from pipeline.models import AviJobRequest

logger = logging.getLogger(__name__)


GMAG_TYPES = [('g_mag_abs_hip', 'Hipparcos', 'hip'), ('g_mag_abs_gaia', 'Gaia DR1', 'gaiadr1')]


AVIJOB_FIELDS = set(['id',
                     'user',
                     'request',
                     'expected_runtime',
                     'resources_ram_mb',
                     'resources_cpu_cores'])
 
def job_model_fields(job_model, excluded_fields=[]):
    """
    Retrieve all non-AVI framework fields from a job model. 
     
    Parameters
    ----------
    job_model : :mod:`pipeline.models.AviJob` 
        Model containing parameters for the pipeline job
             
    Returns
    -------
    list
        list of :mod:`django.db.models.Field`
    """
    # TODO: move into AviTask.
    all_excluded_fields = list(excluded_fields)
    all_excluded_fields.extend(AVIJOB_FIELDS)
    return [x for x in job_model._meta.get_fields() if x.name not in all_excluded_fields]
 
 
def job_params_hash(job_model, excluded_fields, *args):
    """
    Generate a job-specific hash from the values all non-AVI framework fields from a job model. 
    This can be used to generate job-specific output directories for a pipeline.
     
    Parameters
    ----------
    job_model : :mod:`pipeline.models.AviJob` 
        Model containing parameters for the pipeline job
    *args : 
        Any additional arguments
             
    Returns
    -------
    str
        Unique hash hex digest
    """
    # TODO: move into AviTask.
    logger.info('Job model: %s', job_model)
    logger.info('Job model fields: %s', job_model_fields(job_model, excluded_fields))
    params_str = '_'.join([str(getattr(job_model, field.name)) for field in job_model_fields(job_model, excluded_fields)])
    params_str += '_%s' % '_'.join([str(x) for x in args])
    return hashlib.md5(params_str.encode('utf-8')).hexdigest()
 
 
def model_to_dict(job_model, excluded_fields = []):
    """
    Convert a task model to a dictionary. This can be used in a pipeline JSON output.
 
    Returns
    -------
    dict
        Dictionary containing all non-framework model fields and their values
    """
    # TODO: move into AviTask.
    return {field.name: getattr(job_model, field.name) for field in job_model_fields(job_model, excluded_fields)}
 
 
def job_completed(request_id):
    # TODO: this is taken from AviTask, but could be available in a new AviTask function
    job_request = AviJobRequest.objects.get(job_id=request_id)#self.request_id)
    # TODO: refactor - date formatting can be done in template?
    # TODO: the completed date should be set in the view, as technically it's completed once
    # this task is finished. However, the completed date could also be interpreted as the
    # GOG observation executed in one of the earlier pipeline tasks.
    return formats.date_format(job_request.pipeline_state.last_activity_time, "SHORT_DATETIME_FORMAT")


class RetrieveGacsData(AviTask):
    """
    Retrieve data from GACS using specified query.
    """

    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'hr_gacs_data.vot'))

    def run(self):
        logger.info('Executing ADQL query: %s', self.job_model.query)
        gacs_tap_conn = AsyncJob('http://gea.esac.esa.int/tap-server/tap', self.job_model.query, poll_interval=1)
        # Run the job (start + wait + raise_exception)
        gacs_tap_conn.run()

        # Store the response
        result = gacs_tap_conn.open_result()
        with open(self.output().path, 'w') as f:
            f.write(result.content.decode("utf-8"))
        logger.info('Persisted results from ADQL query: %s', self.job_model.query)


class CalculateDensity(AviTask):
    """
    Calculate KDE required for plots
    """
    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'hr_density.csv'))

    def requires(self):
        return self.task_dependency(RetrieveGacsData)
    
    def _calculate_density(self, df, xcol, ycol):
        numrows = len(df)
        x = np.reshape(np.array(df[xcol], copy=False).astype('float'), (numrows))
        y = np.reshape(np.array(df[ycol], copy=False).astype('float'), numrows)
        xy = np.vstack([x,y])

        return gaussian_kde(xy)(xy)
    
    def run(self):
        table = parse_single_table(self.input().path).to_table()
        df = pd.DataFrame(np.ma.filled(table.as_array()), columns=table.colnames)
        
        for gmag_col, _, _ in GMAG_TYPES:
            density_col = '%s_density' % gmag_col
            logger.info('Calculating density for %s', gmag_col)
            df[density_col] = self._calculate_density(df, 'b_v', gmag_col)
            logger.info('Calculated density for %s', gmag_col)

        df.to_csv(self.output().path, index=False)


class GenerateHrDiagrams(AviTask):
    """
    Final task in pipeline that generates HR diagrams.
    """
    def __init__(self, *args, **kwargs):
        super(GenerateHrDiagrams, self).__init__(*args, **kwargs)
        self.excluded_fields = ['output_path']
        self.job_model.output_path = os.path.join(settings.OUTPUT_PATH, job_params_hash(self.job_model, self.excluded_fields))
        self.job_model.save()
        if not os.path.exists(self.job_model.output_path):
            os.makedirs(self.job_model.output_path)
        logger.info('avi output_path: %s' % (self.job_model.output_path))

    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'hr_generation.json'))

    def requires(self):
        return self.task_dependency(CalculateDensity)

    def _density_plot(self, x, y, z, xlabel, ylabel, title, xlim=None, ylim=None):
        # Sort the points by density, so that the densest points are plotted last
        idx = z.argsort()
        x, y, z = x[idx], y[idx], z[idx]

        p=figure()
        colors = [
                  "#%02x%02x%02x" % (int(r), int(g), int(b)) for r, g, b, _ in 255*plt.cm.viridis(mpl.colors.Normalize()(z))
                  ]

        p.circle(x, y, fill_color=colors, line_color=None, size=2)
        p.title.text=title
        p.title.align = "center"
        p.xaxis.axis_label = xlabel
        p.yaxis.axis_label = ylabel
        if xlim:
            p.x_range=Range1d(*xlim)
        if ylim:
            p.y_range=Range1d(*ylim)
        return components(p)
            
    def run(self):
        """
        Generates the HR diagrams.
        """
        df = pd.read_csv(self.input().path)
        generation_date = job_completed(self.request_id)
        profile_cols = ['g_mag_abs_gaia', 'g_mag_abs_hip', 'b_v']
        source_pd_profile = pandas_profiling.ProfileReport(df[profile_cols], correlation_overrides=profile_cols)
        job_output = {'hr': {
                                 'generation_date': generation_date,
                                 'source_pd_profile': source_pd_profile.html,
                                 },
                      }            
        for gmag_col, title_type, gmag_type in GMAG_TYPES:
            density_col = '%s_density' % gmag_col
            plot_script, plot_div = self._density_plot(df['b_v'], 
                                                       df[gmag_col], 
                                                       df[density_col], 
                                                       '(B-V)', 
                                                       'Mg', 
                                                       'HR diagram (%s)' % title_type, xlim=(-0.2, 2), ylim=(13, -4))
            job_output['hr']['%s_script' % gmag_type] = plot_script
            job_output['hr']['%s_div' % gmag_type] = plot_div

        # Add model values
        job_output['hr'].update(model_to_dict(self.job_model, self.excluded_fields))
        
        logger.debug('Job model: %s, set job output to: %s', self.job_model, job_output)
        with open(self.output().path, 'w') as out:
            json.dump(job_output, out)
