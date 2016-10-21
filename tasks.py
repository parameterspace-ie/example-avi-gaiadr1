"""
GAVIP Example AVIS: Gaia DR1 AVI

Pipelines that will be executed by the GAVIP AVI Framework.
"""
import matplotlib as mpl
# Run without UI
mpl.use('Agg')
from astropy.io.votable import parse_single_table
from bokeh.embed import components
from bokeh.models.ranges import Range1d
import bokeh.mpl as mplb
from bokeh.plotting import figure
import hashlib
import json
import logging
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pandas_profiling
from scipy.stats import gaussian_kde
import seaborn as sns
import shutil
import tempfile

from django.conf import settings
from django.utils import formats

from connectors.tapquery import AsyncJob
from pipeline.classes import AviTask, AviLocalTarget
from pipeline.models import AviJobRequest

from avi.models import VariableSourceJob

logger = logging.getLogger(__name__)


GMAG_TYPES = [('g_mag_abs_hip', 'Hipparcos', 'hip'), ('g_mag_abs_gaia', 'Gaia DR1', 'gaiadr1')]


GAIA_TAP_URL = "http://gea.esac.esa.int/tap-server/tap"

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
    return {field.name: getattr(job_model, field.name) for field in job_model_fields(job_model, excluded_fields)}
 
 
def job_completed(request_id):
    job_request = AviJobRequest.objects.get(job_id=request_id)#self.request_id)
    return formats.date_format(job_request.pipeline_state.last_activity_time, "SHORT_DATETIME_FORMAT")


class BaseFinalTask(AviTask):
    """
    Base final task in pipeline that generates GOG observations of sources.
    """
    def __init__(self, *args, **kwargs):
        super(BaseFinalTask, self).__init__(*args, **kwargs)
        self.excluded_fields = ['output_path']
        self.job_model.output_path = os.path.join(settings.OUTPUT_PATH, job_params_hash(self.job_model, self.excluded_fields))
        self.job_model.save()
        if not os.path.exists(self.job_model.output_path):
            os.makedirs(self.job_model.output_path)
        logger.info('avi output_path: %s' % (self.job_model.output_path))
        
    def output(self):
        raise NotImplementedError()

    def requires(self):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()


class RetrieveGacsData(AviTask):
    """
    Retrieve data from GACS using specified query.
    """

    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'gacs_data.vot'))

    def run(self):
        logger.info('Executing ADQL query: %s', self.job_model.query)
        gacs_tap_conn = AsyncJob(GAIA_TAP_URL, self.job_model.query, poll_interval=1)
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


class GenerateHrDiagrams(BaseFinalTask):
    """
    Final task in pipeline that generates HR diagrams.
    """
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


class RetrieveVariableSources(AviTask):
    """
    Retrieve variable star data from Gaia archive.
    """
    def _get_gaia_data(self, query, data_output_path):
        async_check_interval = 1
        logger.info('Executing ADQL query: %s', query)
        gacs_tap_conn = AsyncJob(GAIA_TAP_URL, query, poll_interval=async_check_interval)

        # Run the job (start + wait + raise_exception)
        gacs_tap_conn.run()

        # Store the response
        result = gacs_tap_conn.open_result()

        tmp_vot = tempfile.NamedTemporaryFile(delete = False)
        with open(tmp_vot.name, 'w') as f:
            f.write(result.content.decode("utf-8"))
    
        table = parse_single_table(tmp_vot.name).to_table()

        # finally delete temp files
        os.unlink(tmp_vot.name)
        df = pd.DataFrame(np.ma.filled(table.as_array()), columns=table.colnames)
        df.to_csv(data_output_path, index=False)
        logger.info('Persisted results from ADQL query %s at: %s', query, data_output_path)
        return df

    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'gaia_data', 'retrieved_data'))

    def run(self):
        output_path = os.path.dirname(self.output().path)
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)
        
        variables_csv_path = os.path.join(output_path, 'variables.csv')
        variables_df = self._get_gaia_data("""
        SELECT * FROM gaiadr1.variable_summary 
        WHERE source_id IN (SELECT source_id FROM gaiadr1.gaia_source WHERE (phot_variable_flag='VARIABLE'))
        """,
        variables_csv_path)
        
        variables_extended_df = self._get_gaia_data("SELECT * FROM gaiadr1.gaia_source WHERE (phot_variable_flag='VARIABLE')",
                                                    os.path.join(output_path, 'variables_extended.csv'))
        
        variables_df = variables_df.merge(variables_extended_df, on='source_id')
        variables_df = variables_df.rename(columns={'solution_id_x':'solution_id_var', 'solution_id_y': 'solution_id_source'})
        variables_df['Period'] = variables_df.apply(lambda x: 1./x.phot_variable_fundam_freq1, axis=1)
        variables_df['classification'] = variables_df['classification'].apply(lambda x: x.decode('utf-8'))
        variables_df.to_csv(variables_csv_path, index=False)
        logger.info('Persisted merged variables dataframe to %s', variables_csv_path)

        ### Now grab the photometry for one of these stars
        source_id = self.job_model.source_id
        if not source_id or source_id == VariableSourceJob.RANDOM_SOURCE_ID:
            grabstar = np.random.randint(len(variables_df))
            source_id = variables_df.source_id[grabstar]
        source_id = int(source_id)

        source_phot_path = os.path.join(output_path, 'source_phot.csv')
        phot_df = self._get_gaia_data("SELECT * FROM gaiadr1.phot_variable_time_series_gfov  WHERE (source_id=%s)" % source_id, 
                                      source_phot_path)

        period = variables_df.ix[variables_df.source_id==source_id, 'Period'].values[0]
        phot_df['BJD'] = phot_df.apply(lambda x: x.observation_time + 2455197.5, axis=1)
        phot_df['g_mag_err'] = phot_df.apply(lambda x: 1.086*x.g_flux_error/x.g_flux, axis=1)
        phot_df['Phase'] = phot_df.apply(lambda x: ((x.BJD/ period) - np.floor(x.BJD / period)), axis=1)
        
        phot_df.to_csv(source_phot_path, index=False)
        logger.info('Persisted photometry for source %s to %s', source_id, source_phot_path)
        
        with open(self.output().path, 'w') as f:
            f.write('')


class VisualiseVariableSource(BaseFinalTask):
    """
    Final task in pipeline that visualises a variable source
    """
    def output(self):
        return AviLocalTarget(os.path.join(self.job_model.output_path, 'variable_vis.json'))

    def requires(self):
        return self.task_dependency(RetrieveVariableSources)

    def run(self):
        """
        Generates the visualisation.
        """
        data_path = os.path.dirname(self.input().path)
        variables_df = pd.read_csv(os.path.join(data_path, 'variables.csv'))
        phot_df = pd.read_csv(os.path.join(data_path, 'source_phot.csv'))
        source_id = phot_df['source_id'].values[0]
        period = variables_df[variables_df.source_id==source_id]['Period'].values[0]
        period = str(np.around(period, decimals=4)) + 'd'
        source_type = variables_df[variables_df.source_id==source_id]['classification'].values[0]
        generation_date = job_completed(self.request_id)
        profile_cols = ['g_flux', 'g_flux_error', 'g_magnitude', 'g_mag_err']

        phot_profile = pandas_profiling.ProfileReport(phot_df[profile_cols], correlation_overrides=profile_cols)
        job_output = {'variablesource': {
                                 'generation_date': generation_date,
                                 'source_id': source_id,
                                 'source_type': source_type,
                                 'period': period,
                                 'ra': variables_df[variables_df.source_id==source_id]['ra'].values[0],
                                 'dec': variables_df[variables_df.source_id==source_id]['dec'].values[0],
                                 'l': variables_df[variables_df.source_id==source_id]['l'].values[0],
                                 'b': variables_df[variables_df.source_id==source_id]['b'].values[0],
                                 'parallax': variables_df[variables_df.source_id==source_id]['parallax'].values[0],
                                 'parallax_error': variables_df[variables_df.source_id==source_id]['parallax_error'].values[0],
                                 'phot_g_mean_mag': variables_df[variables_df.source_id==source_id]['phot_g_mean_mag'].values[0],
                                 'phot_profile': phot_profile.html,
                                 },
                      }            
        source_id = str(source_id)
        
        sns.set_style("white")
        sns.set_context("paper", font_scale=1.5, rc={"lines.linewidth": 2.0})
        sns.set_palette(sns.husl_palette(10, l=.4))
        colors = sns.color_palette()
        
        plt.figure(figsize=(10,5))
        plt.errorbar(np.concatenate((phot_df.Phase, phot_df.Phase + 1.0, phot_df.Phase + 2.0)) , np.concatenate((phot_df.g_magnitude, phot_df.g_magnitude, phot_df.g_magnitude)), yerr = np.concatenate((phot_df.g_mag_err, phot_df.g_mag_err, phot_df.g_mag_err)), ls='None',zorder=4, color=colors[0], label='_nolegend_')
        plt.plot(np.concatenate((phot_df.Phase, phot_df.Phase + 1.0, phot_df.Phase + 2.0)) , np.concatenate((phot_df.g_magnitude, phot_df.g_magnitude, phot_df.g_magnitude)), 'o', color=colors[0], ls='None', zorder=4, markeredgecolor='Grey', markeredgewidth=1, label='G')
        plt.xlabel('Phase')#\phi$)')
        plt.ylabel('G Magnitude')
        bp=mplb.to_bokeh()
        bp.title.text = 'ID = %s, %s, %s' % (source_id, period, source_type)
        bp.title.align = "center"
        bp.y_range.flipped=True
        source_script, source_div = components(bp)
        
        job_output['variablesource']['source_script'] = source_script
        job_output['variablesource']['source_div'] = source_div

        # Add model values
        job_output['variablesource'].update(model_to_dict(self.job_model, self.excluded_fields))
        
        job_output['variablesource']['source_id'] = source_id

        logger.debug('Job model: %s, set job output to: %s', self.job_model, job_output)
        with open(self.output().path, 'w') as out:
            json.dump(job_output, out)
