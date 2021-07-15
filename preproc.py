import os
import sys
from datetime import date
import pandas as pd
import datetime as dt
import logging
import glob
from wearables import watchoff
import matplotlib.pyplot as plt

def preproc(in_file, device, sr='1T', truncate=True, write=True, plot=True, recording_period_min=7, interpolate_limit=10, interpolate_method='linear'):


    data = []

    try:
        today = date.today()

        out_dir = os.path.dirname(in_file) + '/preproc/'

        if not os.path.isdir(out_dir):
            os.mkdir(out_dir)
            print("created output directory %s" % (out_dir))

        devices = ['fitbit', 'actiwatch']

        logger = logging.getLogger(__name__)
        f_handler = logging.FileHandler(out_dir + str(recording_period_min) + '_days.log')
        logger.addHandler(f_handler)
        #logging.basicConfig(filename=log_file, filemode='x', format='%(asctime)s - %(message)s', level=logging.INFO)


        if device == 'actiwatch':

            record_id = os.path.basename(in_file).str.split('_')[0] # check this

            with open(in_file) as f:
                for i, l in enumerate(f):
                    if ' Epoch-by-Epoch Data ' in l:
                        try:
                            data = pd.read_csv(in_file, skiprows = i+11, usecols = [1,2,3])
                            print('successfully read Actiware data file')
                        except:
                            try:
                                data = pd.read_csv(in_file, skiprows = i+12, usecols = [1,2,3])
                                print('successfully read Actiware data file')
                            except:
                                print('unable to read Actiware data file')

                        break

            data['Time'] = data['Date'] + ' ' + data['Time']
            data['Time'] = pd.to_datetime(data['Time'])

        elif device == 'fitbit':

            record_id = os.path.basename(in_file).split("WA_")[1][0:5]

            data = pd.read_csv(in_file)
            data.columns = ['Time', 'Activity']
            data['Time'] = pd.to_datetime(data['Time'])

        else:
            raise ValueError("Invalid device type. Expected one of: %s" % devices)

        print('record %s' % (record_id))

        data.index = data['Time']
        data = data.resample(sr).sum()
        data = data['Activity']

        if device == 'fitbit':
            data = watchoff.watchoff(record_id, data, in_file, out_dir)

        start_time = data.first_valid_index() # TO DO: find first non-zero activity value
        end_time = data.last_valid_index()
        period = end_time - start_time

        raw = data
        raw.to_csv(out_dir + '/' + record_id + '.csv', index=True, index_label=None, header=None, na_rep='NaN')

        missingNum = data.isnull().sum()
        error = 0
        logging.info('%s processing' % record_id)

        if missingNum > 0:
            # remove trailing and leading activity values
            length_init = len(data)
            data = data.loc[start_time:end_time]

            logging.info('----- removed leading and trailing NaN activity values')
            missingNum = data.isnull().sum()

        if missingNum > 0:
            # interpolate
            data.interpolate(method=interpolate_method, limit=interpolate_limit, inplace=True, limit_area='inside')
            logging.info('----- interpolated with %s, limit = %s' % (interpolate_method, interpolate_limit))
            if not os.path.isdir(out_dir + '/interpolated/'):
                os.makedirs(out_dir + '/interpolated/')
            data.to_csv(out_dir + '/interpolated/%s_interpolated-method-%s_lim-%s-epoch.csv' % (record_id, interpolate_method, interpolate_limit), index=True, index_label=None, header=None, na_rep='NaN')
            missingNum = data.isnull().sum()

        # truncating to first ndays of data
        if truncate == True:
            data = data[data.index <= (start_time + dt.timedelta(seconds=30) +
                        dt.timedelta(days=recording_period_min))]
            end_time = data.last_valid_index()
            period = end_time - start_time
            logging.info('----- truncated recording period to %s days' % recording_period_min)
            missingNum = data.isnull().sum()
            if not os.path.isdir(out_dir + '/truncated/'):
                os.makedirs(out_dir + '/truncated/')
            data.to_csv(out_dir + '/truncated/%s_truncated-%s_d.csv' % (record_id, recording_period_min), index=True, index_label=None, header=None, na_rep='NaN')

        if plot == True:
            f, axs = plt.subplots(2, 1, sharex=True)
            axs[0].plot(raw.index, raw, color = 'blue')
            axs[0].set_title(record_id + ', ' + str(recording_period_min) + ' days')
            axs[0].xaxis.set_visible(False)

            axs[1].plot(data.index, data, color = 'red')
            plt.xticks(rotation=45)
            plt.tight_layout()

            if not os.path.isdir(out_dir + '/figures/'):
                os.makedirs(out_dir + '/figures/')
            plt.savefig(out_dir + '/figures/' + record_id + '_' + str(recording_period_min) + '_d_interpolate-' + interpolate_method + '.png', dpi = 300)

        if missingNum > 0.10 * len(data):
            print('----- error: missing values = %.2f percent' %
                  (100*(missingNum / len(data))))
            logging.warning(
                '----- discard: missing more than 10 percent of data, %.2f percent missing' % (missingNum / len(data)))
            error = error + 1

        if period < dt.timedelta(days=recording_period_min):
            print('----- error: less than %s days actigraphy data - recording period is %s ' %
                  (str(recording_period_min), str(period)))
            logging.warning('----- discard: insufficient recording period %s' %
                            (str(period)))
            error = error + 1

        if missingNum > 0:
            print('... error: after processing, still missing %.2f percent data' %
                  (100*(missingNum/len(data))))
            logging.warning(
                '----- error: missing %.2f percent after processing' % (100*(missingNum/len(data))))
            error = error + 1

        if error == 0:
            logging.info('----- success: %.2f percent NaN, %s recording period' %
                         (100*(missingNum / len(data)), str(period)))

            print('----- success: %.2f percent NaN, %s recording period' %
                  (100*(missingNum / len(data)), str(period)))
        else:
            print('----- exclude from analysis')

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('unable to preprocess subject %s' % record_id)
        print(e)
        print(exc_type, fname, exc_tb.tb_lineno)

    return data
