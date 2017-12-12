#-----------------------------------------------------------------------------------------------------------------
# description: use python to cut L1M EC into 10 bins on numerical input signals and get cutoffs,  and use these cutoffs to splite the current month EC into also 10 bins and check the difference
# date: 09/26/2017, Tuesday
# author: Xun Wei
#-----------------------------------------------------------------------------------------------------------------

import pandas as pd
import numpy as np
import os
import datetime
import sys


# input_path = '/home/xun.wei2/data/display.model_score.TCM_Dynamic_Decision_Tree_Solution_Scoring.dyn_tree_mst/'

# output_path = '/home/xun.wei2/data/display.model_score.TCM_Dynamic_Decision_Tree_Solution_Scoring.dyn_tree_mst/qc'

# labels = ['2017-08-26', '2017-07-29']

# input_path = '/home/xun.wei2/data/sigout.model_score.TCM_and_DYN_Decision_Tree_Solution_Scoring_D02.dyn_tree_mst/'

# output_path = '/home/xun.wei2/data/sigout.model_score.TCM_and_DYN_Decision_Tree_Solution_Scoring_D02.dyn_tree_mst/qc'

input_path = '/home/xun.wei2/data/sigout.model_score.TCM_and_DYN_Decision_Tree_Solution_Scoring_D02.audience_selection_dyn_tree_mst/'

output_path = '/home/xun.wei2/data/sigout.model_score.TCM_and_DYN_Decision_Tree_Solution_Scoring_D02.audience_selection_dyn_tree_mst/qc'

labels = ['2017-09-17', '2017-08-26']

# input_signals = ['RATIO_FS_TRIP_WITH_CPN',
#                  'CAT_CNT',
#                  'FS_TRIP_NON_CPN_NON_PROMO_RATIO',
#                  'coupon_sales_ratio',
#                  'CPN_BS_TCM_RED_L12M',
#                  'CPN_BS_TCM_RED_L3M',
#                  'TCM_RED_RATIO_DYN_MARGIN_L3_L12',
#                  'BS_LIFT_L12M',
#                  'PROMO_DEPTH_L12M']

input_signals = [ 'FS_TRIP',
                  'MD_RED_PERS',
                  'margin_rate_L12M',
                  'FS_RETL_AMT',
                  'FS_SCAN_QTY_NON_CPN_NON_PROMO_RATIO',
                  'NON_CPN_BS_L12M',
                  'FS_COST_AMT_NON_CPN',
                  'FS_SCAN_AMT',
                  'NON_CPN_BS_L6M',
                  'NON_CPN_BS_L3M',
                  'pred_tgt_NonTCM_Retrain00',
                  'TOTAL_SAVING_L12M' ]

# input_file = 'dyn_tree_mst.csv'

input_file = 'audience_selection_dyn_tree_mst.csv'

bin_num = 10


def load_data(file):
    start_time = datetime.datetime.now()

    try:
        # reader = pd.read_csv(file, sep='|', nrows=10000000)  # for test
        reader = pd.read_csv(file, sep='|')
    except IOError:
        print 'file not exist!'

    end_time = datetime.datetime.now()
    print 'the time for reading file: %s seconds' % (end_time - start_time).seconds

    return reader

# Todo: save the describe result
def describe_data(L1M_df, cur_df, i):
    cur_description = cur_df.describe()[input_signals]
    L1M_description = L1M_df.describe()[input_signals]

    cur_des_file = 'des_cur_' + labels[i][:-3] + '.csv'
    L1M_des_file = 'des_L1M_' + labels[i][:-3] + '.csv'
    cur_description.to_csv(os.path.join(output_path, cur_des_file), index=True, sep='|')
    L1M_description.to_csv(os.path.join(output_path, L1M_des_file), index=True, sep='|')

    print 'the description of input signals:'
    print cur_description
    print L1M_description

    L1M_num = len(L1M_df)
    cur_num = len(cur_df)

    # use count() need a long time, need to calculate all non null columns
    # L1M_num = L1M_df.count()['xtra_card_nbr']
    # cur_num = cur_df.count()['xtra_card_nbr']
    print 'total last one month EC num: %d' %(L1M_num)
    print 'total current month EC num : %d' %(cur_num)

    return L1M_num, cur_num


# Todo: optimize this function
def get_cutoffs(df, signal, L1M_range, L1M_sig_num):
    cutoffs = []
    cutoffs_index = []

    j = 1
    while j < bin_num:
        cutoff_ind = j * L1M_range - 1
        if j > 1:
            while j < bin_num and cutoff_ind <= cutoffs_index[-1]:
                j += 1
                cutoff_ind = j * L1M_range - 1
                if j == bin_num -1:
                    cutoff_ind = L1M_sig_num - 1

        # attention, if not reset index, here not use loc or ix
        # now since we have reset index, so we can both use loc or iloc.
        cutoff = df.loc[cutoff_ind][signal]
        cutoffs.append(cutoff)

        # if the cutoff is not the last index, find the last index
        if cutoff_ind != L1M_sig_num - 1 and cutoff == df.loc[cutoff_ind + 1][signal]:
            cutoff_ind = L1M_sig_num - len(df[df[signal] > cutoff]) - 1

        cutoffs_index.append(cutoff_ind)
        j += 1

    print 'cutoffs-index: (length is %d)' % (len(cutoffs))
    print [(cutoff, index) for cutoff, index in zip(cutoffs,cutoffs_index)]

    return cutoffs, cutoffs_index


def get_condition(cutoffs, signal, k):
    condition = ''
    # caution the boundary!
    if k == 0:
        condition = signal + '<=' + str(cutoffs[k])
    elif k == len(cutoffs):
        condition = signal + '>' + str(cutoffs[k-1])
    else:
        condition = signal + '>' + str(cutoffs[k-1]) + '&' + signal + '<=' + str(cutoffs[k])

    return condition


def output(signal, L1M_cutoffs, L1M_dist, cur_dist):
    length = len(L1M_cutoffs)
    index = range(1, length)
    index.append('Null')

    signal_series = pd.Series([signal]*length, index=index)
    L1M_cutoffs_series = pd.Series(L1M_cutoffs, index=index)
    L1M_dist_series = pd.Series(L1M_dist, index=index)
    cur_dist_series = pd.Series(cur_dist, index=index)

    output_df = pd.DataFrame()
    output_df['signal'] = signal_series
    output_df['L1M_cutoff'] = L1M_cutoffs_series
    output_df['L1M_CNT_EC'] = L1M_dist_series
    output_df['cur_CNT_EC'] = cur_dist_series

    return output_df


def input_qc():
    start_time = datetime.datetime.now()

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for i in range(len(labels)-1):
        print '\n\n'
        print '******************** time label: %s ********************' %(labels[i])
        cur_file = input_path + labels[i] + '/' + input_file
        L1M_file = input_path + labels[i + 1] + '/' + input_file

        L1M_df = load_data(L1M_file)
        cur_df = load_data(cur_file)

        L1M_num, cur_num = describe_data(L1M_df, cur_df, i)

        output_df = pd.DataFrame()

        for signal in input_signals:
            print '\n'
            print '-------------------- signal: %s --------------------' %(signal)
            time_start = datetime.datetime.now()

            L1M_sig_df = L1M_df.loc[:, ['xtra_card_nbr', signal]].dropna()
            cur_sig_df = cur_df.loc[:, ['xtra_card_nbr', signal]].dropna()

            # L1M_sig_num = L1M_sig_df.count()['xtra_card_nbr']
            # cur_sig_num = cur_sig_df.count()['xtra_card_nbr']
            L1M_sig_num = len(L1M_sig_df)
            cur_sig_num = len(cur_sig_df)

            print 'last one month EC num of not null signal: %d' %(L1M_sig_num)
            print 'current month EC num of not null signal : %d' %(cur_sig_num)

            L1M_range = L1M_sig_num / bin_num

            L1M_sig_df_sort = L1M_sig_df.sort_values(by=signal)
            L1M_sig_df_sort.index=range(L1M_sig_num)
            L1M_cutoffs, L1M_cutoffs_index = get_cutoffs(L1M_sig_df_sort, signal, L1M_range, L1M_sig_num)

            # dont need to sort
            # cur_sig_df_sort = cur_sig_df.sort_values(by=signal)
            L1M_dist = []
            cur_dist = []

            for k in range(len(L1M_cutoffs) + 1):
                condition = get_condition(L1M_cutoffs, signal, k)
                cur_decile_num = len(cur_sig_df.query(condition))
                # if k < bin_num - 1:
                #     L1M_dist.append(L1M_range)
                # else:
                #     L1M_dist.append(L1M_sig_num - L1M_range * (bin_num - 1))

                # use current cutoff index minus previous index to get EC number, caution the boundary!
                if k == 0:
                    L1M_dist.append(L1M_cutoffs_index[k] + 1)
                elif k == len(L1M_cutoffs):
                    L1M_dist.append(L1M_sig_num - L1M_cutoffs_index[k-1] - 1)
                else:
                    L1M_dist.append(L1M_cutoffs_index[k] - L1M_cutoffs_index[k-1])

                cur_dist.append(cur_decile_num)

            # append the number of null value
            L1M_dist.append(L1M_num - L1M_sig_num)
            cur_dist.append(cur_num - cur_sig_num)
            L1M_cutoffs.append(L1M_sig_df_sort.max()[signal])
            # just to keep same length
            L1M_cutoffs.append(np.nan)

            print 'last one month EC distribution:'
            print L1M_dist

            print 'current month EC distribution :'
            print cur_dist

            signal_output_df = output(signal, L1M_cutoffs, L1M_dist, cur_dist)
            output_df = output_df.append(signal_output_df)

            print 'the time of signal(%s): %s seconds' % (signal, (datetime.datetime.now() - time_start).seconds)

        output_file = 'bin_' + labels[i][:-3] + '.csv' # modify name from qc to bin
        output_df.to_csv(os.path.join(output_path, output_file), index=True, sep='|')

    end_time = datetime.datetime.now()
    print 'the time of whole process: %s seconds' % (end_time - start_time).seconds



if __name__ == '__main__':
    input_qc()
