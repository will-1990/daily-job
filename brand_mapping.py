# encoding=utf-8

import pandas as pd
import numpy as np
import re
import os
import Levenshtein


# input_file = '/home/xun.wei2/data/profiling.Cat_Offer_Optimization.pull_channel_campaign.pull_channel_campaign/pull_CEB_201606.csv'

input_file = '/home/xun.wei2/data/profiling.Cat_Offer_Optimization.pull_channel_campaign.pull_channel_campaign/pull_CEB_OAR.csv'

brand_file = '/home/xun.wei2/data/profiling.Cat_Offer_Optimization.pull_channel_campaign.pull_channel_campaign/brand.csv'

output_path = '/home/xun.wei2/data/profiling.Cat_Offer_Optimization.pull_channel_campaign.pull_channel_campaign'

limit_rows_num = 1000

distance_ratio = 0.2


def load_data(file, limit_rows):
    try:
        if limit_rows:
            reader = pd.read_csv(file, sep='|', nrows=limit_rows_num)
        else:
            reader = pd.read_csv(file, sep='|')

        print 'the data capacity:' + str(len(reader))

        return reader

    except IOError:
        print 'File not exist!'


def simple_match(str1, str2):
    return str1 == str2


def Levenshtein_Distance_match(str1, str2):
    max_len = max(len(str1), len(str2))
    distance = Levenshtein.distance(str1, str2)
    if max_len > 0:
        ratio = float(distance) / max_len
    else:
        return -1

    return ratio


def match_func_3():
    pass


def output(result, file):
    df = pd.DataFrame(result, columns=['CMPGN_DSC', 'dsc_1', 'dsc_2', 'brand_dsc'])
    df.to_csv(os.path.join(output_path, file), index=True, sep='|')


def count_match(CMPGN_DSC_list, brand_dsc_list):
    simple_count = 0
    LD_count = 0

    new_brand_list = []
    for brand in brand_dsc_list:
        new_brand = re.sub('[^a-zA-Z]', '', brand).upper()
        new_brand_list.append(new_brand)

    simple_hit_result = []
    LD_hit_result = []

    for raw_campgn_dsc in CMPGN_DSC_list:
        campgn_dsc_1 = raw_campgn_dsc.split(',')[0]
        campgn_dsc_2 = re.sub('[^a-zA-Z]', '', campgn_dsc_1).upper()
        exact_match_brand = ''
        possible_new_brand_set = ''
        possible_brand_set = ''

        # Todo: fix bug that use two for loop that generate duplicate record compare with using in operation
                # inexplicably solved...
        # Todo: fix bug that multiple possibile brands are repeated.
                # Solved, cause different brand dsc maybe have same clean type
        for new_brand_dsc, brand_dsc in zip(new_brand_list, brand_dsc_list):
            if simple_match(campgn_dsc_2, new_brand_dsc):
                simple_count += 1
                simple_hit_result.append([raw_campgn_dsc, campgn_dsc_2, new_brand_dsc, brand_dsc])
                # print '(%3d) raw dsc:%-60s  tag:%-30s  raw brand:%-30s' %(count, raw_dsc, dsc_2, brand_dsc_list[new_brand_list.index(dsc_2)])

            ratio = Levenshtein_Distance_match(campgn_dsc_2, new_brand_dsc)
            if ratio == -1:
                print campgn_dsc_2, new_brand_dsc

            elif ratio == 0:
                exact_match_brand = new_brand_dsc
                break    # have an exact match brand, then stop search

            elif ratio <= distance_ratio: # find the satisfied possible match brands, use ~ to concat them.
                # if campgn_dsc_2 == 'EXCEDRINCT':
                #     print raw_campgn_dsc, campgn_dsc_2, new_brand_dsc, brand_dsc
                possible_new_brand_set += new_brand_dsc + '~'
                possible_brand_set += brand_dsc + '~'

        if exact_match_brand != '':
            LD_count += 1
            LD_hit_result.append([raw_campgn_dsc, campgn_dsc_2, exact_match_brand, brand_dsc])

        elif possible_new_brand_set != '':
            LD_count += 1
            if len(possible_new_brand_set.split('~')) == 2: # only one possible match brand, add * in beginning to tag it.
                possible_new_brand_set = '*' + possible_new_brand_set[:-1]
                possible_brand_set = '*' + possible_brand_set[:-1]
                LD_hit_result.append([raw_campgn_dsc, campgn_dsc_2, possible_new_brand_set, possible_brand_set])
            else: # more than one possible match brand
                LD_hit_result.append([raw_campgn_dsc, campgn_dsc_2, possible_new_brand_set[:-1], possible_brand_set[:-1]])


    simple_check_file = 'simple_check.csv'
    output(simple_hit_result, simple_check_file)

    LD_check_file = 'LD_' + str(distance_ratio) + '_check.csv'
    output(LD_hit_result, LD_check_file)

    LD_not_simple_result = [a for a in LD_hit_result if a not in simple_hit_result]
    LD_not_simple_check_file = 'LD_' + str(distance_ratio) + '_not_simple_check.csv'
    output(LD_not_simple_result, LD_not_simple_check_file)

    print 'simple match number: %d' %len(simple_hit_result)
    print 'LD match number:     %d' %len(LD_hit_result)
    print 'LD match not simple: %d' %len(LD_not_simple_result)

    return simple_count, LD_count, np.array(LD_hit_result)[:, [0, 3]]


def tag_brand(campaign, brand_match):
    campaign_brand = pd.merge(campaign, brand_match, on='CMPGN_DSC', how='left')

    tag_brand_file = 'tag_CEB_OAR_brand.csv'

    campaign_brand.to_csv(os.path.join(output_path, tag_brand_file), index=True, sep='|')

    print campaign_brand.count()


def main():

    input_df = load_data(input_file, False)
    brand_df = load_data(brand_file, False)

    CMPGN_DSC_list = input_df['CMPGN_DSC'].dropna()
    brand_dsc_list = brand_df['brand_dsc'].dropna()

    CMPGN_DSC_CNT = CMPGN_DSC_list.count()
    brand_dsc_cnt = brand_dsc_list.count()
    print 'original CMPGN_DSC num:%d' %CMPGN_DSC_CNT
    print 'original brand_dsc num:%d' %brand_dsc_cnt

    CMPGN_DSC_dedup_list = CMPGN_DSC_list.drop_duplicates()
    CMPGN_DSC_dedup_CNT = CMPGN_DSC_dedup_list.count()
    print 'dedup CMPGN_DSC num:   %d' %CMPGN_DSC_dedup_CNT

    brand_dsc_dedup_list = brand_dsc_list.drop_duplicates()
    brand_dsc_dedup_cnt = brand_dsc_dedup_list.count()
    print 'dedup brand_dsc num:   %d' %brand_dsc_dedup_cnt

    simple_count, LD_count, LD_match = count_match(CMPGN_DSC_dedup_list, brand_dsc_dedup_list)

    print 'the accuracy of simple match is:%.4f' % (float(simple_count)/CMPGN_DSC_dedup_CNT)
    print 'the accuracy of LD match is:    %.4f' % (float(LD_count)/CMPGN_DSC_dedup_CNT)
    print 'the distance ratio is:%f' %(distance_ratio)

    campaign = input_df.loc[:, ['VEHICLE', 'CMPGN_ID', 'CMPGN_DSC']]
    match_brand = pd.DataFrame(LD_match, columns=['CMPGN_DSC', 'brand'])
    tag_brand(campaign, match_brand)



if __name__ == '__main__':
    main()