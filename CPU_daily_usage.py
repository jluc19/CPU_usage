# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 09:42:25 2016

@author: jason.lucibello
"""
from __future__ import division
import pandas as pd
import os as os
import copy
import numpy
import re, time, csv
import pylab

from mpld3 import plugins

matplotlib.style.use('ggplot')


def create_dict(filename):
    reader = csv.reader(open(filename, 'r'))
    d = {}
    for v,k in reader:
        if k in d:
            d[k].append(v)
        else:
            d[k] = [v]
    return d
 
def find_ors(max_count, tg_dict, df):
    this_dict = copy.deepcopy(tg_dict)
    start_df = df
    #print tg_dict(start_df['swh_app'][i])
    #df1 = start_df[(start_df.Count*len(tg_dict(start_df.swh_app))< int(max_count))].sort(['swh_app'])
    df1 = start_df[(start_df['Count']< int(max_count))].sort(['swh_app'])    
    end_list = pd.DataFrame()
    task_count = 0
    ots = ''
    for index, row in df1.iterrows():
        if ots != row['swh_app']:
            task_count = row['Count']
            ots = row['swh_app']
        else:
            task_count = task_count + row['Count']
        tenant = row['tenant_n']
        if ots in this_dict:
            if tenant in this_dict[ots]:
                this_dict[ots].remove(tenant)
                if this_dict[ots] ==[]:
                    this_dict.pop(ots, None)
                    
                    raw_data = {'swh_app': [ots], 'tenants': [', '.join(tg_dict[ots])], 'tenant_number': [len(tg_dict[ots])], 'task_count': [task_count]}
                    df6 = pd.DataFrame(raw_data, columns = ['tenants', 'tenant_number', 'swh_app', 'task_count']) 
                    end_list = end_list.append(df6, ignore_index=True)
    return df1, end_list

'''
tasks_all = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/raw_tasks_prod.csv')
tasks_all = tasks_all[['tenant_id','swh_server','swh_app','date_time','read_or_update','Count']]
tasks_all = tasks_all[(tasks_all['read_or_update'] == 'X')]
tasks_all['date_time'] = pd.to_datetime(tasks_all['date_time'], format='%Y-%m-%d %H:%M:%S %p')
tasks_all['date_time'] = tasks_all['date_time'].dt.strftime('%Y-%m-%d')

#tasks_all.to_csv("/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160317_read_tasks.csv", encoding='utf-8', index=False)
#tasks_all = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160317_read_tasks.csv')

g = tasks_all.groupby(['tenant_id', 'date_time'])['Count'].sum().reset_index()
g.to_csv("/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160317_grouped_tasks.csv", encoding='utf-8', index=False)

task_by_tenant = g.loc[g.reset_index().groupby(['tenant_id'])['Count'].idxmax()]
task_by_tenant.to_csv("/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160317_task_by_tenant.csv", encoding='utf-8', index=False)
'''

task_by_tenant = pd.read_csv("/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160317_task_by_tenant.csv")

# create the tenant_mapping list
tenant_names = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/real_size_prod.csv')
tenant_names = tenant_names[['tenant_n']]
tenant_names = tenant_names.drop_duplicates(['tenant_n'])
tenant_names = tenant_names['tenant_n'].apply(lambda x: pd.Series(x.split('-')))
tenant_names.rename(columns={0:'tenant_n',1:'tenant_id'},inplace=True)

tenant_ors = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/tenant_ors.csv')
tenant_ots = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/map_tenant_ots.csv')
ots_host_name = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/tenant_configs_prod.csv')
ots_host_name = ots_host_name[['ots_host', 'ots_name']]
ots_host_name =ots_host_name.rename(columns = {'ots_name':'swh_app'})


#5. Add ORS Count
add_tenant_names = pd.merge(task_by_tenant, tenant_names, on='tenant_id')
add_number_ors = pd.merge(add_tenant_names, tenant_ors, on='tenant_n')

'''
multi_tenant_usage = pd.read_csv('/Users/jason.lucibello/Google Drive/OTS_ORS_Loads/Multi_Tenancy_usages_hosts.csv')
multi_tenant_usage = multi_tenant_usage[['date_time', 'tenant_n', 'read_or_update', 'server_role', 'ots', 'task_running_time']]
multi_tenant_usage['task_running_time'] = multi_tenant_usage['task_running_time'].convert_objects(convert_numeric=True)
multi_tenant_usage = multi_tenant_usage.ix[(multi_tenant_usage['read_or_update']=='X')| (multi_tenant_usage['server_role']=='ots') | (multi_tenant_usage['server_role']=='Report')]

grouped_usage = multi_tenant_usage.groupby(['ots', 'tenant_n', 'date_time'])['task_running_time'].sum().reset_index()
grouped_usage = grouped_usage.groupby(['ots', 'tenant_n'])['task_running_time'].max().reset_index()
grouped_usage =grouped_usage.rename(columns = {'tenant_n':'tenants'})
grouped_usage =grouped_usage.rename(columns = {'ots':'ots_host'})
grouped_usage.to_csv("/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/wd_all_usage.csv", encoding='utf-8', index=False)
grouped_usage = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/wd_all_usage.csv')
'''

add_ots = pd.merge(add_number_ors, tenant_ots, on='tenant_n')
num_ors = add_ots[['number_ors', 'swh_app']]

tg_dict = create_dict('/Users/jason.lucibello/Google Drive/OMS/TAM/2016_02_23_TAM_Input/map_tenant_ots.csv')
wd_stats = pd.read_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/WD_PROD_stats.csv')
wd_stats = wd_stats[['group', 'read_cpu_avail', 'write_cpu_avail', 'read_cpu_util', 'write_cpu_util']]
wd_stats['group'] = wd_stats['group'].apply(lambda x: pd.Series(x.split('..'))[1])

#gr_wd_stats = wd_stats.groupby(['group'])['read_cpu_util'].max().reset_index()
#gr_wd_stats =gr_wd_stats.rename(columns = {'group':'swh_app'})

#for i in [10000000, 8000000, 6000000, 4000000, 2000000, 1000000, 800000, 600000, 400000, 200000, 100000]:
for i in [10000000]: 
    add_ots, end_list = find_ors(i, tg_dict, add_ots)
    if not end_list.empty:
        end_list1 = pd.merge(end_list, num_ors, on='swh_app')
        end_list1 = end_list1.drop('tenant_number',1) 
        end_list1 = end_list1.drop('tenants',1)
        end_list1 = end_list1.drop_duplicates()

        grouped_usage = grouped_usage.groupby(['ots_host'])['task_running_time'].sum().reset_index()
        add_stats = pd.merge(ots_host_name, grouped_usage, on='ots_host')
        add_stats = add_stats.drop_duplicates()        
        
        add_stats = pd.merge(end_list1, add_stats, on='swh_app')
        add_stats = add_stats.drop_duplicates()
        add_stats.task_count = add_stats.task_count.astype(int)
        add_stats.number_ors = add_stats.number_ors.astype(int)
        add_stats = add_stats.drop_duplicates()

        fig, ax = plt.subplots()
        add_stats = add_stats.ix[(add_stats['task_running_time']<= 0.001*13824000000)]
        scatter = ax.scatter(add_stats.task_running_time/138240000/100, add_stats.task_count, s=100+3**add_stats.number_ors, c= add_stats.task_running_time/14400000*400+add_stats.task_count/1000, alpha=0.5, cmap=plt.cm.jet)
        ax.set_title('ORS Usage') 
        plt.ylim(0.0, plt.ylim()[1])
        plt.xlim(0.0, 0.001)
        ax.set_xlabel('Max Task Running Time / 15 min (%)')
        ax.set_ylabel('Max Daily Task Count')
        ''' 
        labels = add_stats.swh_app
        for label, x, y in zip(labels, add_stats.task_running_time/138240000/100, add_stats.task_count):
            plt.annotate(
                label, 
                xy = (x, y), xytext = (-20, 20),
                textcoords = 'offset points', ha = 'right', va = 'bottom',
                bbox = dict(boxstyle = 'round,pad=0.5', fc = 'yellow', alpha = 0.5),
                arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0'))        
        '''
        
        plt.show()
        add_stats.to_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160318_ors_removal_' + str(i) + '.csv', encoding='utf-8', index=False)
    add_ots.to_csv('/Users/jason.lucibello/Google Drive/OMS/TAM/Python Results/20160318_candidate_ors_' + str(i) + '.csv', encoding='utf-8', index=False)
    
    
    