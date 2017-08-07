import csv
import numpy as np
import os.path
import pandas
NG_labels = pandas.read_csv('NG_labels.csv')

import cPickle as pickle
with open('labels.p', 'rb') as fp:
    labels = pickle.load(fp)
with open('to_plot.p', 'rb') as fp:
    to_plot = pickle.load(fp)   
with open('to_ml.p', 'rb') as fp:
    to_ml = pickle.load(fp)
for i, row in NG_labels.iterrows():
    for year in range(2001, 2017):
        file_name = 'cf/' + str(row['orispl_code']) + '_' + row['unitid'].replace('*', '') + '_' + str(year) + '.csv'
        if not os.path.isfile(file_name): continue
        with open(file_name, 'rb') as csvfile:
            dr = csv.DictReader(csvfile)
            to_insert = []
            for row in dr:
                to_insert.append(float( row['capacity_factor_hr'] if row['capacity_factor_hr'] is not '' else 0))
                if int(row['']) % 24 is 23:
                    to_plot.append(to_insert)
                    to_ml.append(to_insert + np.gradient(to_insert).tolist())
                    labels.append((row['op_date'], row['name'], row['unitid'], 'Natural Gas'))
                    to_insert = []
with open('labels.p', 'wb') as fp:
    pickle.dump(labels, fp)
with open('to_plot.p', 'wb') as fp:
    pickle.dump(to_plot, fp)
with open('to_ml.p', 'wb') as fp:
    pickle.dump(to_ml, fp)