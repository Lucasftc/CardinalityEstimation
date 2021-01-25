import numpy as np
import pandas as pd
import json
import math
import xgboost as xgb
import re

class Config:
    minmax_path='E:\\BigData\\column_min_max_vals.csv'
    train_path='E:\\BigData\\train.csv'
    test_path = 'E:\\BigData\\test.csv'
    result_path = 'C:\\Users\\LQ\\source\\codes\\RNN.csv'
    train_planrow_path = 'E:\\BigData\\trainplan.csv'
    test_planrow_path = 'E:\\BigData\\testplan.csv'
    train_queryplan_dir='E:\\BigData\\training_plans'
    test_queryplan_dir = 'E:\\BigData\\testing_plans'

class Domain:
    valists=dict()
    valists['t.kind_id']=[1,2,3,4,6,7]
    valists['mc.company_type_id']=[1,2]
    valists['ci.role_id']=[1,2,3,4,5,6,7,8,9,10,11]
    valists['mi_idx.info_type_id'] = [99, 100, 101, 112, 113]
    excludelist=['t.kind_id','mc.company_type_id','ci.role_id','mi_idx.info_type_id']