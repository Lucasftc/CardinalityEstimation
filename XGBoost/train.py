from preprocess import *

def train_predict(train_sqlvec, test_sqlvec, labels):
    loglabels=[]
    for i in range(len(labels)):
        loglabels.append(math.log(labels[i]+1))
    dtrain = xgb.DMatrix(np.array(train_sqlvec), label=loglabels)
    param={'tree_method':'hist', 'grow_policy':'lossguide'}
    num_round=300
    bst = xgb.train(param, dtrain, num_round)
    dtest = xgb.DMatrix(test_sqlvec)
    logpred = bst.predict(dtest)
    pred=np.exp(logpred)-1
    with open(Config.result_path,mode='w') as f:
        f.write('Query ID'+','+'Predicted Cardinality'+'\n')
        for i in range(len(pred)):
            f.write(str(i) + ',' + str(int(pred[i])) + '\n')
    return pred
            
if __name__ == "__main__":
    
    column, col2i, orig, min_val, max_val = vector_init()
    join2vec,table2vec = join_table_encode()
    train_sqlvec, labels = vectorize(True, join2vec, table2vec, orig, column, max_val, min_val, col2i)
    test_sqlvec,_ = vectorize(False, join2vec, table2vec,orig, column, max_val, min_val, col2i)
    #print(test_sqlvec[100])
    pred=train_predict(train_sqlvec, test_sqlvec, labels)
