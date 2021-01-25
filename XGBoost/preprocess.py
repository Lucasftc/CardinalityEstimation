from config import *
from func import *
def vector_init():
    min_max=pd.read_csv(Config.minmax_path)
    min_val=list(min_max['min'])
    max_val=list(min_max['max'])
    column=list(min_max['name'])
    col2i=dict()
    j=0
    for i in range(len(column)):
        if column[i] not in Domain.excludelist:
            col2i[column[i]]=j*2
            j+=1
    j*=2
    col2i['t.kind_id']=j
    j+=6
    col2i['mc.company_type_id']=j
    j+=2
    col2i['ci.role_id']=j
    j+=11
    col2i['mi_idx.info_type_id']=j
    j+=5
    orig=[0]*56
    for i in range(len(column)):
        if column[i] not in Domain.excludelist:
            orig[col2i[column[i]]]=min_val[i]
            orig[col2i[column[i]]+1]=max_val[i]
    for j in range(len(orig)):
        if orig[j]==0:
            orig[j]=1
    return column, col2i, orig, min_val, max_val

def join_table_encode():
    train_data=pd.read_csv(Config.train_path,delimiter='#',names=['tables','joins','conditions','labels'])
    joins = list(train_data['joins'])
    tables= list(train_data['tables'])
    joinset = set()
    tableset = set()
    for i in range(len(tables)):
        tables[i]=tables[i].split(',')
        if type(joins[i])==float:
            joins[i] = ''

    for _ in joins:
        if _ != '':
            joinset.add(_)
    join2vec, _ = set_encode(joinset)
    for onesql_table in tables:
        for table in onesql_table:
            tableset.add(table)
    table2vec,_=set_encode(tableset)
    return join2vec,table2vec

def extract_planrow():
    pattern=r'rows=(\d*)'
    planrows=[]
    for i in range(100000):
        with open(Config.train_queryplan_dir+str(i)+'.txt',mode='r') as f:
            line=f.readline()
        match=re.search(pattern,line)
        if match:
            planrows.append(int(match.group(1)))
        else:
            planrows.append(-1)
    
    with open(Config.train_planrow_path,mode='w')as f:
        f.write('queryid'+','+'planrows'+'\n')
        for i in range(len(planrows)):
            f.write(str(i)+','+str(planrows[i])+'\n')

    for i in range(5000):
        with open(Config.test_queryplan_dir+str(i)+'.txt',mode='r') as f:
            line=f.readline()
        match=re.search(pattern,line)
        if match:
            planrows.append(int(match.group(1)))
        else:
            planrows.append(-1)

    with open(Config.test_planrow_path,mode='w')as f:
        f.write('queryid'+','+'planrows'+'\n')
        for i in range(len(planrows)):
            f.write(str(i)+','+str(planrows[i])+'\n')
    

def vectorize(is_train,join2vec,table2vec,orig,column,max_val,min_val,col2i):
    if is_train:
        sql_path = Config.train_path
        planrow_path = Config.train_planrow_path
    else:
        sql_path = Config.test_path
        planrow_path = Config.test_planrow_path

    data=pd.read_csv(sql_path,delimiter='#',names=['tables','joins','conditions','labels'])
    joins=list(data['joins'])
    conditions = list(data['conditions'])
    tables = list(data['tables'])
    planrows = list(pd.read_csv(planrow_path)['planrows'])
    if is_train:
        labels = list(data['labels'])
    else:
        labels=[]
    for i in range(len(data)):
        tables[i]=tables[i].split(',')
        if type(conditions[i])==float:
            conditions[i]=''
        if type(joins[i])==float:
            joins[i]=''
    for i in range(len(conditions)):
        conditions[i]=conditions[i].split(',')
    conditions = [list(chunk(_, 3)) for _ in conditions]
        
    
    join_encodes=[]
    for _ in joins:
        if _=='':
            join_encodes.append([0]*(len(join2vec.keys())))
        else:
            join_encodes.append(join2vec[_])

    table_encodes = []
    for i in range(len(tables)):
        table_encodes.append([0]*6)
        onesql_table=tables[i]
        for table in onesql_table:
            table_encodes[i]=list(np.array(table_encodes[i])+np.array(table2vec[table]))
    
    condition_encodes=[]
    for i in range(len(conditions)):
        temp=orig.copy()
        onesql_condition=conditions[i]
        if onesql_condition==[['']]:
            condition_encodes.append(temp)
            continue
        for condition in onesql_condition:
            op=condition[1]
            val=int(condition[2])
            if condition[0] not in Domain.excludelist:
                minval_index=col2i[condition[0]]
                maxval_index=minval_index+1
                if op =='>':
                    temp[minval_index]=val
                if op=='<':
                    temp[maxval_index]=val
                if op=='=':
                    temp[minval_index]=val
                    temp[maxval_index]=val
            elif condition[0] in Domain.excludelist:
                start_index=col2i[condition[0]]
                valist=Domain.valists[condition[0]]
                if op=='>':
                    for j in range(len(valist)):
                        if valist[j]<=val:
                            temp[start_index+j]=0
                if op=='<':
                    for j in range(len(valist)):
                        if valist[j]>=val:
                            temp[start_index+j]=0
                if op=='=':
                    for j in range(len(valist)):
                        if valist[j]!=val:
                            temp[start_index+j]=0
        condition_encodes.append(temp)
    norm_conditions=[]
    for i in range(len(condition_encodes)):
        temp=condition_encodes[i].copy()
        for j in range(len(column)):
            if column[j] not in Domain.excludelist:
                temp[col2i[column[j]]]=(temp[col2i[column[j]]]-min_val[j])/(max_val[j]-min_val[j])
                temp[col2i[column[j]]+1]=(temp[col2i[column[j]]+1]-min_val[j])/(max_val[j]-min_val[j])
        norm_conditions.append(temp)
    sqlvec=[]
    for i in range(len(data)):
        sqlvec.append(table_encodes[i]+norm_conditions[i]+join_encodes[i]+[math.log(1+planrows[i])])
    return sqlvec,labels
    

    