#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pymongo
import json
from bson import json_util,ObjectId
import time


# In[4]:


connection_url='mongodb+srv://lohawalla:Rohan123@hrms-v2.0icliah.mongodb.net/?retryWrites=true&w=majority'
client=pymongo.MongoClient(connection_url)
db=client['test']
collection=db['v2attendances']
cursor=collection.find()
data=list(cursor)
json_data = json.dumps(data, default=json_util.default)
df2=pd.DataFrame(data)
empId=[]
punchIn=[]    
punchOut=[]
while True:
    cursor = collection.find()
    data = list(cursor)
    json_data = json.dumps(data, default=json_util.default)
    df2 = pd.DataFrame(data)
    
    empId = []
    punchIn = []
    punchOut = []
    
    for i in df2['punches']:
        empId.append(i[0]['employeeId'])
        punchIn.append(i[0]['punchIn'])
        punchOut.append(i[0]['punchOut'])
    
    Final_df = pd.DataFrame(zip(empId, punchIn, punchOut), columns=['EmpId', 'PunchIn', 'PunchOut'])
    data_in_memory = Final_df
    data_in_memory.to_csv('output_data.csv', index=False)
    time.sleep(1800)



# In[7]:






# In[5]:


Final_df=pd.DataFrame(zip(empId,punchIn,punchOut),columns=['EmpId','PunchIn','PunchOut'])


# In[6]:


len(Final_df)


# In[10]:


len(df2)


# In[7]:


Final_df

