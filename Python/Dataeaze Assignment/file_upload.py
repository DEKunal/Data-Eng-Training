import pandas as pd
import mysql.connector as sql_db

conn=sql_db.connect(
    user='training',
    password='Kun@l123',
    host='localhost',
    database='training'
)


cur=conn.cursor()

df=pd.read_csv('/home/user/Downloads/startup.csv',keep_default_na=False)

csv_column_list=df.columns.tolist()

data_list=[];

for data in df.iterrows():

    # print(data[1])

    # x=tuple(data[1])
    # print(x)


    try:
        data[1]['Amount_in_USD']=float(data[1]['Amount_in_USD'].replace(',',''))

    except:
        
        data[1]['Amount_in_USD']=float(0)
    
    
    x=tuple(data[1])

    
    insert_query=f'INSERT INTO training.startup ({",".join(csv_column_list)}) Values{x};'

    print('Insertion Started')

    print(insert_query)

    cur.execute(insert_query)
    
    conn.commit()

    #print('Inserion Completed')
