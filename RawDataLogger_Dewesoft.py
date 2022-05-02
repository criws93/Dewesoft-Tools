#!/usr/bin/env python3  
# -*- coding: utf-8 -*- 
#----------------------------------------------------------------------------
# Created By  : Cristhian De la Hoz   
# Created Date: October 2020
# version ='1.0'
# ---------------------------------------------------------------------------
#- Signal Data logger:   This script retrieves the input data of all used (1 through 16) channels 
#-		from a Dewesoft instance and store them (including the health status)
#-		into a Mysql DB  
# ---------------------------------------------------------------------------
# Imports 
# ---------------------------------------------------------------------------

from win32com.client import Dispatch
from time import sleep
import numpy as np

#--MySQL DB Connection--# 
import mysql.connector
from mysql.connector import errorcode

db_host = input("Insert MySQL Database host:")
db_user = input("Insert Database user: ")
db_pwd = input("Insert password: ")
db_name = input("Insert DB schema: ")
db_table_name = input("Insert table name: ")

try:
    cnx = mysql.connector.connect(user=db_user,password=db_pwd,host=db_host,
                                database='dws_test') 
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

#--MySQL DB Cursor--# 
cursor = cnx.cursor()

#--MySQL DB Insert Query Template--# 
query_t="INSERT INTO" + db_table_name + "(AI1, AI2, AI3, AI4, AI5, AI6, AI7, AI8, AI9, AI10, AI11, AI12, AI13, AI14, AI15, AI16) VALUES %s;"
# create DCOM object
dw = Dispatch("Dewesoft.App")
#dw.Init()
AI = []
TS = []
loop = 0
pkg_sz = 10
channels = 16
values = ""

#------Update Channel List-------#
dw.Data.BuildChannelList()

#------Create IConnections List-------#
conn_list = [dw.Data.UsedChannels.Item(i).CreateConnection() for i in range(channels)]

stop = False
#--Setup connections
for con in conn_list:
    con.AType = 3
    con.BlockSize = 100
    
#---Start Acquisition---#        
dw.Start()

#---Wait until the first datablock is full---#
sleep(1.5)
    
#--Loop - Logging to DB --#
while not stop:
    print("Reading data... \n")
    query = query_t
    
    #--Fetching data from channels--#
    dw.Data.StartDataSync()
    
    for con in conn_list:
        AI.append(con.GetDataBlocks(1))
    
    dw.Data.EndDataSync()
        
    #--Storing in DB--#
    AI2 = np.array(AI)
    print("*AI size: %s \n" % str(np.shape(AI2.T))) 
    AI = []
    for row in AI2.T:
        if row[0] != 0:    
            str_row = ' , '.join([str(num) for num in row])
            line = "(%s),\n" % str_row 
            values+=line
        
    if values != '':
        values = values[:values.rindex(",")]
        query = query % values
        if cursor.execute(query): print("Data Saved. \n")
    
    values = ""
    sleep(0.5)

cnx.close()
dw.Stop()

    