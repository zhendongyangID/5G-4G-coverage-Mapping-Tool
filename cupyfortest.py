# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 14:24:17 2019

@author: sanshao
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 15:13:36 2019

@author: sanshao
"""
import random
import pandas as pd
import numpy as np
import time,os
import multiprocessing
import csv
import cupy as cp


def summaryResult(sorc,stacpart_5_percent,stacpart_50_percent,stacfactor_50_percent,stacfactor_5_percent):
    
    def descripe(df,stacpart,stacfactor_5_percent,col):
        lenth=len(df)
        df=df.sort_values(by=col , ascending=True)
        df= df.reset_index(drop=True)

        indexforused=int(stacpart*lenth)
        df=df.loc[indexforused]
        
        valuegot=df[col]
    
        valuegot=round(valuegot,2)
        return valuegot
        
    
    lenth=len(sorc)+1
    lenth1=lenth+1
    sorc.loc[lenth] = {'index':0, 'CLUSTERID':0, 'CLUSTERLONGITUDE_sorc':0, 'CLUSTERLATITUDE_sorc':0, 'NR_DL_RSRP_sorc':0, 'NR_UL_SPEED_sorc':0, 'NR_DL_SPEED_sorc':0, 'NR_DL_SPEED_UL_RESTRICTION_sorc':0, 'distance':0, 'targetClusterID':0, 'CLUSTERLONGITUDE_target':0, 'CLUSTERLATITUDE_target':0, 'NR_DL_RSRP_target':0, 'NR_UL_SPEED_target':0, 'NR_DL_SPEED_target':0, 'NR_DL_SPEED_UL_RESTRICTION_target':0, 'Type':"Overlap", 'MAX RSRP':0, 'MAX UL THP':0, 'SUM DL THP':0}
    sorc.loc[lenth1] = {'index':0, 'CLUSTERID':0, 'CLUSTERLONGITUDE_sorc':0, 'CLUSTERLATITUDE_sorc':0, 'NR_DL_RSRP_sorc':0, 'NR_UL_SPEED_sorc':0, 'NR_DL_SPEED_sorc':0, 'NR_DL_SPEED_UL_RESTRICTION_sorc':0, 'distance':0, 'targetClusterID':0, 'CLUSTERLONGITUDE_target':0, 'CLUSTERLATITUDE_target':0, 'NR_DL_RSRP_target':0, 'NR_UL_SPEED_target':0, 'NR_DL_SPEED_target':0, 'NR_DL_SPEED_UL_RESTRICTION_target':0, 'Type':"Non_Overlap", 'MAX RSRP':0, 'MAX UL THP':0, 'SUM DL THP':0}
    OverlapData=sorc.loc[sorc["Type"] == "Overlap"]
    NoverlapData=sorc.loc[sorc["Type"] == "Non_Overlap"]
    
    
    RSRP_5_percent_O=descripe(OverlapData,stacpart_5_percent,stacfactor_5_percent,"MAX RSRP")
    ULTHP_5_percent_O=descripe(OverlapData,stacpart_5_percent,stacfactor_5_percent,"MAX UL THP")
    DL_THP_5_percent_O=descripe(OverlapData,stacpart_5_percent,stacfactor_5_percent,"SUM DL THP")
    
    RSRP_50_percent_O=descripe(OverlapData,stacpart_50_percent,stacfactor_5_percent,"MAX RSRP")
    ULTHP_50_percent_O=descripe(OverlapData,stacpart_50_percent,stacfactor_5_percent,"MAX UL THP")
    DL_THP_50_percent_O=descripe(OverlapData,stacpart_50_percent,stacfactor_5_percent,"SUM DL THP")
    
    RSRP_5_percent_N=descripe(NoverlapData,stacpart_5_percent,stacfactor_50_percent,"MAX RSRP")
    ULTHP_5_percent_N=descripe(NoverlapData,stacpart_5_percent,stacfactor_50_percent,"MAX UL THP")
    DL_THP_5_percent_N=descripe(NoverlapData,stacpart_5_percent,stacfactor_50_percent,"SUM DL THP")
    
    RSRP_50_percent_N=descripe(NoverlapData,stacpart_50_percent,stacfactor_50_percent,"MAX RSRP")
    ULTHP_50_percent_N=descripe(NoverlapData,stacpart_50_percent,stacfactor_50_percent,"MAX UL THP")
    DL_THP_50_percent_N=descripe(NoverlapData,stacpart_50_percent,stacfactor_50_percent,"SUM DL THP")
    
    RSRP_edge=RSRP_5_percent_O*stacfactor_5_percent+RSRP_5_percent_N*(1-stacfactor_5_percent)
    ULTHP_edge=ULTHP_5_percent_O*stacfactor_5_percent+ULTHP_5_percent_N*(1-stacfactor_5_percent)
    DL_THP_edge=DL_THP_5_percent_O*stacfactor_5_percent+DL_THP_5_percent_N*(1-stacfactor_5_percent)
    
    RSRP_avg=RSRP_50_percent_O*stacfactor_50_percent+RSRP_50_percent_N*(1-stacfactor_50_percent)
    ULTHP_avg=ULTHP_50_percent_O*stacfactor_50_percent+ULTHP_50_percent_N*(1-stacfactor_50_percent)
    DL_THP_avg=DL_THP_50_percent_O*stacfactor_50_percent+DL_THP_50_percent_N*(1-stacfactor_50_percent)
    #result=[RSRP_edge,RSRP_avg,ULTHP_edge,ULTHP_avg,DL_THP_edge,DL_THP_avg,]



    #x=summaryResult(df,stacpart_5_percent,stacpart_50_percent,stacfactor_50_percent,stacfactor_5_percent)
    row1=["Edge5%","Overlap","Non-overlap","Final"]
    row2=["RSRP",str(RSRP_5_percent_O),str(RSRP_5_percent_N),str(RSRP_edge)]
    row3=["UL THP",str(ULTHP_5_percent_O),str(ULTHP_5_percent_N),str(ULTHP_edge)]
    row4=["DL THp",str(DL_THP_5_percent_O),str(DL_THP_5_percent_N),str(DL_THP_edge)]
    row5="\n"
    row6=["Average50%","Overlap","Non-overlap","Final"]
    row7=["RSRP",str(RSRP_50_percent_O),str(RSRP_50_percent_N),str(RSRP_avg)]
    row8=["UL THP",str(ULTHP_50_percent_O),str(ULTHP_50_percent_N),str(ULTHP_avg)]
    row9=["DL THp",str(DL_THP_50_percent_O),str(DL_THP_50_percent_N),str(DL_THP_avg)]
    
    rowlist=[row1,row2,row3,row4,row5,row6,row7,row8,row9]
    for i in rowlist:
        for j in i:
            if j=="0.0":
                i[3]=" "
    return rowlist
    
    
    


#@jit
def Handle(sorc,target,overlapThr,ULTHPfactor,temID):
    dislist=[]
    targetcIDlist=[]
    #x=sorc.apply(lambda x:PDcalculate(x["CLUSTERLONGITUDE"],x["CLUSTERLATITUDE"],target),axis=1)  
    lenthes=len(sorc)
    for th in range(lenthes):
        
        xx=sorc.loc[th]["CLUSTERLONGITUDE"]
        y=sorc.loc[th]["CLUSTERLATITUDE"]
        diss=PDcalculate(xx,y,target)
        #print(diss)
        dislist.append(diss[0])
        targetcIDlist.append(diss[1])
    
    dis=pd.DataFrame(dislist)
    sorc["distance"]=dis
    sorc["targetClusterID"]=targetcIDlist
    target.rename(columns={"CLUSTERID":"targetClusterID"},inplace=True)
    outdata=pd.merge(sorc,target,how="left",on="targetClusterID",suffixes=("_sorc","_target"))
    
    outdata["Type"]=outdata["distance"].apply(lambda y: "Overlap" if y<overlapThr else "Non_Overlap")
    outdata["MAX RSRP"]=outdata.apply(lambda y:np.max([y["NR_DL_RSRP_sorc"],y["NR_DL_RSRP_target"]]) if y["Type"]=="Overlap" else y["NR_DL_RSRP_sorc"],axis=1)
    
    outdata["MAX UL THP"]=outdata.apply(lambda y:np.max([y["NR_UL_SPEED_sorc"],y["NR_UL_SPEED_target"]]) if y["Type"]=="Overlap" else y["NR_UL_SPEED_sorc"],axis=1)
    outdata["SUM DL THP"]=outdata.apply(lambda y:np.min([y["NR_DL_SPEED_sorc"]+y["NR_DL_SPEED_target"],y["MAX UL THP"]*ULTHPfactor]) if y["Type"]=="Overlap" else y["NR_DL_SPEED_UL_RESTRICTION_sorc"],axis=1)
    outdata.index.set_names(["index"])
    outdata.to_csv(temID+"temOutput.csv",index=False)
    
def geodistance(lng1,lat1,lng2,lat2):
        lng1, lat1, lng2, lat2 = map(np.radians, [lng1, lat1, lng2, lat2])
        dlon=lng2-lng1
        dlat=lat2-lat1
        a=np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        dis=2*np.arcsin(np.sqrt(a))*6371*1000  
        return dis
def geodistance_cp(lng1,lat1,lng2,lat2):
        lng1, lat1, lng2, lat2 = map(cp.radians, [lng1, lat1, lng2, lat2])
        dlon=lng2-lng1
        dlat=lat2-lat1
        a=cp.sin(dlat/2)**2 + cp.cos(lat1) * cp.cos(lat2) * cp.sin(dlon/2)**2
        dis=2*cp.arcsin(cp.sqrt(a))*6371*1000  
        return dis    

def PDcalculate(x,y,data2):
    lon1=x
    lat1=y
    lon1=cp.asarray(lon1)
    lat1=cp.asarray(lat1)
    lon2=data2["CLUSTERLONGITUDE"]
    lat2=data2["CLUSTERLATITUDE"]
    lon3=lon2.values
    lat3=lat2.values
    lon4 = cp.asarray(lon3)
    lat4=cp.asarray(lat3)
    shortdistance=geodistance_cp(lon1,lat1,lon4,lat4)
   
    indexmin=cp.argmin(shortdistance)
    indexmin=cp.int(indexmin)
    targetcID=data2.at[indexmin,"CLUSTERID"]
    mindistance=cp.int(cp.min(shortdistance))
    return mindistance,targetcID

def MergeResult():
    temOutfilename="temOutput.csv"
    outdata=pd.DataFrame()
    path=os.getcwd()
    files=os.listdir(path)
    for file in files:
        if temOutfilename in file:
            dataset = pd.read_csv(file)
            outdata=pd.concat([outdata,dataset], axis=0,join='outer',ignore_index=True)
            os.remove(file)
    outdata.index.name="index"
    return outdata
    
if __name__ == '__main__':
    multiprocessing.freeze_support()
    with open("config.csv","r") as f:
        paralist=[]
        conf=csv.reader(f)
        for para in conf:
            paralist.append(para[1])
    stacpart_5_percent=float(paralist[1])
    stacpart_50_percent=float(paralist[2])
    stacfactor_5_percent=float(paralist[3])
    stacfactor_50_percent=float(paralist[4])
    overlapThr=float(paralist[5])
    ULTHPfactor=float(paralist[6])
    splitNum=int(paralist[7])
    splitNum=10
    print("已完成参数导入")
    sorcefile=r"C:\Users\sanshao\Desktop\EricssonTool\BinMerage\CUWFSA_Dense_2.1 100%.csv"
    targetfile=r"C:\Users\sanshao\Desktop\EricssonTool\BinMerage\CUWFSA_Dense_3.5 100%.csv"
    outputpath=r"C:\Users\sanshao\Desktop\EricssonTool\BinMerage\output"
    t0=time.perf_counter()
    print("开始处理")
    sorc = pd.read_csv(sorcefile)
    target = pd.read_csv(targetfile)
    
    sorc=sorc.loc[sorc["EARFCN"] == "1650"]
    sorc= sorc.reset_index(drop=True)
    sorc=sorc[["CLUSTERID","CLUSTERLONGITUDE","CLUSTERLATITUDE","NR_DL_RSRP","NR_UL_SPEED","NR_DL_SPEED","NR_DL_SPEED_UL_RESTRICTION"]]
    
    target=target.loc[target["EARFCN"] == "1650"]
    target=target[["CLUSTERID","CLUSTERLONGITUDE","CLUSTERLATITUDE","NR_DL_RSRP","NR_UL_SPEED","NR_DL_SPEED","NR_DL_SPEED_UL_RESTRICTION"]]
    target= target.reset_index(drop=True)
    x=len(sorc)
    n=int(x/splitNum)
    startNum=0
    endNum=0
    
    for i in range(splitNum):
        t1=time.perf_counter()
        targetC=target.copy(deep=True)
        newdir=random.randint(70000,99999)
        
        newdir=str(newdir)
        temID=newdir
        endNum=endNum+n
        sub_sorc=sorc.iloc[startNum:endNum]
        sub_sorc= sub_sorc.reset_index(drop=True)
        startNum=startNum+n
        percent=i/splitNum*100+1
        Handle(sub_sorc,targetC,overlapThr,ULTHPfactor,temID)
        t2=time.perf_counter()
        td=(t2-t1)*(splitNum-i)+10
        print("距离计算进度已完成%.1f%%,预计剩余处理时间为%d 秒" % (percent,td))
        
        
#==============================================================================================        
    newdir=random.randint(70000,99999)
    newdir=str(newdir)
    temID=newdir
    targetC=target.copy(deep=True)
    sub_sorc=sorc.iloc[endNum:x]
    sub_sorc= sub_sorc.reset_index(drop=True)
    if len(sub_sorc)>1:
        Handle(sub_sorc,targetC,overlapThr,ULTHPfactor,temID)
#==============================================================================================    
    print("开始进行数据合并")
    outdata=MergeResult()
    
    outdata.to_csv(outputpath+"\\"+"output.csv",index=True)
#============================================================================================== 
    print("完成数据合并")
    df = pd.read_csv(outputpath+"\\"+"output.csv")
    print("output文件已输出")
    print("开始进行mapping  result输出")
    rowlist=summaryResult(df,stacpart_5_percent,stacpart_50_percent,stacfactor_50_percent,stacfactor_5_percent)
    
    with open(outputpath+"\\"+"Mapping Result.csv","w") as f:
        for rows in rowlist:
            lines=",".join(rows)
            lines=lines+"\n"
            f.writelines(lines)
    t3=time.perf_counter()
    print("Mapping Result 文件完成输出")
    print("done!")
    print("耗时共计 %.3f s" % (t3-t0))
    time.sleep(20)



