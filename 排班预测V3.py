# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 01:02:26 2019

@author: Administrator
"""

import pandas as pd
from fbprophet import Prophet
from time import strftime
#import datetime 
from datetime import datetime, timedelta
#from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from math import log,exp

#当月月末
def this_month_end(date):
    return datetime(date.year, date.month, 1, 0, 0, 0) \
        +relativedelta(months=+1)-timedelta(days=1)

def adjust_pre_holiday(holiday,year1,year0,name,seq):
    if holiday[(holiday.年==year0)&(holiday.节日==name)&(holiday.序号==seq)].empty:
        year0=year0-1
    adj=holiday[(holiday.年==year1)&(holiday.节日==name)&(holiday.序号==seq)]['星期'].values[0] \
        -holiday[(holiday.年==year0)&(holiday.节日==name)&(holiday.序号==seq)]['星期'].values[0]
    if adj<-3:
        adj=adj+7
    elif adj>3:
        adj=adj-7
    return (holiday.loc[(holiday.年==year1)&(holiday.节日==name)&(holiday.序号==seq),'日期'] \
        -pd.to_timedelta(adj,unit='d')).values[0]


#单次预测天数
predict_day=70
predict_month=2
#预测月数
n=19
#

rawdata = pd.read_excel('历史话量.xlsx')

rawdata.set_index('ds',drop=False,inplace=True)

para = pd.read_excel('调假设置.xlsx',sheetname=None)



holiday=pd.DataFrame()
spclday=pd.DataFrame()
kmh=pd.DataFrame()

fb_rawdata0=rawdata.copy()


for key in para.keys():
    if key not in ['特殊工作日','特殊周末','开门红']:
        ##从历史来电数据中去除节假日
        cols=[x for i,x in enumerate(rawdata.index) if rawdata.iat[i,0] in para[key].iloc[:,0].tolist()]
        fb_rawdata0=fb_rawdata0.drop(cols)

        
        ##从特殊周末工作日中删除已在节假日中设定过的日期
        for a in ['特殊工作日','特殊周末']:
            for t in para[a].columns:
                cols=[x for i,x in enumerate(para[a][t].index) if para[a][t].iat[i] in para[key].iloc[:,0].tolist()]
                para[a][t].drop(cols,inplace=True)

        #假期
        para[key].rename(columns={para[key].columns[1]:'序号'},inplace=True)
        tmp=para[key].iloc[:,:2]
        tmp['节日']=key
        holiday=holiday.append(tmp)
    elif key in ['特殊工作日','特殊周末']:
        cols=[]
        for t in para[key].columns:
            tmp=[x for i,x in enumerate(rawdata.index) if rawdata.iat[i,0] in para[key][t].tolist()]
            cols=list(set(cols+tmp))
            tmp=para[key][t].dropna()
            tmp=tmp.rename('日期')
            tmp=tmp.reset_index(drop=True)
            spclday=spclday.append(pd.DataFrame({'日期':tmp,'类别':pd.Series([t]*tmp.count())}))
        fb_rawdata0=fb_rawdata0.drop(cols)
        
    elif key=='开门红':

        para[key].rename(columns={para[key].columns[1]:'序号'},inplace=True)
        tmp=para[key].iloc[:,:3]
        tmp['节日']=key
        kmh=kmh.append(tmp)

holiday['年']=holiday['日期'].apply(lambda x:(x+relativedelta(days=+10)).year)
holiday['星期']=holiday['日期'].apply(lambda x:x.dayofweek)
#设置节日顺序
order=['元旦','春节','清明','五一','端午','中秋','国庆']
holiday['节日']=holiday['节日'].astype('category')
holiday['节日'].cat.set_categories(order, inplace=True)
holiday=holiday.sort_values(axis=0,by=['年','节日'],ascending=False).reset_index(drop=True)
holiday['话量']=pd.merge(holiday,rawdata,how='left',left_on='日期',right_index=True)['y']

spclday['话量']=pd.merge(spclday,rawdata,how='left',left_on='日期',right_index=True)['y']
spclday=spclday.reset_index(drop=True)



last_date=rawdata['ds'][rawdata.shape[0]-1] #取数据最后一天
month_end=this_month_end(last_date)
holiday_result=pd.DataFrame()

if last_date!=month_end:
    month_end=month_end-relativedelta(months=+1)
    month_end=this_month_end(month_end) 

for i in range(n):

    fb_rawdata=fb_rawdata0.copy()
    holiday_spcl=pd.DataFrame(columns=['holiday','ds','lower_window','upper_window'])
        
    predict_end=this_month_end(month_end+pd.to_timedelta(predict_month,unit='M'))
    if predict_end in holiday['日期'].to_list():
        ix=holiday[holiday.日期==predict_end].index
        predict_end=holiday[(holiday.年==holiday.loc[ix,'年'].values[0])&(holiday.节日==holiday.loc[ix,'节日'].values[0])]['日期'].max()
    
    holiday_tmp=holiday[holiday['日期']<=predict_end]
    holiday_tmp=holiday_tmp[['年','节日']].drop_duplicates().sort_values(axis=0,by=['年','节日'],ascending=False)
    holiday_tmp=holiday_tmp.reset_index(drop=True)[:7]
    
    holiday_adjust=holiday[holiday['日期']<=predict_end]
    holiday_adjust=holiday_adjust[holiday_adjust.序号!=0]
    
    spclday_adjust=spclday[spclday['日期']<=predict_end]
    
    for [y0,h] in holiday_tmp.values.tolist():
        tmp=holiday_adjust[holiday_adjust.节日==h].年.drop_duplicates().values
        for yi in tmp:
            if yi!= y0:
                 
                t02=pd.to_datetime(holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==2),'日期'].values[0])
                holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==2),'日期']=adjust_pre_holiday(holiday_adjust,yi,y0,h,2)

        
                for j in holiday[(holiday.年==y0)&(holiday.节日==h)]['序号']:
                    if (j>-10)&(j<20):
                        #以序号2为基准，对往年节日日期进行调整
                        if j in holiday_adjust[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)]['序号'].tolist():
                            if j==2:
                                t0=t02
                                
                            else:                            
                                adj=pd.to_datetime(holiday_adjust.loc[(holiday_adjust.年==y0)&(holiday_adjust.节日==h)&(holiday_adjust.序号==j),'日期'].values[0]) \
                                    -pd.to_datetime(holiday_adjust[(holiday_adjust.年==y0)&(holiday_adjust.节日==h)&(holiday_adjust.序号==2)]['日期'].values[0])
                                    
                                t0=pd.to_datetime(holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==j),'日期'].values[0])
                                                                      
                                holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==j),'日期']= \
                                    holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==2),'日期'].values[0]+adj
                                
                            t1=pd.to_datetime(holiday_adjust.loc[(holiday_adjust.年==yi)&(holiday_adjust.节日==h)&(holiday_adjust.序号==j),'日期'].values[0])
                            #同步调整开门红
                            if h=='元旦':
                                kmh=kmh.drop(kmh[kmh.日期==t1].index)
                                kmh.loc[kmh.日期==t0,'日期']= t1
                                        
                            #如有特殊工作日休息日，同步调整
                            for a in ['特殊工作日','特殊周末']:
                                for b in para[a].columns:  
                                    if t1 in para[a][b].tolist():
                                        if j>0:
                                            t= t1+pd.to_timedelta(7,unit='d')
                                            
                                        else:
                                            t= t1-pd.to_timedelta(7,unit='d')
                                        
                                        spclday_adjust.loc[(spclday.日期==t1)&(spclday.类别==b),'日期'] =t

                                    
    holiday_adjust['星期']=holiday_adjust['日期'].apply(lambda x:x.dayofweek)
    
    #调整话量数据日期
    
    cols=[x for i,x in enumerate(fb_rawdata.index) if fb_rawdata.iat[i,0] in holiday_adjust['日期'].append(spclday_adjust['日期']).tolist()]
    fb_rawdata=fb_rawdata.drop(cols)
    

    #将调整后的节日话量加入源数据，序号小于-10的后续单独处理
    fb_rawdata=fb_rawdata.append(holiday_adjust[(holiday_adjust.序号>=-10)&(holiday_adjust.序号<20)][['日期','话量']].rename(columns={'日期':'ds','话量':'y'}).set_index('ds',drop=False))
    fb_rawdata=fb_rawdata.append(spclday_adjust[['日期','话量']].rename(columns={'日期':'ds','话量':'y'}).set_index('ds',drop=False))

    fb_rawdata=fb_rawdata.dropna()
    fb_rawdata['y']=fb_rawdata.y.apply(log)
    
    dftmp=fb_rawdata[fb_rawdata.ds<=month_end]
    
    dftmp['ds']=dftmp.ds.apply(lambda x :  x.strftime('%Y/%m/%d'))
    
    #设置节假日


    for t in spclday_adjust['类别'].unique():
        holiday_spcl =holiday_spcl.append(
            pd.DataFrame({
            'holiday':t,
            'ds': spclday_adjust[spclday_adjust.类别==t]['日期'].apply(lambda x:x.strftime('%Y/%m/%d')),
            'lower_window': 0,
            'upper_window': 0,        
            })
        )
    
    for t in kmh['序号'].unique():

        holiday_spcl =holiday_spcl.append(
            pd.DataFrame({
            'holiday':'开门红_'+str(t),
            'ds': kmh[kmh.序号==t]['日期'].apply(lambda x:x.strftime('%Y/%m/%d')),
            'lower_window': 0,
            'upper_window': 0,        
            })
        )

#            )
    for h in holiday_tmp['节日'].values.tolist():
        for seq in holiday_adjust[holiday_adjust.节日==h]['序号'].unique().tolist():
            if (seq>-10)&(seq<20):
                holiday_spcl =holiday_spcl.append(
                    pd.DataFrame({
                    'holiday':h+'_'+str(seq),
                    'ds': holiday_adjust[(holiday_adjust.节日==h)&(holiday_adjust.序号==seq)]['日期'].apply(lambda x:x.strftime('%Y/%m/%d')),
                    'lower_window': 0,
                    'upper_window': 0,
                    })
                )
#    for t in holiday[holiday['序号']>30]['日期'].values:
#        holiday_spcl =holiday_spcl.append(
#                        pd.DataFrame({
#                        'holiday':'工作日_'+str(holiday[holiday['日期']==t]['序号'].values[0]),
#                        'ds': holiday[holiday['日期']==t]['日期'].apply(lambda x:x.strftime('%Y/%m/%d')),
#                        'lower_window': 0,
#                        'upper_window': 0,
#                        })
#                    )
    

    
    m = Prophet(holidays=holiday_spcl,
                yearly_seasonality=13, 
                changepoint_range =0.3,
                changepoint_prior_scale=0.015,
                weekly_seasonality=False,
                holidays_prior_scale=26,
                #seasonality_prior_scale=100,
                seasonality_mode='additive')
    m.add_seasonality(name='quartly', period=91.3125, fourier_order=1, prior_scale=4.9)
    m.add_seasonality(name='monthly', period=30.4375, fourier_order=3, prior_scale=4)
    m.add_seasonality(name='weekly', period=7, fourier_order=7, prior_scale=4)
    dftmp=dftmp.reset_index(drop=True)
    f=m.fit(dftmp)
 #   future = m.make_future_dataframe(freq='H',periods=1488)
    future = m.make_future_dataframe(periods=predict_day)
    forecast = f.predict(future)
    forecast_begin=month_end+relativedelta(months=+1)
    forecast_begin=this_month_end(forecast_begin) 
    forecast_end=month_end+relativedelta(months=+predict_month)
    forecast_end=this_month_end(forecast_end) 
    if i==0:
        result=forecast[(forecast.ds<=forecast_end) & (forecast.ds>forecast_begin)]
    else:
        result=result.append(forecast[(forecast.ds<=forecast_end) & (forecast.ds>forecast_begin)])

    
    forecast.set_index('ds',drop=False,inplace=True)

    tmp=holiday[((holiday.序号<-10)|(holiday.序号>20) )&(holiday.日期>forecast_begin)& (holiday.日期<=forecast_end)]
    for j in tmp.index:
        w=tmp.loc[j,'日期'].dayofweek
        t=tmp.loc[j,'序号']
        if t==-11: #非周五变休息日前一天
            w=4-w
        elif t==-12: #非周四变休息日前二天
            w=3-w
        elif t==21: #非周一变休息日后一天
            w=0-w
        elif t==22:#非周二变休息日后二天
            w=1-w
        elif t==23:#非周三变休息日后三天
            w=2-w
        elif t==24:
            w=3-w
#        elif t==25:
#            w=4-w
        else:
            continue
        t1=forecast[forecast.ds==(tmp.loc[j,'日期']+pd.to_timedelta(w,unit='d'))]['weekly'].values[0]
        t0=forecast[forecast.ds==tmp.loc[j,'日期']]['weekly'].values[0]
        result.loc[result.ds==tmp.loc[j,'日期'],'yhat']=result.loc[result.ds==tmp.loc[j,'日期'],'yhat']-t0+t1
    
    
    

    #序号1.5的节日取前后一天的均值
    tmp=holiday[(holiday.序号==1.5)&(holiday.日期>forecast_begin)& (holiday.日期<=forecast_end)]['日期']
    cols=[i  for i,x in enumerate(result.index) if result.iat[i,0] in tmp.tolist()]
    t=result.columns.get_loc('yhat')
    for j in cols:
        result.iloc[j,t]=(result.iloc[j-1,t]+result.iloc[j+1,t])*0.5

    
    month_end=month_end-relativedelta(months=+1)
    month_end=this_month_end(month_end) 



#排序后输出结果

result['yhat']=result.yhat.apply(exp)


result=result.sort_index(axis=0,by='ds',ascending=True)
result[['ds','yhat']].to_excel('预测结果.xlsx')

forecast.to_excel('forecast.xlsx')

