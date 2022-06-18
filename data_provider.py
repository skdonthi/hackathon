import pandas as pd
import numpy as np

class SessionsData():
    def __init__(self,start_date,end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.df = None
        
    def load_session_data(self):
        dates_list = self.get_dates_list()
        dataframe_list = []
        for date in dates_list: 
            filepath = r'C:\Users\paul9\Documents\data_schickler\sessions\students_pageviews_'+date+'.parquet.gzip'
            df = pd.read_parquet(filepath, engine='pyarrow')
            dataframe_list.append(df)
        self.df = pd.concat(dataframe_list) 
        return

    def get_sessions_data(self):
        if self.df == None:
            self.load_session_data
        return self.df

    def get_dates_list(self):
        dates_list = pd.date_range(start=self.start_date, end=self.end_date).strftime("%Y-%m-%d").to_list()
        return dates_list 

class ArticleData():
    def __init__(self):
        self.df = None
        

    def load_article_data(self):
        df_1 = pd.read_parquet(r'C:\Users\paul9\Documents\data_schickler\articles\students_articles_enriched_1.parquet.gzip')
        df_2 = pd.read_parquet(r'C:\Users\paul9\Documents\data_schickler\articles\students_articles_enriched_2.parquet.gzip')
        df_3 = pd.read_parquet(r'C:\Users\paul9\Documents\data_schickler\articles\students_articles_enriched_3.parquet.gzip')
        df_4 = pd.read_parquet(r'C:\Users\paul9\Documents\data_schickler\articles\students_articles_enriched_4.parquet.gzip')
        self.df = pd.concat([df_1,df_2,df_3,df_4])
        return

class MergedData():
    def __init__(self,session_data,article_data):
        self.df = pd.merge(session_data.df,article_data.df,'left','article_drive_id')

    def enhance(self):
        df = self.df
        df['hour'] = df.page_view_start_local.dt.hour
        df['weekday'] = df.page_view_start_local.dt.weekday.apply(lambda x : x+1)
        df['month'] = df.page_view_start_local.dt.month
        df.loc[df.month.isin([3,4,5]) , 'season'] = 'Spring'
        df.loc[df.month.isin([6,7,8]), 'season'] = 'Summer'
        df.loc[df.month.isin([9,10,11]), 'season'] = 'Autumn'
        df.loc[df.month.isin([12,1,2]), 'season'] = 'Winter'
        df.loc[df.hour.isin([23,24,1,2,3,4]),'daytime'] = 'Night'
        df.loc[df.hour.isin([5,6,7,8,9,10]),'daytime'] = 'Morning'
        df.loc[df.hour.isin([11,12,13,14,15,16]),'daytime'] = 'Midday'
        df.loc[df.hour.isin([17,18,19,20,21,22]),'daytime'] = 'Evening'
        return 