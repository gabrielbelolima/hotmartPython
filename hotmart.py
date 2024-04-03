# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 10:06:31 2024

@author: Gabriel Belo
"""
import requests
import pandas as pd
from tqdm.notebook import tqdm
from time import sleep
from datetime import datetime as dt, timedelta

class HotMart:

    def __init__(self, login_params):
        self.token = self.auth_hm(login_params)

# - Autenticação
    def auth_hm(self, params=None):
        params = {
            'headers': {
                'Content-Type': 'application/json',
                'Authorization': params['Authorization']
            },
            'client_id': params['client_id'],
            'client_secret': params['client_secret']
        }

        url_auth = f"https://api-sec-vlc.hotmart.com/security/oauth/token?grant_type=client_credentials&client_id={params['client_id']}&client_secret={params['client_secret']}"


        with requests.post(url_auth, headers=params['headers']) as response:
            if response.status_code == 200:
                token = response.json()['access_token']
                print("Successfully obtained access token!!!")
            else:
                print(f"Failed to obtain access token. Status code: {response.status_code}, Response: {response.text}")
        return token

# - Chama API    
    def converter_para_milissegundos(self, start_date, end_date, formato = "%Y-%m-%d"):

        data_inicio = pd.to_datetime(start_date, format=formato).timestamp()
        data_fim = pd.to_datetime(end_date, format=formato).timestamp()

        timestamp_inicio_milissegundos = int(data_inicio * 1000)
        timestamp_fim_milissegundos = int(data_fim * 1000)
        
        return timestamp_inicio_milissegundos, timestamp_fim_milissegundos
        
        
    def chamar_api(self, params_s = None, endpoint = None):
       
        if endpoint == None:
            endpoint = 'history'
        url =f'https://developers.hotmart.com/payments/api/v1/sales/{endpoint}'
        headers_s={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        
        response = requests.get(url, params=params_s, headers = headers_s)
        
        if response.status_code == 200:
            return response.json().get('items', []), response.json().get('page_info', {}), response.headers
        else:
            print(f"Failed to fetch sales data. Status code: {response.status_code}, Response: {response.text}")
            
# Transforma dados em df    
    def get_sales_hm_aux(self, start_date = None, end_date=None, transaction = '', formato ='%Y-%m-%d', endpoint = None, transaction_status = str, pbar = True):
        try:
            timestamp_inicio_milissegundos, timestamp_fim_milissegundos = self.converter_para_milissegundos(start_date, end_date, formato = formato)
        except:
            pass
        
        if transaction != '':
            params_s = {
                'transaction': transaction
            }
        else:        
            params_s = {
                'start_date': f'{timestamp_inicio_milissegundos}',
                'end_date': f'{timestamp_fim_milissegundos}',
            }
        
        # Inicializando o next_page_token como None
        next_page_token = None
        all_sales_data = []
        
        switch = 0
        
        while True:
            
            sales_data, page_info, resp_headers = self.chamar_api(params_s = params_s, endpoint = endpoint)           
            all_sales_data.extend(sales_data) # Adiciona dados de vendas
            next_page_token = page_info.get('next_page_token') # Atualiza next_page_token nos parâmetros
            
            if int(resp_headers['RateLimit-Remaining']) <= 25:
                #print(resp_headers['RateLimit-Remaining']) #DEBUG
                sleep(int(resp_headers['RateLimit-Reset'])+5)
            if pbar is True:
                # progress bar
                if len(all_sales_data) >0: # Evita divisão por zero!
                    if switch == 0: # Garante que o total_runs será definido apenas uma vez!
                        total = int(page_info['total_results']/100)+1
                        pbar_tqdm = tqdm(total = total, colour = '#2CD0E5', leave = False, desc = 'DEBUG..')
                # incrementa pbar e ativa switch (após o primeiro loop)
                switch = 1
                pbar_tqdm.update(1)
                
                # Atualiza token da página para próxima chamada, se houver
                if next_page_token:
                    params_s.update({'page_token':next_page_token})
                else:
                    break
            else:
                #pbar is False
                if next_page_token:
                    params_s.update({'page_token':next_page_token})
                else:
                    break
            
        # Criar DataFrame a partir dos dados coletados
        dataframe = pd.DataFrame(all_sales_data)
        
        return dataframe 

    def get_transaction_hm(self, transactions, return_not_found = False):
        
        assert type(transactions) != str, 'transactions must be an iterable object such as list, set, colection or pd.Series!'
        
        dfs_t = []
        not_found = []
        for t in tqdm(transactions, leave = False, desc = 'Transactions..'):
            try:
                dfs_t.append(self.get_sales_hm(transaction = t))
            except:
               not_found.append(t) 
        
        if len(dfs_t) == 0:
            pass
        try:
            df = dfs_t[0]
            df.loc[:,[c for c in df.columns if 'date' in c]] = df.loc[:,[c for c in df.columns if 'date' in c]].apply(lambda x: pd.to_datetime(x, unit = 'ms'))
        except:
            print(f'\nTransação {t} não encontrada!')
            return
        
        if len(dfs_t) > 1:
            df = pd.concat(dfs_t, ignore_index = True)
            df.loc[:,[c for c in df.columns if 'date' in c]] = df.loc[:,[c for c in df.columns if 'date' in c]].apply(lambda x: pd.to_datetime(x, unit = 'ms'))
        
        if return_not_found == True:
            return df, not_found
        else:
            return df
    
# - Utilitárias
    def join_json_col(self, df = pd.DataFrame, col_name = str):
            
        # Cria df a partir da coluna de jsons
        df_temp = pd.json_normalize(df[col_name])
        #adiciona prefixo às colunas do novo df com o nome da coluna original
        df_temp.columns = [col_name+'.'+x for x in df_temp.columns] 
        
        # adiciona as colunas novas ao df original
        df = df.drop(col_name, axis = 1).join(df_temp, how = 'left')
        
        return df
    
    def to_date(self, x):
        try:
          return pd.to_datetime(x, errors='coerce', infer_datetime_format=True) - timedelta(hours = 3)
        except:
          pass
      

    def date_cols(self, df):
      df.loc[:,[x for x in df.columns if 'date ' in x.lower() or 'data ' in x.lower()]] = df.loc[:,[x for x in df.columns if 'date ' in x.lower() or 'data ' in x.lower()]].apply(lambda x: self.to_date(x))

      return df
    

    def get_df_from_json_serie(self, serie = pd.Series):
        df_aux = pd.concat([pd.DataFrame([x]) for x in tqdm(serie, leave = False, desc = 'Loading json..')])
        df = df_aux.reset_index(drop = True)
        
        return df
    
    
# - Main Function (Executa loop na paginação)
    def get_sales_hm(self, start_date=None, end_date= None, formato = '%Y-%m-%d', endpoint = None, transaction = '', pbar = True):
            
        # Chama API do hotmart
        if transaction != '':
            df = self.get_sales_hm_aux(transaction = transaction, endpoint = endpoint, pbar = False)
        else:
            df = self.get_sales_hm_aux(start_date, end_date, formato = formato, endpoint = endpoint, pbar = pbar)
        
        # Carrega tipos dos itens de cada coluna do df
        dct_types = df.apply(lambda x: x.iloc[[0]].apply(lambda y: type(y))).iloc[0].to_dict()
        
        # Carrega lista de colunas que contém objetos do tipo 'dict'
        try:
            lst_col_dict = {k:v for k,v in dct_types.items() if 'dict' in str(v)}.keys()
            # Abre colunas json dos dfs carregados:
            for c in tqdm(lst_col_dict, leave = False, desc = 'Loading Sales..'):
                df = self.join_json_col(df, c)
                
        except: # Se não houver nenhuma, siga a diante.
            print('Colunas json não encontradas\n', f'len(lst_col_dict) = {len(lst_col_dict)}')
            pass
        
        # Transforma colunas de data
        df.loc[:, [x for x in df.columns if 'date' in x.lower()]] = df.loc[:, [x for x in df.columns if 'date' in x.lower()]].apply(lambda x: x.apply(lambda y: dt.fromtimestamp(y/1000)))
        return df
