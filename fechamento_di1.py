# -*- coding: utf-8 -*-
from datetime import date
import pandas as pd
import requests
from bs4 import BeautifulSoup
import getpass

class fechamento:
    def __init__(self):
        self.headers = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
        self.path = f'C:\\Users\\{getpass.getuser()}\\Desktop\\base_DI1\\'
        
    def di1(self, data, export=False):
        url = f'http://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/SistemaPregao_excel1.asp?Data={data}&Mercadoria=DI1&XLS=true'
        colunas = ['Vencimento','Cont_ab','Cont_fech','nNegocios','Cont_neg',
                   'Volume','Ajuste_ant','Ajuste_corr','Open','Min','Max','Med',
                   'Close','Ajuste','Var_p','Ultima_compra','Ultima_venda']
        fim = ['', '', '']
        fim2 = ['', '']
        disclaimer = ['', 'A taxa e os preços são apenas\r', '      prévias, não devendo ser considerados definitivos.', '']
        r = requests.get(url,headers=self.headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        texto = soup.find_all('tr')
        qbs=[]
        qbs_t=[]
        for i in range(23,len(texto)-2):
            qb = texto[i].text.split('\n')[1:]
            if qb!=fim and qb!=fim2 and qb!=disclaimer:
                qbs.append(qb)
        for i in range(1,len(qbs)):
            tmp=[]
            for x in range(len(qbs[i])-1):
                t = qbs[i][x].replace('.','').replace(',','.').replace(' ','')
                if x in [1,2,3,4,5]:
                    try:
                        tmp.append(int(t))
                    except:
                        tmp.append(t)
                elif x in [6,7,8,9,10,11,12,13,15,16]:
                    try:
                        tmp.append(float(t))
                    except:
                        tmp.append(t)
                elif x in [0,14]:
                    tmp.append(t)
            qbs_t.append(tmp)
        
        spt = data.split('/')
        data_spt = date(int(spt[2]),int(spt[1]),int(spt[0]))
        df=pd.DataFrame(data=qbs_t,columns=colunas, index=[data_spt for i in range(len(qbs_t))])
        dt_spt = data.replace('/','-').split('-')
        dt_arq = dt_spt[2]+'-'+dt_spt[1]+'-'+dt_spt[0]
        if export==True: df.to_csv(f'{self.path}DI1 {dt_arq}.csv')
        return df

    def relatorio_bmf(self,arquivo_datas='datas_curva_pre.txt'):
        self.arquivo_datas = arquivo_datas
        df=pd.DataFrame()
        # Importa as datas desejadas
        with open(self.arquivo_datas, 'r') as f:
            self.datas = [line.replace('\n','') for line in f.readlines()]
        f.close()
        # Importa as datas que dão erro
        # with open(self.arquivo_erros, 'r') as f:
        #     self.dt_err = [line.replace('\n','') for line in f.readlines()]
        # f.close()
        # Gera as datas que serão utilizadas
        dt_t=[]
        for i in trange(len(self.datas),position=0,leave=True):
            dt_spt = self.datas[i].replace('/','-').split('-')
            dt_arq = dt_spt[2]+'-'+dt_spt[1]+'-'+dt_spt[0]
            nome_arquivo = f'{self.path}DI1 {dt_arq}.csv'
            if os.path.isfile(nome_arquivo)==False:
                # if self.datas[i] not in self.dt_err:
                dt_t.append(self.datas[i])
            elif os.path.isfile(nome_arquivo)==True:
                df=df.append(pd.read_csv(nome_arquivo, sep=',', index_col=0))
                
