# -*- coding: utf-8 -*-

#===================================================================#
# Desativa o aviso de request não seguro
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#===================================================================#
from datetime import datetime
import pandas as pd
from datetime import date
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor as PoolExecutor #Roda nos threads
import time

class bmf:
    def __init__(self, val_date):
        """
        Gera o objeto da bmf a partir de val_date em formato de datetime
        """
        self.val_date = val_date
        mes = self.val_date.month
        dia = self.val_date.day
        if self.val_date.month<10: mes='0'+str(self.val_date.month)
        if self.val_date.day<10: dia = '0'+str(self.val_date.day)
        self.dt_barra = f'{dia}/{mes}/{val_date.year}'
        self.dt_corrida = f'{val_date.year}{mes}{dia}'
        self.headers = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
    
    def _baixa_cupom(self):
        """
        Dada a val_date baixa a curva de cupom limpo
        """
        link = f'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?Data={self.dt_barra}&Data1={self.dt_corrida}&slcTaxa=DOC'
        page = requests.get(link, headers=self.headers, verify=False)
        soup = BeautifulSoup(page.content, 'html.parser')
        texto = soup.find_all('td')
        dias, taxas = [], []
        tabelas = ["['tabelaConteudo1']", "['tabelaConteudo2']"]
        for i in range(len(texto)):
            try:
                if str(texto[i]['class']) in tabelas:
                    tratado = texto[i].text.replace('\r\n','').replace(',','.').replace(' ','')
                    if i==0 or i%2==0:
                        dias.append(int(tratado))
                    else:
                        taxas.append(float(tratado)/100)
            except:
                pass
        return pd.DataFrame(data=taxas, index=dias, columns={'taxas360'})
    
    def _baixa_pre(self):
        """
        Dada a val_date baixa a curva pré-di
        """
        link = f'https://www2.bmf.com.br/pages/portal/bmfbovespa/boletim1/TxRef1.asp?Data={self.dt_barra}&Data1={self.dt_corrida}&slcTaxa=PRE'
        page = requests.get(link, headers=self.headers, verify=False)
        soup = BeautifulSoup(page.content, 'html.parser')
        texto = soup.find_all('td')
        dias, taxas252, taxas360 = [], [], []
        tabelas = ["['tabelaConteudo1']", "['tabelaConteudo2']"]
        for i in range(0,len(texto),3):
            try:
                if str(texto[i]['class']) in tabelas:
                    if i<=len(texto)-2:
                        dias.append(int(texto[i].text.replace('\r\n','').replace(',','.').replace(' ','')))
                        taxas252.append(float(texto[i+1].text.replace('\r\n','').replace(',','.').replace(' ',''))/100)
                        taxas360.append(float(texto[i+2].text.replace('\r\n','').replace(',','.').replace(' ',''))/100)
            except:
                pass
        return pd.DataFrame({'taxas252':taxas252,'taxas360':taxas360}, index=dias)  
    
class live():
    def __init__(self, path='di1.txt', fut_ovrr=None):
        if fut_ovrr==None:
            f = open('di1.txt','r',encoding="utf8")
            futuros = f.readlines()
            self.futuros=['DI1'+futuros[i].replace('\n','') for i in range(len(futuros))]
            f.close()
        else:
            if type(fut_ovrr)==str: fut_ovrr=[fut_ovrr]
            self.futuros = list(fut_ovrr)
        self.urls = [f"https://br.advfn.com/bolsa-de-valores/bmf/{self.futuros[i]}/cotacao"
                     for i in range(len(self.futuros))]
        self.headers = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
    
    def _parse(self, url):
        """
        Faz o parsing da url para retirar os dados necessários
        """
        try:
            page = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(page.content, 'html.parser')
            try:
                if datetime.now().hour<18:
                    bid = float(soup.find(id='quoteElementPiece16').text.replace(',','.'))/100
                    ask = float(soup.find(id='quoteElementPiece17').text.replace(',','.'))/100
                else:
                    px_last = float(soup.find(id='quoteElementPiece10').text.replace(',','.'))/100
                    px_max = float(soup.find(id='quoteElementPiece11').text.replace(',','.'))/100
                    px_min = float(soup.find(id='quoteElementPiece12').text.replace(',','.'))/100
                    px_open = float(soup.find(id='quoteElementPiece13').text.replace(',','.'))/100
                    px_close = float(soup.find(id='quoteElementPiece14').text.replace(',','.'))/100
                tds = soup.find_all('td')
                for i in range(len(tds)):
                    if tds[i].text[0:9]=='Futuro - ':
                        spt = tds[i].text[-10:].split('/')
                        vencimento = date(int(spt[2]),int(spt[1]),int(spt[0]))
                if datetime.now().hour<18: return [bid,ask,vencimento]
                else: return [px_last,px_max,px_min,px_open,px_close,vencimento]
            except:
                pass
        except:
            pass
    
    def baixa(self):
        """
        Baixa os dados live (caso a hora seja < 18horas) ou de fechamento
        """
        if datetime.now().hour<18: bid,ask,mid,vcts = [],[],[],[]
        else: px_last,px_max,px_min,px_open,px_close,vcts=[],[],[],[],[],[]
        with PoolExecutor(max_workers=16) as executor:
            for _ in executor.map(self._parse, self.urls):
                if _ is not None:
                    if datetime.now().hour<18:
                        bid.append(_[0])
                        ask.append(_[1])
                        vcts.append(_[2])
                        mid.append((_[0]+_[1])/2)
                    else:
                        px_last.append(_[0])
                        px_max.append(_[1])
                        px_min.append(_[2])
                        px_open.append(_[3])
                        px_close.append(_[4])
                        vcts.append(_[5])
            
            if datetime.now().hour<18:
                return pd.DataFrame({'Futuro':self.futuros[0:len(bid)],
                                     'Bid':bid,
                                    'Ask':ask,
                                    'Mid':mid},
                                    index=vcts)
            
            else:
                return pd.DataFrame({'Futuro':self.futuros[0:len(px_last)],
                                      'Last':px_last,
                                      'Max':px_max,
                                      'Min':px_min,
                                      'Open':px_open,
                                      'Close':px_close},
                                      index=vcts)

class historico():
    """
    Classe responsável por baixar os dados históricos do site advfn
    """
    def __init__(self, futuro, inicio=None, fim=None):
        self.inicio = inicio
        self.fim = fim
        mes_ini = self.inicio.month
        dia_ini = self.inicio.day
        mes_fim = self.fim.month
        dia_fim = self.fim.day
        if self.inicio.month<10: mes_ini = '0'+str(self.inicio.month)
        if self.inicio.day<10: dia_ini = '0'+str(self.inicio.day)
        if self.fim.month<10: mes_fim = '0'+str(self.fim.month)
        if self.fim.day<10: dia_fim = '0'+str(self.fim.day)
        self.dt_barra_ini = f'{dia_ini}/{mes_ini}/{str(inicio.year)[-2:]}'
        self.dt_barra_fim = f'{dia_fim}/{mes_fim}/{str(fim.year)[-2:]}'
        self.futuro = futuro
        self.headers = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
        self.urls = [f'https://br.advfn.com/bolsa-de-valores/bmf/{self.futuro}/historico/mais-dados-historicos?current={x}&Date1={self.dt_barra_ini}&Date2={self.dt_barra_fim}'
                     for x in range(6)]
        self.start_time = time.time()
    
    def _datas(self,lista):
        """
        Converte o string de data em datetime.date
        """
        dts=[]
        for data in lista:
            dt = data.split(' ')
            if dt[1]=='Jan': dts.append(date(int(dt[2]),1,int(dt[0])))
            if dt[1]=='Fev': dts.append(date(int(dt[2]),2,int(dt[0])))
            if dt[1]=='Mar': dts.append(date(int(dt[2]),3,int(dt[0])))
            if dt[1]=='Abr': dts.append(date(int(dt[2]),4,int(dt[0])))
            if dt[1]=='Mai': dts.append(date(int(dt[2]),5,int(dt[0])))
            if dt[1]=='Jun': dts.append(date(int(dt[2]),6,int(dt[0])))
            if dt[1]=='Jul': dts.append(date(int(dt[2]),7,int(dt[0])))
            if dt[1]=='Ago': dts.append(date(int(dt[2]),8,int(dt[0])))
            if dt[1]=='Set': dts.append(date(int(dt[2]),9,int(dt[0])))
            if dt[1]=='Out': dts.append(date(int(dt[2]),10,int(dt[0])))
            if dt[1]=='Nov': dts.append(date(int(dt[2]),11,int(dt[0])))
            if dt[1]=='Dez': dts.append(date(int(dt[2]),12,int(dt[0])))
        return dts
    
    def _parse(self,url):
        """
        Faz o parsing do link gerando uma matriz de resposta com todos os dados da página
        """
        datas,closes,var,var_perc,op,mx,mn,vol=[],[],[],[],[],[],[],[]
        r = requests.get(url,headers=self.headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        # texto = soup.find_all('tr',{'class':'result'})
        texto = soup.find_all('table')[1].find_all('tr')
        if len(texto)>1:
            for i in range(1,len(texto)):
                ft = texto[i].text.replace('.','').replace(',','.').split('\n')
                datas.append(ft[1])
                closes.append(float(ft[2]))
                var.append(float(ft[3]))
                var_perc.append(ft[4])
                op.append(float(ft[5]))
                mx.append(float(ft[6]))
                mn.append(float(ft[7]))
                vol.append(int(ft[8]))
            
            return(datas,closes,var,var_perc,op,mx,mn,vol)
        else:
            pass
        
    def relatorio(self):
        """
        Roda simultaneamente várias requisições oara gerar um dataframe com os dados da tabela
        """
        datas,closes,var,var_perc,op,mx,mn,vol=[],[],[],[],[],[],[],[]
        with PoolExecutor(max_workers=16) as executor:
            for _ in executor.map(self._parse, self.urls):
                if _ is not None:
                    datas.extend(_[0])
                    closes.extend(_[1])
                    var.extend(_[2])
                    var_perc.extend(_[3])
                    op.extend(_[4])
                    mx.extend(_[5])
                    mn.extend(_[6])
                    vol.extend(_[7])

        df = pd.DataFrame({'Fechamento':closes,
                           'Variação':var,
                           'Variação %':var_perc,
                           'Abertura':op,
                           'Máxima':mx,
                           'Mínima':mn,
                           'Volume':vol}, index=self._datas(datas))

        elapsed_time = time.time() - self.start_time
        print('Tempo de execução: ' + str(round(elapsed_time,4)) + 's')
        return df.sort_index().drop_duplicates()
