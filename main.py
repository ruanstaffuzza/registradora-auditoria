import pandas as pd
import os
from os import walk


import textwrap
def wrap_labels(ax, width, break_long_words=False):
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        labels.append(textwrap.fill(text, width=width,
                      break_long_words=break_long_words))
    ax.set_xticklabels(labels, rotation=0)
    
import seaborn as sns


l_files = list(walk('data'))[0][2]


def open_sample_rx():
    rx = pd.read_excel(os.path.join('data',l_files[3]))
    
    rx = pd.read_excel(r"C:\Users\ruan.staffuzza\Downloads\raiox_sample_consulta.xlsx",
                       dtype={'CNPJ_CPF': 'str'})
    
    rx.to_parquet('data/raiox_sample_consulta.parquet')
        
    rx = rx[['CNPJ_CPF', 'Concorrente']].drop_duplicates()
    rx['cnpj'] = pd.to_numeric(rx['CNPJ_CPF']).astype(float)
    rx.columns  = [x.lower() for x in rx.columns]
    rx['concorrente'] = rx['concorrente'].str.lower()
    return rx

def open_rx_general():
    rx = pd.read_csv(r'data/raiox_consulta_atualizacao.csv')
    rx = rx[rx.dtlote.isin(['2022-05-11', '2022-05-12', '2022-05-13'])]
    rx['dt'] = pd.to_datetime(rx.dtlote)
    rx['cnpj'] = pd.to_numeric(rx['nrcpfcnpj']).astype(float)
    rx['adq'] = rx['adq'].str.lower()
    return rx
    

def open_rx_total():
    rx = open_rx_general()
    rx = rx[['cnpj', 'adq']].drop_duplicates()
    return rx
    

rx = open_rx_total()


def open_rx_by_date():
    rx = open_rx_general()
    rx = rx[['cnpj', 'adq', 'dt']].drop_duplicates()
    return rx    


def open_agenda_excel():
    l = []
    for n in ['11', '12']:
        f = f'Agenda_CIP_RAIOX_{n}0522.xlsx'
        df = pd.read_excel(os.path.join('data', f), dtype={'NRCNPJ': 'str'})
        df['filename'] = str(f)
        l.append(df)
        print(f)
    df = pd.concat(l)
    df.columns  = [x.lower() for x in df.columns]
    
    df['adq'] = df.nmcredenciadora.str.lower()
    df['adq'] = df['adq'].map({
        'pagseguro': 'pagseguro', 
        'getnet': 'getnet', 
        'rede': 'rede',
        'banco safra s.a.': 'safrapay', 
        'cielo': 'cielo',
        'banco cooperativo sicoob s.a.': 'sipag',
        'ifood.com agência de restaurantes online s.a.': 'ifood'
        })
    
    df['cnpj'] = df['nrcnpj'].astype(float)
    del df['nrcnpj']
    
    df['file_date'] = df['filename'].str.extract(r'Agenda_CIP_RAIOX_(\d{2})0522.xlsx')
    df['file_date'] = pd.to_datetime('2022-05-' + df['file_date'])
    df['dtliqui'] = pd.to_datetime(df['dtliquidacaour'])
    
    return df

#df_orig = open_agenda_excel()
#df_orig.to_csv(r'data/agenda_cip.csv', index=False)
df_orig = pd.read_csv(r'data/agenda_cip.csv')
df = df_orig.copy()

bands = ['mastercard', 'visa', 'elo']
df['bandeira'] = df.nmarranjo.str.extract(r'^(\w+)\s')
df['bandeira'] = df['bandeira'].str.lower()
df['produto'] = df.nmarranjo.str.extract(r'\s(\w+)$')
df['produto'] = df['produto'].str.lower()


df['dtliqui'] = pd.to_datetime(df['dtliqui'])
df['file_date'] = pd.to_datetime(df['file_date'])
    
    
df = df[df.bandeira.isin(bands)]


#df = df[df['file_date']==df['dtliqui']]

a = df.groupby('filename')['dtliquidacaour'].value_counts()
a = a.rename('vlr').reset_index().sort_values(['filename', 'vlr'])


def analise_total(df, rx):
    df = df[['adq', 'cnpj']].drop_duplicates()
    dfg = rx.merge(df, how='left', on=['cnpj', 'adq'], indicator=True) 
    dfg['match'] = (dfg['_merge']=='both')
    (dfg.groupby('adq')['match'].mean()*100).plot.barh(title='% "relações-RX"encontradas na CIP',
                                                          xlabel='',
                                                          ylabel='')

analise_total(df_orig, rx)



rx = open_rx_by_date()

def analise_by_date(df, rx):
    df['dt'] = df['file_date']
    df = df[['adq', 'cnpj', 'dt']].drop_duplicates()
    dfg = rx.merge(df, how='left', on=['cnpj', 'adq', 'dt'], indicator=True) 
    dfg['match'] = (dfg['_merge']=='both')
    dfg = dfg.groupby(['adq', 'dt'])['match'].mean().reset_index()
    return dfg
    
dfg = analise_by_date(df, rx)








def analise_by_produto(df):
    rx = open_rx_general()
    rx['produto'] = rx['produto'].map({
        'Débito': 'débito',
        'Crédito à vista': 'crédito',
        'Parcelado': 'crédito'
        })
    
    rx.bandeira = rx.bandeira.str.lower()
    rx = rx[rx.bandeira.isin(bands)]
    rx = rx[['cnpj', 'adq', 'produto']].drop_duplicates()



    
    df = df[['adq', 'cnpj', 'produto']].drop_duplicates()
    dfg = rx.merge(df, how='left', on=['cnpj', 'adq', 'produto'], indicator=True) 
    dfg['match'] = (dfg['_merge']=='both')
    dfg = (dfg.groupby(['adq', 'produto'])['match'].mean()*100).reset_index()
    
    g = sns.catplot(data=dfg, x="adq", y="match",
                    hue="produto", kind="bar",
                    height=4, aspect=2
                    )
    
    for ax in g.axes.flat:
      wrap_labels(ax, 10)
    
    g.set_axis_labels("", "")
    g.fig.suptitle('% "relações-RX"encontradas na CIP', size=16, y=1.05)
    



def analise_by_produto_dia(df):
    rx = open_rx_general()
    rx['produto'] = rx['produto'].map({
        'Débito': 'débito',
        'Crédito à vista': 'crédito',
        'Parcelado': 'crédito'
        })
    
    rx.bandeira = rx.bandeira.str.lower()
    rx = rx[rx.bandeira.isin(bands)]
    rx = rx[['cnpj', 'adq', 'produto']].drop_duplicates()


    df['liquidacao'] = (df['dtliqui']-df['file_date']).dt.days
    
    
    
    
    df['liquidacao'] = (df['liquidacao']>0)*2 + (df['liquidacao']==0)*1
    df['liquidacao'] = df['liquidacao'].map({
        2: 'posterior',
        1: 'simultaneo',
        0: 'anterior'})
    df = df[['adq', 'cnpj', 'produto', 'liquidacao']].drop_duplicates()
 
    dfg = rx.merge(df, how='left', on=['cnpj', 'adq', 'produto'], indicator=True) 
    dfg['match'] = (dfg['_merge']=='both')
    dfg = (dfg.groupby(['adq', 'produto', 'liquidacao'])['match'].mean()*100).reset_index()
    
    
    dfg.liquidacao = dfg.liquidacao.map({
        'posterior': 'Dt. de liq. posterior ao file_date',
        'simultaneo': 'Dt. de liq. igual ao file_date',
        'anterior': 'Dt. de liq. anterior ao file_date',
        })
    g = sns.catplot(data=dfg, x="adq", y="match", col='liquidacao',
                    col_wrap=1,
                    hue="produto", kind="bar",
                    height=4, aspect=2
                    )
    
    for ax in g.axes.flat:
      wrap_labels(ax, 10)
    
    g.set_axis_labels("", "")
    g.fig.suptitle('% "relações-RX"encontradas na CIP', size=16, y=1.05)
    g.set_titles(col_template="{col_name}")
analise_by_produto_dia(df)





def analise_dia_liquidacao(df):

    df['dias'] = (df['dtliqui']-df['file_date']).dt.days
    df['liquidacao'] = df['dias'].astype('object')
    df['liquidacao'] = df['liquidacao'].where(df['dias']>=0, 'negativo')
    df['liquidacao'] = df['liquidacao'].where((df['dias']<2) | (df['dias']>31), '2-31')
    df['liquidacao'] = df['liquidacao'].where((df['dias']<32) | (df['dias']>61), '32-61')
    df['liquidacao'] = df['liquidacao'].where(df['dias']<62, '62+')
    df['liquidacao'] = df['liquidacao'].astype(str)
    
    
    
    
    #df['liquidacao'] = (df['liquidacao']>0)*2 + (df['liquidacao']==0)*1
    #df['liquidacao'] = df['liquidacao'].map({
     #   2: 'posterior',
      #  1: 'simultaneo',
       # 0: 'anterior'})
    #df = df[['adq', 'cnpj', 'produto', 'liquidacao']].drop_duplicates()
 
    dfg = (df.groupby(['adq', 'produto', 'liquidacao'])['vlconstituido'].sum()*100)/df.groupby(['adq', 'produto'])['vlconstituido'].sum()

    dfg = dfg.rename('var').reset_index()
    return dfg


dfg = analise_dia_liquidacao(df)


def plot_liq(dfg):
    g = sns.catplot(data=dfg, x="adq", y="var", col='produto', 
                    hue='liquidacao', hue_order=['negativo', '0', '1', '2-31', '32-61',  '62+'],
                    col_wrap=1,
                    palette=['#999999'] + sns.color_palette('GnBu', 6),
                    kind="bar",
                    height=3, aspect=3,
                    )
    
    for ax in g.axes.flat:
      wrap_labels(ax, 10)
    
    g.set_axis_labels("", "")
    g.fig.suptitle('% Valor Constituído por "dias para a liquidação"', size=16, y=1.05)
    g.set_titles(col_template="{col_name}")
    g._legend.set_title('')



plot_liq(dfg)









def analise_by_vlr(df):
    rx = open_rx_general()
    rx['produto'] = rx['produto'].map({
        'Débito': 'débito',
        'Crédito à vista': 'crédito',
        'Parcelado': 'crédito'
        })
    
    
    rx.bandeira = rx.bandeira.str.lower()
    rx = rx[rx.bandeira.isin(bands)]
    
    df['cnpj'] = df['cnpj'].astype('float').astype('str')
    rx['cnpj'] = rx['cnpj'].astype('float').astype('str')
    
    df = df.groupby(['adq', 'cnpj', 'produto'])['vlconstituido'].sum().rename('vlr_cip')
    rx = rx.groupby(['adq', 'cnpj', 'produto'])['tpv'].sum().rename('vlr_rx')
    
    dfg = rx.to_frame().join(df).reset_index()
    dfg['diff'] = (dfg['vlr_cip'].fillna(0)*(-1) + dfg['vlr_rx'])*100/dfg['vlr_rx']
    dfg['diff'] = (dfg['vlr_cip'].fillna(0)*(-1) + dfg['vlr_rx'])/10**6
    g = sns.catplot(data=dfg, x="adq", y="diff", col = 'produto',
                    col_wrap=1, 
                    hue="produto",  kind="boxen",
                    height=4, aspect=4, sharey=False,
                    )
    
    for ax in g.axes.flat:
      wrap_labels(ax, 10)
    
    g.set_axis_labels("", "TPVrx - TPVcip (MM R$)")
    g.fig.suptitle('Diferença (MM R$) do TPV-RX e do TPV-CIP', size=16, y=1.05)
    g.set_titles(col_template="{col_name}")
    return dfg
dfg = analise_by_vlr(df)



rx = open_rx_general()
rx['produto'] = rx['produto'].map({
    'Débito': 'débito',
    'Crédito à vista': 'crédito',
    'Parcelado': 'crédito'
    })


rx.bandeira = rx.bandeira.str.lower()
rx = rx[rx.bandeira.isin(bands)]

df['cnpj'] = df['cnpj'].astype('float').astype('str')
rx['cnpj'] = rx['cnpj'].astype('float').astype('str')
    

dfg_rx = (rx
 .groupby(['adq', 'cnpj', 'produto', 'dt'])['tpv']
 .sum()
 .rename('vlr_rx')
 .reset_index()
 .pivot(index=['adq', 'cnpj', 'produto'],
        columns='dt',
        values='vlr_rx'))

dfg_rx.columns = ['tpv_rx_dia11', 'tpv_rx_dia12', 'tpv_rx_dia13']


dfg = (df
 .groupby(['adq', 'cnpj', 'produto', 'dt'])['vlconstituido']
 .sum()
 .rename('vlr_rx')
 .reset_index()
 .pivot(index=['adq', 'cnpj', 'produto'],
        columns='dt',
        values='vlr_rx'))

dfg.columns = ['vlrconst_cip_filedate11', 'vlrconst_cip_filedate12']


dfgr = dfg_rx.join(dfg).reset_index()


dfgr = dfgr[dfgr['produto']=='débito']
dfgr = dfgr[dfgr['tpv_rx_dia11'].notnull()]
dfgr['diff'] = dfgr['vlrconst_cip_filedate12'].fillna(0)*100/dfgr['tpv_rx_dia11']

dfgr = dfgr[dfg['diff']<150]

import matplotlib.pyplot as plt
g = sns.FacetGrid(dfgr[dfgr['diff']<150], col="adq", col_wrap=3)
g.map(plt.hist, "diff")
g.set_axis_labels("diferença (%)", "incidência")
g.fig.suptitle('Variação (%) entre TPV-RX e do TPV-CIP - Débito', size=16, y=1.05)
g.set_titles(col_template="{col_name}")

ax = sns.scatterplot(x='tpv_rx_dia11', y='vlrconst_cip_filedate12', data=dfgr)
ax.set(yscale='log', xscale='log')    

dfgr.to_csv(r'data\diferenca_tpv_rx_cip.csv', index=False)
