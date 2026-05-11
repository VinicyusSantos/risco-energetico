import pandas as pd
import numpy as np
from scipy.stats import linregress

df = pd.read_csv("data/processed/dataset_final.csv")

df['data'] = pd.to_datetime(df['data'])

df = df.sort_values(['regiao', 'data']).reset_index(drop=True)

vars_numericas = ['nivel', 'chuva', 'temperatura', 'ena', 'carga']

janelas = [7, 15, 30]

def rolling_slope(series, window):

    slopes_list = [np.nan] * (window - 1)

    for i in range(window - 1, len(series)):

        y = series.iloc[i - window + 1 : i + 1].values
        x = np.arange(window)

        slope, *_ = linregress(x, y)

        slopes_list.append(slope)

    return pd.Series(slopes_list, index=series.index)

for j in janelas:

    result_slope = df.groupby('regiao')['nivel'].apply(
        rolling_slope,
        window=j
    )

    df[f'nivel_slope_{j}d'] = result_slope.reset_index(
        level=0,
        drop=True
    )

    result_slope_ena = df.groupby('regiao')['ena'].apply(
        rolling_slope,
        window=j
    )

    df[f'ena_slope_{j}d'] = result_slope_ena.reset_index(
        level=0,
        drop=True
    )

df['chuva_acc_15d'] = (
    df.groupby('regiao')['chuva']
    .rolling(15)
    .sum()
    .reset_index(level=0, drop=True)
)

vars_delta = ['nivel', 'ena', 'carga']

for col in vars_delta:

    df[f'delta_{col}_30d'] = (
        df.groupby('regiao')[col]
        .diff(periods=30)
    )

df_model = df.dropna().copy()

df_model['mes'] = df_model['data'].dt.month

df_model['mes_sin'] = np.sin(
    2 * np.pi * df_model['mes'] / 12
)

df_model['mes_cos'] = np.cos(
    2 * np.pi * df_model['mes'] / 12
)

df_model['bandeira_prox_mes'] = (
    df_model.groupby('regiao')['bandeira']
    .shift(-1)
)

cols_to_pivot = [
    'nivel',
    'ena',
    'chuva',
    'temperatura',
    'carga',
    'nivel_slope_7d',
    'nivel_slope_15d',
    'nivel_slope_30d',
    'ena_slope_7d',
    'ena_slope_15d',
    'ena_slope_30d',
    'chuva_acc_15d',
    'delta_nivel_30d',
    'delta_ena_30d',
    'delta_carga_30d'
]

df_pivot = df_model.pivot(
    index='data',
    columns='regiao',
    values=cols_to_pivot
)

df_pivot.columns = [
    f'{reg}_{col}'
    for col, reg in df_pivot.columns
]

cols_globais = [
    'data',
    'bandeira',
    'bandeira_code',
    'mes_sin',
    'mes_cos',
    'bandeira_prox_mes'
]

bandeira_global = (
    df_model[cols_globais]
    .drop_duplicates('data')
    .set_index('data')
)

df_final = (
    df_pivot
    .join(bandeira_global)
    .reset_index()
)

df_final = df_final.dropna(
    subset=['bandeira_prox_mes']
)

print(df_final.head())

print(df_final.shape)

df_final.to_csv(
    "data/processed/dataset_feature_engineering.csv",
    index=False
)