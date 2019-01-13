import multiprocessing
import pandas as pd
import itertools
import feather
import scipy.stats as stats


dataset = feather.read_dataframe("datasets/train.feather")
parametros = dataset.groupby(["sku"]).agg({"demand":"mean","price":"mean"}).reset_index()
restock_lag = 3

estadisticos = pd.DataFrame()
for n in range(3,31):
    aux = parametros.copy()
    aux["nivel"] = stats.poisson.interval(0.99,parametros.demand*(n+restock_lag))[1]
    aux["restock"] = n
    estadisticos = estadisticos.append(aux)

estadisticos.reset_index(drop=True).to_feather("solutions/param_stat.feather")
