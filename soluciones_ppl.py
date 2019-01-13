import multiprocessing
import pandas as pd
import itertools
import feather
import seaborn as sns
import sys,os
import matplotlib.pyplot as plt
if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())
from ppl_sin_motoboy import get_solution

dataset = feather.read_dataframe("datasets/train.feather")


def callback(x):
    soluciones.append(x)

pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), maxtasksperchild=1)
soluciones = []
for n in range(3,31):
    res = pool.apply_async(get_solution,kwds={"dataset" : dataset,
                                              "periodo_restock" : n,
                                              "restock_lag" : 2,
                                              "factor_dcto_anual" : 0.5,
                                              "margen" : 0.20,
                                              "costo_fijo" : 20,
                                              "costo_variable" : 0.1,
                                              "solver":"coin"},
    callback= callback)

pool.close()
pool.join()

soluciones = pd.concat(soluciones)
margen = soluciones.groupby("restock").agg({"objetivo":"mean"}).reset_index(drop=False)
sns.lineplot("restock","objetivo",data = margen)
plt.show()

soluciones = soluciones.reset_index(drop=True)
soluciones.to_feather("solutions/param_ppl.feather")




