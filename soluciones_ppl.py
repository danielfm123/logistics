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
soluciones = pd.DataFrame()
for n in range(2,10):
    soluciones = soluciones.append(get_solution(dataset,n,1,0.2,0.2,1,0.1))

soluciones = soluciones.reset_index(drop=True)
soluciones.to_feather("solutions/param_ppl.feather")

margen = soluciones.groupby("restock").agg({"objetivo":"mean"}).reset_index(drop=False)
sns.lineplot("restock","objetivo",data = margen)
plt.show()

optimo = soluciones.loc[soluciones.restock == 6]




