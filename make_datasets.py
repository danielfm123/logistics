import pandas as pd
import multiprocessing
import feather
import numpy as np
pd.set_option('display.max_columns', None)

sku = range(5)
avg_sales_sku = [1,5,10,15,1]
price = [30,10,10,2,2]

def make_dataset(days):
    dataset = pd.DataFrame({"sku":sku,"avg_sales":avg_sales_sku,"price": price})

    simulation = dataset.groupby("sku").apply(lambda x: pd.DataFrame({"day":range(1,days+1),
                                                       "demand":np.random.poisson(x.avg_sales,days)}))
    simulation = dataset.drop(["avg_sales"],1).set_index("sku").join(simulation).reset_index()
    simulation.drop("level_1",1,inplace=True)

    return (simulation)


for n in range(30):
    dataset = make_dataset(360)
    dataset.to_feather("datasets/test_"+str(n)+".feather")

dataset = make_dataset(360)
dataset.to_feather("datasets/train.feather")