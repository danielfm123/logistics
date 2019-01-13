from pulp import *
import pandas as pd
import feather
import numpy as np
import multiprocessing
import seaborn as sns
import matplotlib.pyplot as plt
pd.set_option('display.max_columns', None)

#from functions import dataset_ppl
#dataset = dataset_ppl.make_dataset(size = 30)

def get_solution(dataset, periodo_restock = 7,restock_lag = 3,factor_dcto_anual = 0.1,margen = 0.20,costo_fijo = 1, costo_variable = 0.1,solver = "coin"):
    print("Periodo restock: " + str(periodo_restock))
    ## parametros
    #costoMotoboy = 3000
    #margen = 0.20
    #restock_lag = 3
    #periodo_restock = 11
    #factor_dcto_anual = 0.1

    # Datos
    dataset = dataset.sort_values(['sku','day'])
    dataset = dataset.groupby('sku',as_index=False)
    dataset = dataset.apply(lambda x : x.iloc[ range( int(np.floor(len(x)/periodo_restock)*periodo_restock))  ])
    dataset = dataset.reset_index(drop=True).set_index(["day","sku"])

    # indices
    period =  dataset.index.levels[0].values
    skues = dataset.index.levels[1].values

    # auxiliares
    numero_orden = [x + 1 for x in range(int((period[-1] + restock_lag) / periodo_restock))]
    factor_dcto_diario = ((factor_dcto_anual + 1) ** (1 / 365) - 1)

    nivel_restock = LpVariable.dict("Nivel_Restock", skues,lowBound= 0, upBound=1000)
    stock = LpVariable.dicts("Stock_Inicial", (period,skues), lowBound=0, upBound=1000)
    venta_real =  LpVariable.dicts("Earns", (period,skues), lowBound=0)
    orden = LpVariable.dicts("Order", (numero_orden,skues), lowBound=0, upBound=1000)

    # Problema
    prob = LpProblem("Perdida",LpMaximize)

    #print("Funcion Objetivo")
    prob += - factor_dcto_diario * lpSum([stock[p][c] * dataset.loc[p, c]["price"] for p, c in itertools.product(period, skues)])\
            - costo_variable * lpSum( orden ) \
            - costo_fijo * len(numero_orden) \
            + margen * lpSum([venta_real[p][c] * dataset.loc[p, c]["price"] for p, c in itertools.product(period, skues)])

    #print("Venta Real")
    for p, c in itertools.product(period, skues):
        prob += venta_real[p][c] <= dataset.loc[p, c]["demand"]
        prob += venta_real[p][c] <= stock[p][c]

    #print("Tamano Ordenes")
    P = period
    for n, c in itertools.product(numero_orden, skues):
        prob += orden[n][c] == nivel_restock[c] - stock[ P[n * periodo_restock - restock_lag - 1] ][c]

    #print("Consistencia de Stock")
    for p, c in itertools.product(range(len(period)), skues):
        if P[p] % periodo_restock == 0:
            prob += stock[P[p]][c] == stock[P[p-1]][c] - venta_real[P[p-1]][c] + orden[int(P[p] / periodo_restock)][c]
        else:
            prob += stock[P[p]][c] == stock[P[p-1]][c] - venta_real[P[p-1]][c]

    #print("resolviendo")
    if(solver == "coin"):
        solver_cmd = solvers.COIN_CMD(threads = 1,msg=0, presolve=True)
    elif(solver == "glpk"):
        solver_cmd = solvers.GLPK_CMD()
    else:
        solver_cmd = solvers.SCIP_CMD()

    prob.solve(solver_cmd)
    print("Status:", LpStatus[prob.status])
    print(value(prob.objective))

    print("Generando Detalles")
    #detalle = []
    #for c,p in itertools.product(skues,period):
    #    detalle.append(
    #        collections.OrderedDict({
    #            "periodo":p,
    #            "sku":c,
    #            "precio": dataset.loc[p, c]["price"],
    #            "venta_potencial": dataset.loc[p, c]["demand"],
    #            "stock_inicial": value(stock[p][c]),
    #            "venta": value(venta_real[p][c]),
    #            "venta_perdida": dataset.loc[p, c]["demand"] - value(venta_real[p][c]),
    #            "margen_venta": margen * dataset.loc[p, c]["price"] * value(venta_real[p][c]),
    #            "costo_inventario": value(stock[p][c]) * dataset.loc[p, c]["price"] * factor_dcto_diario,
    #            "orden": value(orden[int((p + restock_lag) / periodo_restock)][c]) if (p + restock_lag) % periodo_restock == 0 and p >= restock_lag else 0,
    #            "restock": value(orden[int(p / periodo_restock)][c]) if p % periodo_restock == 0 else 0,
    #            #"excedente": value(excedente[int((p + restock_lag) / periodo_restock)][c]) if (p + restock_lag) % periodo_restock == 0 and p >= restock_lag else 0,
    #            "nivel_restock": value(nivel_restock[c])
    #        })
    #    )
    #detalle = pd.DataFrame(detalle)
    #detalle.head(10)

    compilado = []
    for s in skues:
        compilado.append({"sku": s,
                          "nivel":value(nivel_restock[s]),
                          "objetivo": value(prob.objective),
                          "restock":periodo_restock,
                          "price":dataset.loc[1,s]["price"]})
    compilado = pd.DataFrame(compilado)

    return(compilado)

# Ejecutar el PPL

if __name__ == "__main__":
    dataset = feather.read_dataframe("datasets/train.feather")

    def callback(x):
        soluciones.append(x)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count(), maxtasksperchild=1)
    soluciones = []
    for n in range(2,31):
        res = pool.apply_async(get_solution,kwds={"dataset" : dataset,
                                                  "periodo_restock" : n,
                                                  "restock_lag" : 4,
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

