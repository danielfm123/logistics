from pulp import *
import pandas as pd
import feather
import itertools
import multiprocessing
import numpy as np
import collections
import sys,os
if not os.getcwd() in sys.path:
    sys.path.append(os.getcwd())

#from functions.dataset_ppl import make_dataset
#dataset = make_dataset('random',132)

def get_solution(dataset = None, periodo_restock = 7,costoMotoboy = 3000,restock_lag = 3,factor_dcto_anual = 0.1,margen = 0.20, max_despacho = None, solver = "coin"):
    print("Periodo restock: " + str(periodo_restock) + ", CostoMotoboy: " + str(costoMotoboy) + ', solver: ' + solver)
    ## parametros
    #costoMotoboy = 3000
    #margen = 0.20
    #restock_lag = 3
    #periodo_restock = 11
    #factor_dcto_anual = 0.1

    # Datos
    dataset = dataset.sort_values(['codigo_material','dia'])
    dataset = dataset.groupby('codigo_material',as_index=False)
    dataset = dataset.apply(lambda x : x.iloc[ range( int(np.floor(len(x)/periodo_restock)*periodo_restock))  ])
    dataset = dataset.reset_index()
    
    dataset.loc[dataset.venta < 0,"venta"] = 0
    if max_despacho == None:
        max_despacho = int(2 * sum(dataset.groupby("codigo_material").venta.mean()) * periodo_restock )
        print("Max despacho "+str(max_despacho))
    costos = feather.read_dataframe('files/datasets/output/costos.feather').iloc[range(max_despacho),:]
    costos = (costos.costo_unitario_logistica + costos.costo_unitario_transporte)*costos.stock_en_traslado
    valores = feather.read_dataframe('files/datasets/output/value.feather')
    valores.codigo_material = [int(x) for x in valores.codigo_material]

    # indices
    period =  dataset.dia.unique()
    codigo_materiales = dataset.codigo_material.unique()
    unidades = list(range(max_despacho))

    # auxiliares
    costo_marginal = costos.diff().fillna(0)
    ventas = dataset.merge(valores)
    ventas = ventas.set_index(["dia", "codigo_material"])
    numero_orden = [x + 1 for x in range(int((period[-1] + restock_lag) / periodo_restock))]
    factor_dcto_diario = ((factor_dcto_anual + 1) ** (1 / 365) - 1)

    nivel_restock = LpVariable.dict("Nivel_Restock", codigo_materiales,lowBound= 0, upBound=1000)
    stock = LpVariable.dicts("Stock_Inicial", (period,codigo_materiales), lowBound=0, upBound=1000)
    #venta_real =  LpVariable.dicts("Earns", (period,codigo_materiales), lowBound=0)
    venta_sin_stock = LpVariable.dicts("Loss", (period,codigo_materiales), lowBound=0, upBound=1000)
    orden = LpVariable.dicts("Order", (numero_orden,codigo_materiales), lowBound=0, upBound=1000)
    #excedente = LpVariable.dicts("Excedente", (numero_orden,codigo_materiales), lowBound=0, upBound=1000)
    contador_orden = LpVariable.dicts("Case_Order", (numero_orden,unidades),cat = LpBinary)

    # Problema
    prob = LpProblem("Perdida",LpMinimize)

    print("Funcion Objetivo")
    prob += lpSum([stock[p][c] * ventas.loc[p, c]["precio"] for p, c in itertools.product(period, codigo_materiales)]) *  factor_dcto_diario\
            + lpSum( [contador_orden[n][u] * costo_marginal[u] for n,u in itertools.product(numero_orden, unidades) ] ) \
            + lpSum(venta_sin_stock) * costoMotoboy
            #+ (margen * lpSum([venta_sin_stock[n][c] * ventas.loc[1, c]["precio"] for n, c in itertools.product(numero_orden, codigo_materiales)]) if costoMotoboy > 0 else 0)
            #margen * lpSum([ventas.loc[p, c]["venta"] * ventas.loc[p, c]["precio"] for p, c in itertools.product(period, codigo_materiales)]) \

    print("Tamano Ordenes")
    N = numero_orden
    P = period
    for n, c in itertools.product(range(len(numero_orden)), codigo_materiales):
        prob += orden[N[n]][c] == nivel_restock[c] - stock[P[n * periodo_restock - restock_lag]][c] #+ excedente[n][c]

    #print("Stock Inicial")
    #aux = dataset.set_index(["codigo_material"])
    #for c in codigo_materiales:
    #    prob += stock[1][c] == nivel_restock[c] #- np.ceil(np.mean(aux.loc[c].venta) * restock_lag)

    print("Consistencia de Stock")
    for p, c in itertools.product(range(len(period)), codigo_materiales):
        if P[p] % periodo_restock == 0:
            prob += stock[P[p]][c] == stock[P[p - 1]][c] - ventas.loc[P[p - 1], c]["venta"] + venta_sin_stock[P[p - 1]][c] + orden[int(P[p] / periodo_restock)][c]
        else:
            prob += stock[P[p]][c] == stock[P[p - 1]][c] - ventas.loc[P[p - 1], c]["venta"] + venta_sin_stock[P[p - 1]][c]

    print("Restriccion costo incremental")
    for n, u in itertools.product(numero_orden, unidades[1:]):
        prob += contador_orden[n][u] <= contador_orden[n][u-1]

    print("Tamano Total Orden")
    for n in numero_orden:
        prob += lpSum([orden[n][c] for c in codigo_materiales]) == lpSum([contador_orden[n][u] for u in unidades])

    print("resolviendo")
    if(solver == "coin"):
        solver_cmd = solvers.COIN_CMD(threads = 1,msg=0, presolve=True)
    elif(solver == "glpk"):
        solver_cmd = solvers.GLPK_CMD()
    else:
        solver_cmd = solvers.SCIP_CMD()

    prob.solve(solver_cmd)
    print("Status:", LpStatus[prob.status])
    #print(value(prob.objective))

    print("Generando Detalles")
    compilado = []
    #for p, c in itertools.product(period, [133185]):
    for p, c in itertools.product(period, codigo_materiales):
        compilado.append(
            collections.OrderedDict({
                "periodo":p,
                "codigo_material":c,
                "precio": ventas.loc[p, c]["precio"],
                "venta_potencial": ventas.loc[p, c]["venta"],
                "stock_inicial": value(stock[p][c]),
                "venta": ventas.loc[p, c]["venta"],
                "venta_sin_stock": value(venta_sin_stock[p][c]),
                "venta_perdida" :0,
                "margen_venta": margen * ventas.loc[p, c]["precio"] * ventas.loc[p, c]["venta"],
                "costo_motoboy" : value(venta_sin_stock[p][c])*costoMotoboy,
                "costo_inventario": value(stock[p][c]) * ventas.loc[p, c]["precio"] * factor_dcto_diario,
                "orden": value(orden[int((p + restock_lag) / periodo_restock)][c]) if (p + restock_lag) % periodo_restock == 0 and p >= restock_lag else 0,
                "restock": value(orden[int(p / periodo_restock)][c]) if p % periodo_restock == 0 else 0,
                #"excedente": value(excedente[int((p + restock_lag) / periodo_restock)][c]) if (p + restock_lag) % periodo_restock == 0 and p >= restock_lag else 0,
                "nivel_restock": value(nivel_restock[c])
            })
        )

    compilado = pd.DataFrame(compilado)
    #compilado.loc[compilado.venta_sin_stock > 0,:]
    #compilado.to_excel('ver.xlsx')

    #compilado.to_excel("files/modeling_output/reports/solucion_ppl.xlsx")

    ordenes = []
    for n,u in itertools.product(numero_orden,range(max_despacho)):
        ordenes.append(
            collections.OrderedDict({
                "periodo": n*periodo_restock,
                "orden": n,
                "unidad_numero": u,
                "unidades": value(contador_orden[n][u]),
                "costo_marginal": costo_marginal[u] * value(contador_orden[n][u])
            })
        )
    ordenes = pd.DataFrame(ordenes)

    resumen = ({
        "margen_venta_dia": np.sum(compilado.margen_venta) / max(dataset.dia),
        "costos_inventario_dia": np.sum(compilado.costo_inventario) / max(dataset.dia),
        "costo_logistica_dia": np.sum(ordenes.costo_marginal) / max(dataset.dia),
        "costo_motoboy_dia": np.sum(compilado.costo_motoboy) / max(dataset.dia),
        "ordenes": ordenes,
        "ventas": compilado,
        "costoMotoboy": costoMotoboy,
        "margen": margen,
        "restock_lag": restock_lag,
        "periodo_restock": periodo_restock,
        "factor_dcto_anual": factor_dcto_anual,
        "porcentaje_unidades": max(ordenes.groupby("periodo").unidades.sum())/max_despacho,
        "tiempo_simplex":prob.solutionTime,
        "solver" : solver,
        "dataset_id": hash(str(dataset))
    })
    resumen['margen_dia'] = resumen["margen_venta_dia"] - resumen["costos_inventario_dia"] - resumen[
        "costo_logistica_dia"] - resumen["costo_motoboy_dia"]
    print(resumen['margen_dia'] * 360)
    print(resumen['porcentaje_unidades'])

    return (resumen)
