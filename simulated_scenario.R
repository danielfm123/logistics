library(tidyverse)
library(data.table)
library(future)
library(openxlsx)
library(feather)
plan(multiprocess)


scenarios = dir("datasets/",pattern = "^test",full.names = T)
parameters_ppl = read_feather("solutions/param_ppl.feather") %>%  mutate(type="ppl") %>%  select(-objetivo)
parameters_stat = read_feather("solutions/param_stat.feather") %>%  mutate(type = "stat") %>%  select(-demand)
parameters = bind_rows(parameters_ppl,parameters_stat)

simular = function(dataset,
                    restock = 2,
                    restock_lag = 1,
                    fractor_dcto_anual = 0.2,
                    porcentaje_margen = 0.2,
                    costo_fijo = 1,
                    costo_variable = 0.1){

  dataset = dataset %>% 
    arrange(sku,day)
  # summarise(dataset,sum(venta*precio))
  #Simulacion
  dataset_anterior = filter(dataset,day == 1) %>%
    group_by(sku) %>% 
    mutate(stock_inicial=nivel,
           venta = min(demand,stock_inicial),
           stock_final = stock_inicial - venta,
           stock_en_traslado = 0,
           stock_recibido = stock_inicial)
  simulacion = list(dataset_anterior)
  
  for(dia in unique(dataset$day)[-1]){
    # print(fecha)
    dia_pedido = dia - restock_lag
    dia_anterior = dia - 1
    
    if(dia_pedido > 1){
      dataset_pedido = simulacion[[dia_pedido]]
    }else{
      dataset_pedido = data.frame(stock_en_traslado = 0)
    }
    dataset_anterior = simulacion[[dia_anterior]]
    
    dataset_actual = filter(dataset,day == dia) %>% 
      ungroup() %>% 
      mutate(stock_inicial = dataset_anterior$stock_final) %>% 
      group_by(sku) %>% 
      mutate(venta = min(demand,stock_inicial),
             stock_en_traslado = max(0,((dia %% restock) == 0)*(restock - stock_inicial)) ) %>% 
      ungroup() %>% 
      mutate(
        stock_recibido = dataset_pedido$stock_en_traslado,
        stock_final = stock_inicial - venta  + stock_recibido)
    simulacion[[dia]] = dataset_actual

  }
  simulacion = do.call(bind_rows,simulacion)
  simulacion = simulacion %>% arrange(day,sku) %>% data.frame()
  
  detalle_sim = data.frame(restock,
                          restock_lag,
                          fractor_dcto_anual,
                          costo_fijo,
                          costo_variable,
                          porcentaje_margen,
                          simulacion)
  # filter(simulacion,codigo_material == 106597)

  
  # summarise(simulacion,sum(venta*precio))
  # summarise(simulacion,sum((venta_tienda+venta_remota)*precio))
  # filter(simulacion, stock_en_traslado <0)
  dataset_inventario = simulacion %>% 
    mutate(inventario = price * stock_final,
          valor_venta = price*venta)
  
  dataset_diario = dataset_inventario %>% 
    group_by(day) %>% 
    summarise(stock_en_traslado = sum(stock_en_traslado) ,
              inventario = sum(inventario),
              venta = sum(venta),
              costo_logistico = stock_en_traslado*costo_variable + (stock_en_traslado > 0)*costo_fijo)
  
  # Costo Logistico
  costo_logistica = sum(dataset_diario$costo_logistico)
  
  # Inventario Promedio
  inventario_promedio = mean(dataset_diario$inventario)
  costo_inventario_anual = fractor_dcto_anual*inventario_promedio*360/max(dataset$day)
  
  # Ventas 
  ventas = sum(dataset_inventario$valor_venta)
  margen_ventas = porcentaje_margen * ventas
  
  # evaluacion anual
  margen = margen_ventas - costo_inventario_anual - costo_logistica
  
  resultado = data.frame(
    restock,
    costo_logistica,
    ventas,
    margen_ventas,
    inventario_promedio,
    costo_inventario_anual,
    margen)
  
  return(resultado) 
  }


casos = crossing(
  restock = 2:9,
  type =c("ppl","stat"),
  scenario = scenarios
)

simulaciones = list()
for(n in 1:nrow(casos)){
  print(n/nrow(casos))
  dataset = parameters %>% subset(restock == casos$restock[n] & type == casos$type[n])
  scenario = read_feather(casos$scenario[n])
  dataset = dataset %>% left_join( scenario)
  simulacion = future({simular(dataset,casos$restock[n]) %>% 
    mutate(tipo = casos$type[n], dataset = casos$scenario[n])})
  simulaciones[[n]] = simulacion
}
simulaciones = map(simulaciones,value) %>%bind_rows()

simulaciones %>% group_by(restock,tipo) %>%  summarise(margen = mean(margen))
