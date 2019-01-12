source("init.R")
safeLibrary(tidyverse)
safeLibrary(data.table)
safeLibrary(future)
safeLibrary(openxlsx)
safeLibrary(readr)
safeLibrary(feather)
plan(multiprocess)

output_file = "files/modeling_output/reports/simulated_scenarios.xlsx"
output_detail_file = "files/modeling_output/reports/simulated_scenarios_detail.RDS"
output_fig = "files/modeling_output/figures/margen_vs_restock.png"

sales = read_feather("files/datasets/output/sales.feather") %>% select(codigo_material,fecha_dato,venta)
costs = read_feather("files/datasets/output/costos.feather")
value = read_feather("files/datasets/output/value.feather")
means = read_feather("files/datasets/output/mean_sales.feather") %>% select(codigo_material,mean_venta)

dataset_prim = sales %>% 
  left_join(value) %>% 
  group_by(codigo_material) %>% 
  arrange(fecha_dato) %>% 
  crossing(n = 1:12) %>% 
  arrange(n,fecha_dato) %>% 
  mutate(fecha_dato = min(fecha_dato) + row_number()-1) %>% 
  ungroup()

# filter(data.frame(dataset_prim),codigo_material == 162114)

simular = function(restock = 14,
                    restock_lag = 3,
                    factor_seguridad = 0.5,
                    costo_venta_remota = 5000,
                    fractor_dcto_anual = 0.1,
                    bool_venta_remota = T,
                    porcentaje_margen = 0.3){

  parametros_sim = means %>% 
    mutate(security_forecast = qpois(factor_seguridad, mean_venta*(restock+restock_lag)))
  
  dataset = dataset_prim %>% 
    group_by(codigo_material) %>% 
    arrange(fecha_dato) %>% 
    mutate(dia_n = rep(1:restock,length.out = n()),
           dia_restock = dia_n == restock)%>% 
    left_join(parametros_sim) %>% 
    ungroup() %>% 
    arrange(codigo_material,fecha_dato)
  # summarise(dataset,sum(venta*precio))
  #Simulacion
  dataset_anterior = filter(dataset,fecha_dato == min(fecha_dato)) %>%
    group_by(codigo_material) %>% 
    mutate(stock_inicial=security_forecast,
           venta_tienda = min(venta,stock_inicial),
           venta_remota = if_else(bool_venta_remota,venta - venta_tienda,0),
           stock_final = stock_inicial - venta_tienda,
           stock_en_traslado = 0,
           stock_recibido = stock_inicial)
  simulacion = list(dataset_anterior)
  names(simulacion) = min(dataset_anterior$fecha_dato)
  
  for(fecha in unique(dataset$fecha_dato)[-1]){
    fecha = as.Date(fecha,origin = as.Date("1970-01-01"))
    # print(fecha)
    fecha_pedido = as.character(fecha - restock_lag + 1)
    fecha_anterior = as.character(fecha -1)
    
    if(fecha_pedido %in% names(simulacion)){
      dataset_pedido = simulacion[[fecha_pedido]]
    }else{
      dataset_pedido = data.frame(stock_en_traslado = 0)
    }
    dataset_anterior = simulacion[[fecha_anterior]]
    
    dataset_actual = filter(dataset,fecha_dato == fecha) %>% 
      ungroup() %>% 
      mutate(stock_inicial = dataset_anterior$stock_final) %>% 
      group_by(codigo_material) %>% 
      mutate(venta_tienda = min(venta,stock_inicial),
             venta_remota = if_else(bool_venta_remota,venta - venta_tienda,0),
             stock_en_traslado = max(0,dia_restock*(security_forecast - stock_inicial)) ) %>% 
      ungroup() %>% 
      mutate(
        stock_recibido = dataset_pedido$stock_en_traslado,
        stock_final = stock_inicial - venta_tienda  + stock_recibido)
    simulacion[[as.character(fecha)]] = dataset_actual

  }
  simulacion = do.call(bind_rows,simulacion)
  simulacion = simulacion %>% arrange(fecha_dato,codigo_material) %>% data.frame()
  
  detalle_sim = data.frame(restock,
                          restock_lag,
                          factor_seguridad,
                          costo_venta_remota,
                          fractor_dcto_anual,
                          bool_venta_remota,
                          porcentaje_margen,
                          simulacion)
  # filter(simulacion,codigo_material == 106597)

  
  # summarise(simulacion,sum(venta*precio))
  # summarise(simulacion,sum((venta_tienda+venta_remota)*precio))
  # filter(simulacion, stock_en_traslado <0)
  dataset_inventario = simulacion %>% 
    mutate(inventario = precio * stock_final,
          valor_venta = precio*(venta_tienda + venta_remota),
          valor_venta_tienda = venta_tienda*precio,
          valor_venta_remova = precio*venta_remota)
  
  dataset_diario = dataset_inventario %>% 
    group_by(fecha_dato) %>% 
    summarise(stock_en_traslado = sum(stock_en_traslado) ,
              inventario = sum(inventario),
              q_venta_remota = sum(venta_remota),
              q_venta_tienda = sum(venta_tienda)) %>%
    left_join(costs) %>% 
    mutate(costo_logistico = stock_en_traslado * (costo_unitario_transporte + costo_unitario_logistica),
           costo_venta_remota = costo_venta_remota * q_venta_remota)
  
  # Costo Logistico
  costo_logistica = sum(dataset_diario$costo_logistico) + sum(dataset_diario$costo_venta_remota)
  
  # Inventario Promedio
  inventario_promedio = mean(dataset_diario$inventario)
  costo_inventario_anual = fractor_dcto_anual*inventario_promedio
  
  # Ventas 
  ventas = sum(dataset_inventario$valor_venta)
  margen_ventas = porcentaje_margen * ventas
  
  # Rotacion
  rotacion_tienda = sum(dataset_inventario$valor_venta_tienda)/inventario_promedio/12
  
  # evaluacion anual
  margen_anual = margen_ventas - costo_inventario_anual - costo_logistica
  
  resultado = data.frame(
    restock,
    restock_lag,
    factor_seguridad,
    costo_venta_remota,
    fractor_dcto_anual,
    bool_venta_remota,
    porcentaje_margen,
    costo_logistica_tienda =  sum(dataset_diario$costo_logistico),
    costo_logistica_remoto = sum(dataset_diario$costo_venta_remota),
    costo_logistica,
    venta_remota = sum(dataset_inventario$valor_venta_remova),
    venta_tienda = sum(dataset_inventario$valor_venta_tienda),
    ventas,
    inventario_promedio,
    costo_inventario_anual,
    rotacion_tienda,
    margen_ventas,
    margen_anual)
  
  return(list(resultado=resultado,detalle = detalle_sim)) 
  }


casos = crossing(
  restock = 4:15,
  restock_lag = 3,
  factor_seguridad = c(0.5,0.8,0.9,0.99),
  costo_venta_remota = c(5000),
  fractor_dcto_anual = 0.1,
  bool_venta_remota = c(T,F),
  porcentaje_margen = 0.2
)

simulaciones = pmap(casos,
                    ~future(simular(..1,..2,..3,..4,..5,..6,..7)))
simulaciones = map(simulaciones,~ value(.))

resultado = map_dfr(simulaciones,~.[["resultado"]])
write.xlsx(resultado,output_file)

detalle = map_dfr(simulaciones,~.[["detalle"]])
saveRDS(detalle,output_detail_file)

data_plot = resultado %>% 
  filter(restock >= 4) %>% 
  group_by(restock,bool_venta_remota) %>% 
  summarize(margen = max(margen_anual))

ggplot(data_plot,aes(restock,margen, color = bool_venta_remota, grou = bool_venta_remota )) + 
  geom_line(size=2) + 
  ylab("Margen Anual") + xlab("Periodo de Restock") + ggtitle("Margen Anual VS Periodo de Restock")+
  scale_y_continuous(labels = function(x) format(x, scientific = FALSE, big.mark = ".",decimal.mark = ",")) +
  geom_hline(aes(yintercept = 150307002))

ggsave(output_fig,width = 10,height = 6)

# filter(sales,codigo_material == 102344)
# filter(compilado,codigo_material == 102344)
