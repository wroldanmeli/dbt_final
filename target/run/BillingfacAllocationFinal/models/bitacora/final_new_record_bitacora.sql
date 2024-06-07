
  
    

    create or replace table `metrics-streams-dev`.`TemporalData`.`final_new_record_bitacora`
      
    
    

    OPTIONS()
    as (
      -- depends on : (<BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_fury`>, <BigQueryRelation `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_offfury`>)  


    SELECT 
      A.sufijo, 
      B.datetime,
      'Test dbt' observation,
      STRUCT(A.records, A.total_cost) AS last_final,
      STRUCT(B.records, B.total_cost) AS prev_final,
      STRUCT(C.records, C.total_cost) AS last_dash_fury,
      STRUCT(D.records, D.total_cost) AS prev_dash_fury,
      STRUCT(E.records, E.total_cost) AS last_dash_offfury,
      STRUCT(F.records, F.total_cost) AS prev_dash_offfury,
      (A.records    - B.records)    dif_final_records,
      (A.total_cost - B.total_cost) dif_final_cost,
      (C.records    - D.records)    dif_dash_fury_records,
      (C.total_cost - D.total_cost) dif_dash_fury_cost,
      (E.records    - F.records)    dif_dash_offfury_records,
      (E.total_cost - F.total_cost) dif_dash_offfury_cost,
      '2024-06-01'  date_start_repro,
      '2024-06-04'  date_end_repro,
      G.value struct_fury_dash, 
      H.value struct_offfury_dash
    FROM  `metrics-streams-dev`.`TemporalData`.`final_last_final_values`                A 
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_prev_final_values`       B  ON (A.sufijo=B.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_last_dashfury_values`    C  ON (A.sufijo=C.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_prev_dashfury_values`    D  ON (A.sufijo=D.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_last_dashOffFury_values` E  ON (A.sufijo=E.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_prev_dashOffFury_values` F  ON (A.sufijo=F.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_fury`      G  ON (A.sufijo=G.sufijo)
        INNER JOIN `metrics-streams-dev`.`TemporalData`.`final_for_newrecord_offfury`  H  ON (A.sufijo=H.sufijo)
    );
  