-- depends on : `metrics-streams-dev`.`TemporalData`.`final_tmp_process_output`




    
    SELECT
        'application_name' tag_key, 
        'mqadmin' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "mqadmin")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'mq-pubsub-poc' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "mq-pubsub-poc")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'mqapi' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "mqapi")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'bq-provider-manager' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "bq-provider-manager")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'mqpusher' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "mqpusher")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'mq-publisher' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "mq-publisher")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'bigqueue-load-testing-utils' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "bigqueue-load-testing-utils")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'bq-consumer-mock' tag_value,
        '!=' operator,
        '2023-02-01' from_date,
        '2030-12-31' to_date,
        '(application_name != "bq-consumer-mock")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'logan' tag_value,
        '!=' operator,
        '2022-12-02' from_date,
        '2030-12-31' to_date,
        '(application_name != "logan")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'logs-collector' tag_value,
        '!=' operator,
        '2022-12-02' from_date,
        '2030-12-31' to_date,
        '(application_name != "logs-collector")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'logs-admin' tag_value,
        '!=' operator,
        '2022-12-02' from_date,
        '2030-12-31' to_date,
        '(application_name != "logs-admin")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melidata-tracking-api' tag_value,
        '!=' operator,
        '2024-04-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melidata-tracking-api")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'traffic-layer' tag_value,
        '!=' operator,
        '2022-01-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "traffic-layer")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'kvsapi' tag_value,
        '!=' operator,
        '2022-01-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "kvsapi")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'kvsapi-migrator' tag_value,
        '!=' operator,
        '2022-01-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "kvsapi-migrator")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'dsapi' tag_value,
        '!=' operator,
        '2022-01-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "dsapi")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'dsadmin' tag_value,
        '!=' operator,
        '2022-01-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "dsadmin")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melistreams-commons' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melistreams-commons")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melistreams-producer' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melistreams-producer")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melistreams-admin' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melistreams-admin")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melistreams-pusher' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melistreams-pusher")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'melistreams' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "melistreams")' cadena

    
    union all
    


    
    SELECT
        'application_name' tag_key, 
        'message-services-util' tag_value,
        '!=' operator,
        '2023-08-01' from_date,
        '2031-12-31' to_date,
        '(application_name != "message-services-util")' cadena

    

