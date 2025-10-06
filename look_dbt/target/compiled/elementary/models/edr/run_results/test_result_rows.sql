-- indexes are not supported in all warehouses, relevant to postgres only


-- depends_on: sujeet_data_analytics_workspace.default.elementary_test_results
select * from (
            select
            
                
        cast('this_is_just_a_long_dummy_string' as string) as elementary_test_results_id

,
                
        cast('this_is_just_a_long_dummy_string' as string) as result_row

,
                cast('2091-02-17' as timestamp) as detected_at

,
                cast('2091-02-17' as timestamp) as created_at


        ) as empty_table
        where 1 = 0