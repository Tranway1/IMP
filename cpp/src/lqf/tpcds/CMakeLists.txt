function(ADD_TPCDS_EXEC NAME SOURCE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} PUBLIC tpcds_static)
    target_compile_options(${NAME} PUBLIC ${LQF_SIMD_FLAGS})
endfunction()


set(TPCDS_SRC
        tpcdsquery.cc
        q1.cc
        q2.cc
        q3.cc
        q4.cc
        q5.cc
        query1.cc
        query2.cc
        query3.cc
        query4.cc
        query5.cc
        query6.cc
        query7.cc
        query8.cc
        query9.cc
        query10.cc
        query11.cc
        query12.cc
        query13.cc
        query14.cc
        query15.cc
        query16.cc
        query17.cc
        query18.cc
        query19.cc
        query20.cc
        query21.cc
        query22.cc
        )

add_library(tpcds_static ${TPCDS_SRC})
target_link_libraries(tpcds_static PUBLIC lqf_static)
target_compile_options(tpcds_static PUBLIC ${LQF_SIMD_FLAGS})

set(LQF_BENCHMARK_LINK_LIBS ${LQF_BENCHMARK_LINK_LIBS} tpcds_static)

add_lqf_benchmark(tpcdsquery_benchmark)

add_tpcds_exec(lqf-tpcds-query-dev tpcdsquery_dev.cc)

