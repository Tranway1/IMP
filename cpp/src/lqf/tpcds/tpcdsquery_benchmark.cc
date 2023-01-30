//
// Created by harper on 3/2/20.
//

#include <benchmark/benchmark.h>
#include "tpcdsquery.h"

class TpcdsBenchmark : public benchmark::Fixture {
protected:
public:

    TpcdsBenchmark() {
    }

    virtual ~TpcdsBenchmark() {
    }
};


BENCHMARK_F(TpcdsBenchmark, Q1)(benchmark::State &state) {
    for (auto _ : state) {
        //run your benchmark
        lqf::tpcds::executeQ1();
    }
}

//BENCHMARK_F(TpcdsBenchmark, Q2)(benchmark::State &state) {
//    for (auto _ : state) {
//    }
//}
