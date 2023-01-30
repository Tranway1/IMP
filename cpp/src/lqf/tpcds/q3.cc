//
// Created by harper on 3/3/20.
//
#include <iostream>
#include <chrono>
#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpcdsquery.h"

using namespace std;
using namespace parquet;
using namespace lqf::sboost;
using namespace lqf::agg;

namespace lqf {
    namespace tpcds {
        namespace q03 {
            ByteArray genderChoosen("M");
            ByteArray maritalChoosen("D");
            ByteArray educationChoosen("College");


        }

        using namespace q03;
        // SELECT cd_demo_sk
        // FROM customer_demographics
        // where cd_gender = 'M' AND cd_marital_status = 'D' AND cd_education_status = 'College'

        void exeQ3Scalar() {
          std::cout<<" apply filter simple scalar on " << Customer_demographics::path<< std::endl;
          auto customer_demographicsTable = ParquetTable::Open(Customer_demographics::path,
                                                               {Customer_demographics::cd_demo_sk, Customer_demographics::cd_gender, Customer_demographics::cd_marital_status,
                                                                Customer_demographics::cd_education_status});
          ColFilter filter({ new SimpleDictPredicate<ByteArrayType>(Customer_demographics::cd_gender,'=',genderChoosen),
                            new SimpleDictPredicate<ByteArrayType>(Customer_demographics::cd_marital_status,'=',maritalChoosen),
                            new SimpleDictPredicate<ByteArrayType>(Customer_demographics::cd_education_status,'=',educationChoosen)
          });
          auto filtered = filter.filter(*customer_demographicsTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped scalar results size: " << s << std::endl;
        }

        void exeQ3Simple() {
          std::cout<<" apply filter simple on " << Customer_demographics::path<< std::endl;
          auto customer_demographicsTable = ParquetTable::Open(Customer_demographics::path,
                                                               {Customer_demographics::cd_demo_sk, Customer_demographics::cd_gender, Customer_demographics::cd_marital_status,
                                                                Customer_demographics::cd_education_status});
          ColFilter filter({new SimplePredicate(Customer_demographics::cd_gender,
                                                [](const DataField &field) { return field.asByteArray() == genderChoosen; }
                                                ),
                            new SimplePredicate(Customer_demographics::cd_marital_status,
                                                [](const DataField &field) { return field.asByteArray() == maritalChoosen; }),
                            new SimplePredicate(Customer_demographics::cd_education_status,
                                                [](const DataField &field) { return field.asByteArray() == educationChoosen; })
          });
          auto filtered = filter.filter(*customer_demographicsTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped simple results size: " << s << std::endl;

        }

        void exeQ3() {
            ExecutionGraph graph;
            std::cout<<" apply filter graph on " << Customer_demographics::path<< std::endl;
            auto customer_demographics = ParquetTable::Open(Customer_demographics::path,
                                               {Customer_demographics::cd_demo_sk, Customer_demographics::cd_gender, Customer_demographics::cd_marital_status,
                                                             Customer_demographics::cd_education_status});
            auto customer_demographicsTable = graph.add(new TableNode(customer_demographics), {});

            // Use SBoost Filter

            auto filtered = graph.add(new ColFilter({new SboostPredicate<ByteArrayType>(Customer_demographics::cd_gender,
                                                                    bind(&ByteArrayDictEq::build, genderChoosen)),
                                                     new SboostPredicate<ByteArrayType>(Customer_demographics::cd_marital_status,
                                                                                        bind(&ByteArrayDictEq::build, maritalChoosen)),
                                     new SboostPredicate<ByteArrayType>(Customer_demographics::cd_education_status,
                                                                    bind(&ByteArrayDictEq::build, educationChoosen))}),
                                       {customer_demographicsTable});
            graph.add(new Printer(PBEGIN PI(0) PB(1) PB(2) PB(3) PEND),{filtered});
            graph.execute(false);
        }

        void exeQ3Backup() {
          std::cout<<" apply filter steped on " << Customer_demographics::path<< std::endl;
          auto customer_demographicsTable = ParquetTable::Open(Customer_demographics::path,
                                                  {Customer_demographics::cd_demo_sk, Customer_demographics::cd_gender, Customer_demographics::cd_marital_status,
                                                                Customer_demographics::cd_education_status});
          ColFilter filter({new SboostPredicate<ByteArrayType>(Customer_demographics::cd_gender,
                                                               bind(&ByteArrayDictEq::build, genderChoosen)),
                            new SboostPredicate<ByteArrayType>(Customer_demographics::cd_marital_status,
                                                               bind(&ByteArrayDictEq::build, maritalChoosen)),
                            new SboostPredicate<ByteArrayType>(Customer_demographics::cd_education_status,
                                                               bind(&ByteArrayDictEq::build, educationChoosen))});
          auto filtered = filter.filter(*customer_demographicsTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped results size: " << s << std::endl;

        }
    }
}
