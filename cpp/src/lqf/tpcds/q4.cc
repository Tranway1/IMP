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
        namespace q04 {
          double ws_cost = 80.0;
          double etax = 500.0;
        }

        using namespace q04;

        //SELECT cs_ext_sales_price, cs_sold_date_sk, cs_item_sk
        // FROM catalog_sales
        // where cs_wholesale_cost>80.0 and cs_ext_tax<500.0
        void exeQ4Scalar() {
          std::cout<<" apply filter simple scalar on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                       {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                        Catalog_sales::cs_ext_tax});
          ColFilter filter({
              new SimpleDictPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost, '>', ws_cost),
              new SimpleDictPredicate<DoubleType>(Catalog_sales::cs_ext_tax, '<', etax)
          });
          auto filtered = filter.filter(*catalog_salesTable);
          //
          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped scalar results size: " << s << std::endl;

        }

        void exeQ4Simple() {
          std::cout<<" apply filter simple on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                       {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                        Catalog_sales::cs_ext_tax});
          ColFilter filter({
              new SimplePredicate(Catalog_sales::cs_wholesale_cost,
                                  [](const DataField &field) { return field.asDouble() > ws_cost; }),
              new SimplePredicate(Catalog_sales::cs_ext_tax,
                                  [](const DataField &field) {  return field.asDouble() < etax; })
          });
          auto filtered = filter.filter(*catalog_salesTable);
          //
          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped simple results size: " << s << std::endl;

        }


        void exeQ4() {
            ExecutionGraph graph;
            std::cout<<" apply filter graph on " << Catalog_sales::path<< std::endl;
            auto catalog_sales = ParquetTable::Open(Catalog_sales::path,
                                               {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                     Catalog_sales::cs_ext_tax});
            auto catalog_salesTable = graph.add(new TableNode(catalog_sales), {});

            // Use SBoost Filter

            auto filtered = graph.add(new ColFilter({new SboostPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost,
                                                                                     bind(&DoubleDictGreater::build, ws_cost)),
                                          new SboostPredicate<DoubleType>(Catalog_sales::cs_ext_tax,
                                                                          bind(&DoubleDictLess::build, etax))}),
                                       {catalog_salesTable});
            graph.add(new Printer(PBEGIN PI(0) PI(1) PD(2)  PD(3) PD(4) PEND),{filtered});
            graph.execute();
        }

        void exeQ4Backup() {
          std::cout<<" apply filter steped on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                       {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                        Catalog_sales::cs_ext_tax});
          ColFilter filter({
              new SboostPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost,
                                              bind(&DoubleDictGreater::build, ws_cost)),
              new SboostPredicate<DoubleType>(Catalog_sales::cs_ext_tax,
                                              bind(&DoubleDictLess::build, etax))
                           });
          auto filtered = filter.filter(*catalog_salesTable);
//
          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped results size: " << s << std::endl;

        }
    }
}
