//
// Created by  on 3/3/20.
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
        namespace q05 {
            double ws_cost = 80;

        }

        using namespace q05;


        // SELECT cs_ext_sales_price, cs_sold_date_sk, cs_item_sk, cs_net_paid_inc_tax, cs_net_paid_inc_ship_tax, cs_net_profit
        // FROM catalog_sales
        // where cs_wholesale_cost>80



        void exeQ5Scalar() {
          std::cout<<" apply q5 simple scalar filter graph on " << Catalog_sales::path<< std::endl;
          auto catalog_sales = ParquetTable::Open(Catalog_sales::path,
                                                  {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                   Catalog_sales::cs_net_paid_inc_tax, Catalog_sales::cs_net_paid_inc_ship_tax, Catalog_sales::cs_net_profit});
          ColFilter filter({
              new SimpleDictPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost, '>', ws_cost),
          });
          auto filtered = filter.filter(*catalog_sales);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped scalar results size: " << s << std::endl;
        }

        void exeQ5Simple() {
          std::cout<<" apply q5 simple filter graph on " << Catalog_sales::path<< std::endl;
          auto catalog_sales = ParquetTable::Open(Catalog_sales::path,
                                                  {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                   Catalog_sales::cs_net_paid_inc_tax, Catalog_sales::cs_net_paid_inc_ship_tax, Catalog_sales::cs_net_profit});
          ColFilter filter({
              new SimplePredicate(Catalog_sales::cs_wholesale_cost,
                                  [](const DataField &field) { return field.asDouble() > ws_cost; }
                                  )
          });
          auto filtered = filter.filter(*catalog_sales);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped simple results size: " << s << std::endl;

        }


        void exeQ5() {
            ExecutionGraph graph;
            std::cout<<" apply filter graph on " << Catalog_sales::path<< std::endl;
            auto catalog_sales = ParquetTable::Open(Catalog_sales::path,
                                               {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                     Catalog_sales::cs_net_paid_inc_tax, Catalog_sales::cs_net_paid_inc_ship_tax, Catalog_sales::cs_net_profit});
            auto catalog_salesTable = graph.add(new TableNode(catalog_sales), {});

            // Use SBoost Filter

            auto filtered = graph.add(new ColFilter({new SboostPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost,
                                                                    bind(&DoubleDictGreater::build, ws_cost))
                                      }),
                                       {catalog_salesTable});
            graph.add(new Printer(PBEGIN PI(0) PI(1) PD(2) PD(3) PD(4) PD(5) PD(6) PEND),{filtered});
            graph.execute();
        }

        void exeQ5Backup() {
          std::cout<<" apply filter steped on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                  {Catalog_sales::cs_ext_sales_price, Catalog_sales::cs_sold_date_sk, Catalog_sales::cs_item_sk, Catalog_sales::cs_wholesale_cost,
                                                        Catalog_sales::cs_net_paid_inc_tax, Catalog_sales::cs_net_paid_inc_ship_tax, Catalog_sales::cs_net_profit});
          ColFilter filter({
              new SboostPredicate<DoubleType>(Catalog_sales::cs_wholesale_cost,
                                              bind(&DoubleDictGreater::build, ws_cost))
                           });
          auto filtered = filter.filter(*catalog_salesTable);
//          Printer printer(PBEGIN PI(0)
//                              PI(1)
//                                  PI(2)
//                                      PI(3)
//                                          PEND);
//          printer.print(*filtered);
          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped results size: " << s << std::endl;

//          Printer printer(PBEGIN PD(0) PEND);
//          printer.print(*filtered);
        }
    }
}
