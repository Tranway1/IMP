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
        namespace q01 {
            ByteArray dateFrom("1998-09-01");
            int quantity = 24;
            int time = 38212;
            int date = 2450815;


            class Field3 : public DoubleSum {
            public:
                Field3() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ = value_.asDouble() + dataRow[LineItem::EXTENDEDPRICE].asDouble()
                                                 * (1 - dataRow[LineItem::DISCOUNT].asDouble());
                }
            };

            class Field4 : public DoubleSum {
            public:
                Field4() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ = value_.asDouble() + dataRow[LineItem::EXTENDEDPRICE].asDouble()
                                                 * (1 - dataRow[LineItem::DISCOUNT].asDouble()) *
                                                 (1 + dataRow[LineItem::TAX].asDouble());
                }
            };

        }

        using namespace q01;
// SELECT cs_ship_date_sk, cs_bill_customer_sk  FROM catalog_sales where cs_sold_time_sk=12032 and cs_sold_date_sk=2452653
// SELECT cd_demo_sk, cd_dep_college_count  FROM customer_demographics where cd_gender='F' AND cd_education_status='Secondary'
// SELECT cd_demo_sk FROM customer_demographics where cd_gender = 'M' AND cd_marital_status = 'D' AND cd_education_status = 'College'
// SELECT cs_ext_sales_price, cs_sold_date_sk, cs_item_sk  FROM catalog_sales where cs_wholesale_cost>80.0 and cs_ext_tax<500.0
// SELECT cs_ext_sales_price, cs_sold_date_sk, cs_item_sk, cs_net_paid_inc_tax, cs_net_paid_inc_ship_tax, cs_net_profit FROM catalog_sales where cs_wholesale_cost>80

        void exeQ1Scalar() {
          std::cout<<" apply filter scalar version on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                       {Catalog_sales::cs_ship_date_sk, Catalog_sales::cs_bill_customer_sk, Catalog_sales::cs_sold_time_sk,
                                                        Catalog_sales::cs_sold_date_sk});
          ColFilter filter({
              new SimpleDictPredicate<Int32Type>(Catalog_sales::cs_sold_date_sk,'=',date),
              new SimpleDictPredicate<Int32Type>(Catalog_sales::cs_sold_time_sk,'=',time)
          });
          auto filtered = filter.filter(*catalog_salesTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter scalar results size: " << s << std::endl;
        }

        void exeQ1Simple() {
          std::cout<<" apply filter steped on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                       {Catalog_sales::cs_ship_date_sk, Catalog_sales::cs_bill_customer_sk, Catalog_sales::cs_sold_time_sk,
                                                        Catalog_sales::cs_sold_date_sk});
          ColFilter filter({
              new SimplePredicate(Catalog_sales::cs_sold_date_sk,
                                  [](const DataField &field) { return field.asInt() == date; }
                                  ),
              new SimplePredicate(Catalog_sales::cs_sold_time_sk,
                                  [](const DataField &field) { return field.asInt() == time; })
          });
          auto filtered = filter.filter(*catalog_salesTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped results size: " << s << std::endl;

        }

        void exeQ1() {
            ExecutionGraph graph;
            std::cout<<" apply filter graph on " << Catalog_sales::path<< std::endl;
            auto catalog_sales = ParquetTable::Open(Catalog_sales::path,
                                               {Catalog_sales::cs_ship_date_sk, Catalog_sales::cs_bill_customer_sk, Catalog_sales::cs_sold_time_sk,
                                                Catalog_sales::cs_sold_date_sk});
            auto catalog_salesTable = graph.add(new TableNode(catalog_sales), {});

            // Use SBoost Filter

            auto filtered = graph.add(new ColFilter({new SboostPredicate<Int32Type>(Catalog_sales::cs_sold_time_sk,
                                                                    bind(&Int32DictEq::build, time)),
                                     new SboostPredicate<Int32Type>(Catalog_sales::cs_sold_date_sk,
                                                                    bind(&Int32DictEq::build, date))}),
                                       {catalog_salesTable});
            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PI(3) PEND),{filtered});
            graph.execute(false);
        }

        void exeQ1Backup() {
          std::cout<<" apply filter steped on " << Catalog_sales::path<< std::endl;
          auto catalog_salesTable = ParquetTable::Open(Catalog_sales::path,
                                                  {Catalog_sales::cs_ship_date_sk, Catalog_sales::cs_bill_customer_sk, Catalog_sales::cs_sold_time_sk,
                                                   Catalog_sales::cs_sold_date_sk});
          ColFilter filter({
              new SboostPredicate<Int32Type>(Catalog_sales::cs_sold_date_sk,
                                             bind(&Int32DictEq::build, date)),
              new SboostPredicate<Int32Type>(Catalog_sales::cs_sold_time_sk,
                                             bind(&Int32DictEq::build, time))
                           });
          auto filtered = filter.filter(*catalog_salesTable);

          int s = filtered->size();

          auto col = filtered->colSize();
          std::cout<<"filter steped results size: " << s << std::endl;

        }
    }
}
