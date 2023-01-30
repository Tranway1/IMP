//
// Created by harper on 3/3/20.
//

#ifndef ARROW_TPCDSQUERY_H
#define ARROW_TPCDSQUERY_H

#include <string>
#include <sstream>
#include <parquet/types.h>

using namespace std;

namespace lqf {
    namespace tpcds {

        class Catalog_sales {
        public:
            static const string path;
            static const int cs_sold_date_sk = 0;
            static const int cs_sold_time_sk = 1;
            static const int cs_ship_date_sk = 2;
            static const int cs_bill_customer_sk= 3;
            static const int cs_bill_cdemo_sk= 4;
            static const int cs_bill_hdemo_sk= 5;
            static const int cs_bill_addr_sk= 6;
            static const int cs_ship_customer_sk= 7;
            static const int cs_ship_cdemo_sk= 8;
            static const int cs_ship_hdemo_sk= 9;
            static const int cs_ship_addr_sk= 10;
            static const int cs_call_center_sk= 11;
            static const int cs_catalog_page_sk= 12;
            static const int cs_ship_mode_sk= 13;
            static const int cs_warehouse_sk= 14;
            static const int cs_item_sk= 15;
            static const int cs_promo_sk= 16;
            static const int cs_order_number= 17;
            static const int cs_quantity= 18;
            static const int cs_wholesale_cost= 19;
            static const int cs_list_price= 20;
            static const int cs_sales_price= 21;
            static const int cs_ext_discount_amt= 22;
            static const int cs_ext_sales_price= 23;
            static const int cs_ext_wholesale_cost= 24;
            static const int cs_ext_list_price= 25;
            static const int cs_ext_tax= 26;
            static const int cs_coupon_amt= 27;
            static const int cs_ext_ship_cost= 28;
            static const int cs_net_paid= 29;
            static const int cs_net_paid_inc_tax= 30;
            static const int cs_net_paid_inc_ship= 31;
            static const int cs_net_paid_inc_ship_tax= 32;
            static const int cs_net_profit= 33;
        };

        class Customer_demographics {
         public:
          static const string path;
          static const int cd_demo_sk = 0;
          static const int cd_gender = 1;
          static const int cd_marital_status = 2;
          static const int cd_education_status= 3;
          static const int cd_purchase_estimate= 4;
          static const int cd_credit_rating= 5;
          static const int cd_dep_count= 6;
          static const int cd_dep_employed_count= 7;
          static const int cd_dep_college_count= 8;
        };

        class LineItem {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int PARTKEY = 1;
            static const int SUPPKEY = 2;
            static const int LINENUMBER = 3;
            static const int QUANTITY = 4;
            static const int EXTENDEDPRICE = 5;
            static const int DISCOUNT = 6;
            static const int TAX = 7;
            static const int RETURNFLAG = 8;
            static const int LINESTATUS = 9;
            static const int SHIPDATE = 10;
            static const int COMMITDATE = 11;
            static const int RECEIPTDATE = 12;
            static const int SHIPINSTRUCT = 13;
            static const int SHIPMODE = 14;
            static const int COMMENT = 15;
        };

        class Part {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int NAME = 1;
            static const int MFGR = 2;
            static const int BRAND = 3;
            static const int TYPE = 4;
            static const int SIZE = 5;
            static const int CONTAINER = 6;
            static const int RETAILPRICE = 7;
            static const int COMMENT = 8;
        };

        class PartSupp {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int SUPPKEY = 1;
            static const int AVAILQTY = 2;
            static const int SUPPLYCOST = 3;
            static const int COMMENT = 4;
        };

        class Supplier {
        public:
            static const string path;
            static const int SUPPKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int NATIONKEY = 3;
            static const int PHONE = 4;
            static const int ACCTBAL = 5;
            static const int COMMENT = 6;
        };

        class Nation {
        public:
            static const string path;
            static const int NATIONKEY = 0;
            static const int NAME = 1;
            static const int REGIONKEY = 2;
            static const int COMMENT = 3;
        };

        class Region {
        public:
            static const string path;
            static const int REGIONKEY = 0;
            static const int NAME = 1;
            static const int COMMENT = 2;
        };

        class Customer {
        public:
            static const string path;
            static const int CUSTKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int NATIONKEY = 3;
            static const int PHONE = 4;
            static const int ACCTBAL = 5;
            static const int MKTSEGMENT = 6;
            static const int COMMENT = 7;
        };

        class Orders {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int CUSTKEY = 1;
            static const int ORDERSTATUS = 2;
            static const int TOTALPRICE = 3;
            static const int ORDERDATE = 4;
            static const int ORDERPRIORITY = 5;
            static const int CLERK = 6;
            static const int SHIPPRIORITY = 7;
            static const int COMMENT = 8;
        };

        void exeQ1Backup();
        void exeQ1Simple();
        void exeQ1();
        void exeQ1Scalar();
        void exeQ2Backup();
        void exeQ2Simple();
        void exeQ2();
        void exeQ2Scalar();
        void exeQ3Backup();
        void exeQ3Simple();
        void exeQ3();
        void exeQ3Scalar();
        void exeQ4Backup();
        void exeQ4Simple();
        void exeQ4();
        void exeQ4Scalar();
        void exeQ5Backup();
        void exeQ5Simple();
        void exeQ5();
        void exeQ5Scalar();
        void executeQ1();
        void executeQ2();
        void executeQ3();
        void executeQ4();
        void executeQ5();
        void executeQ6();
        void executeQ7();
        void executeQ8();
        void executeQ9();
        void executeQ10();
        void executeQ11();
        void executeQ12();
        void executeQ13();
        void executeQ14();
        void executeQ15();
        void executeQ16();
        void executeQ17();
        void executeQ18();
        void executeQ19();
        void executeQ20();
        void executeQ21();
        void executeQ22();

        void exeQ1();

    }

    namespace udf {
        int date2year(parquet::ByteArray &);
    }
}
#endif //ARROW_TPCDSQUERY_H
