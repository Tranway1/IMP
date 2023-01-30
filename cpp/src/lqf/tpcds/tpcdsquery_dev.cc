//
// Created by harper on 4/9/20.
//

#include "tpcdsquery.h"
#include <chrono>
#include <iostream>
#include <unistd.h>
#include <fstream>

using namespace std;
using namespace std::chrono;

int main(int argc, char** argv) {
  cout << "You have entered " << argc
       << " arguments:" << "\n";

  for (int i = 0; i < argc; ++i)
    cout << argv[i]<<" ";
  cout << "\n";

  std::string query(argv[1]);
  std::string type(argv[2]);
  std::string clear(argv[3]);

  if (argc>3&& clear=="true"){
    sync();
    std::ofstream ofs("/proc/sys/vm/drop_caches");
    std::cout << "Clear cache ... "<<std::endl;
    ofs << "3" << std::endl;
  }


    // Get starting timepoint

    auto start = high_resolution_clock::now();

    if (query=="q1"){
      std::cout << "Running query 1 ... "<<std::endl;
      // Call the function,
      if (type=="simple"){
        lqf::tpcds::exeQ1Simple();
      }
      else if (type=="scalar"){
        lqf::tpcds::exeQ1Scalar();
      }
      else if (type=="graph"){
        lqf::tpcds::exeQ1();
      }
      else if (type=="step"){
        lqf::tpcds::exeQ1Backup();
      }
    }
    else if (query=="q2"){
      // Call the function,
      if (type=="simple"){
        lqf::tpcds::exeQ2Simple();
      }
      else if (type=="scalar"){
        lqf::tpcds::exeQ2Scalar();
      }
      else if (type=="graph"){
        lqf::tpcds::exeQ2();
      }
      else if (type=="step"){
        lqf::tpcds::exeQ2Backup();
      }
    }
    else if (query=="q3"){
      // Call the function,
      if (type=="simple"){
        lqf::tpcds::exeQ3Simple();
      }
      else if (type=="scalar"){
        lqf::tpcds::exeQ3Scalar();
      }
      else if (type=="graph"){
        lqf::tpcds::exeQ3();
      }
      else if (type=="step"){
        lqf::tpcds::exeQ3Backup();
      }
    }
    else if (query=="q4"){
      // Call the function,
      if (type=="simple"){
        lqf::tpcds::exeQ4Simple();
      }
      else if (type=="scalar"){
        lqf::tpcds::exeQ4Scalar();
      }
      else if (type=="graph"){
        lqf::tpcds::exeQ4();
      }
      else if (type=="step"){
        lqf::tpcds::exeQ4Backup();
      }
    }
    else if (query=="q5"){
      // Call the function,
      if (type=="simple"){
        lqf::tpcds::exeQ5Simple();
      }
      else if (type=="scalar"){
        lqf::tpcds::exeQ5Scalar();
      }
      else if (type=="graph"){
        lqf::tpcds::exeQ5();
      }
      else if (type=="step"){
        lqf::tpcds::exeQ5Backup();
      }
    }



    // Get ending timepoint
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    cout << "Time taken: " << duration.count() << " microseconds" << endl;
}