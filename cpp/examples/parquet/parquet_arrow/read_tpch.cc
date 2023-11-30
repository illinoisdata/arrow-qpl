#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <qpl/qpl.h>

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <arrow/ipc/api.h>

#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/io/file.h"
#include <parquet/properties.h>

#include <map>
#include <boost/tokenizer.hpp>
#include <boost/variant.hpp>

typedef boost::variant<int, double, std::string> cell_t;

// void temp() {
//     std::ios::sync_with_stdio(false); // Optimize IO performance

//     std::ifstream file("/home/raunaks3/TPC-H/dbgen/data-1gb/lineitem.csv");

//     if (!file.is_open()) {
//         std::cout << "File could not be opened!" << std::endl;
//         // return 1;
//     }

//     std::vector<std::string> columnNames;
//     std::vector<std::string> dataTypes = {"int", "int", "int", "int", "int", "double", "double", "double", "string",
//                                             "string", "int", "int", "int", "string", "string", "string"}; // replace with your actual data types
//     std::map<std::string, std::vector<cell_t>> table;
// }

using namespace std;

int main() {
    string filename = "/home/raunaks3/TPC-H/dbgen/data-1gb/lineitem.csv";
    fstream infile(filename);
    char buffer[65536];
    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    vector<string> splittedString;

    std::map<std::string, std::vector<std::string>> lineitemMap;
    std::map<std::int32_t, std::string> colNameMap;

    int index = 0;

    string line;
    string token;
    int linecount = 0;
    while (getline(infile, line, '\n')) {
        // create istringstream from line
        istringstream iss(line);

        // use getline on isstream to split line at commas
        index = 0;
        while (getline(iss, token, ',')) {

            if(linecount == 0)
                colNameMap[index] = token;
            else
                lineitemMap[colNameMap[index]].push_back(token);
            
            index += 1;
        }

        linecount += 1;
    }

    


}
