#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <boost/filesystem.hpp>

using namespace std::literals::string_literals;

int main()
{
    std::string line;
    std::stringstream ss;

    while (std::cin)
    {
        std::cout << "> ";
        std::getline(std::cin, line);
        ss.clear();
        ss.str(line);

        std::string command;
        ss >> command;

        if ("map" == command || "reduce" == command)
        {
            std::string processor;
            if (!(ss >> processor))
            {
                std::cerr << "no processor specified\n";
                continue;
            }

            std::vector <std::string> tables;
            std::string table;

            while (ss >> table)
                tables.push_back(table);

            if (!(tables.size() > 1))
            {
                std::cerr << "no output file specified\n";
                continue;
            }

            tables.pop_back();
            std::string outputFile(table);

            char tempFileName[] = "temp";

            std::ofstream tempFile(tempFileName);
            for (const auto& inputFile : tables)
                tempFile << std::ifstream(inputFile + ".tbl").rdbuf();
            tempFile.close();
            if ("reduce" == command)
                system(("sort "s + tempFileName + " -o " + tempFileName).c_str());
            system((processor + " < " + tempFileName + " > " + outputFile + ".tbl").c_str());
        }
        else if ("list" == command)
        {
            namespace fs = boost::filesystem;
            for (fs::directory_iterator i("."), end; i != end; ++i)
                if (fs::is_regular_file(*i) && i->path().extension() == ".tbl")
                    std::cout << i->path().stem() << '\n';
        }
    }

    return 0;
}

