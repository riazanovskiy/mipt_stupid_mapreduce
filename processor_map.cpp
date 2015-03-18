#include <iostream>
#include <sstream>
#include <string>

int main()
{
    std::string line;
    std::stringstream ss;

    while (std::cin)
    {
        std::getline(std::cin, line);
        ss.clear();
        ss.str(line);
        std::string key, subkey, value;
        ss >> key >> subkey >> value;
        // printf("key: '%s'; subkey: '%s'; value: '%s'\n", key.c_str(), subkey.c_str(), value.c_str());
        if (key.size() < 3)
            std::cout << key << "\t1\n";
        else
            for (size_t i = 0; 2 + i < key.size(); i++)
                std::cout << key.substr(i, 3) << "\t1\n";
    }

    return 0;
}
