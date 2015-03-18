#include <iostream>
#include <sstream>
#include <string>

int main()
{
    std::string line;
    std::stringstream ss;

    std::string lastKey;
    int counter = 0;

    while (std::cin)
    {
        std::getline(std::cin, line);
        ss.clear();
        ss.str(line);
        std::string key, subkey, value;
        ss >> key >> subkey >> value;
        if (lastKey != key)
        {
            std::cout << lastKey << '\t' << counter << '\n';
            lastKey = key;
            counter = 0;
        }
        counter++;
    }

    std::cout << lastKey << '\t' << counter << '\n';

    return 0;
}
