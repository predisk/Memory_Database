#ifndef LOADER_H
#define LOADER_H
#include<string>
#include <iostream>
#include <fstream>
#include "parser.h"
#include "assert.h"
#include "exceptions.h"
class WriteTable;

using std::string;

class Loader
{
    public:
        Loader(const char separator):sep_(separator){}

        void load(const string& filename, WriteTable &output);

    private:
        char* read_full_line(char* cur, const char* buf_start, const int buf_len);

        const char sep_;

        static const unsigned int MAX_LINE_ = 1024;

        static const unsigned int MAX_COL_ = 64;
};

#endif // LOADER_H
