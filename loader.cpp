#include "loader.h"
#include "table.h"

using std::ifstream;

void Loader::load(const string &filename, WriteTable &output)
{
    const char* parse_result[MAX_COL_];
    int parse_result_count;

    Parser parser(sep_);

    string line;
    ifstream f(filename.c_str(), ifstream::in);

    if(!f)
    {
        std::cout<<"ERROR File Open!"<<std::endl;
        throw NotYetImplemented();
    }
    getline(f,line);
    while(f)
    {
        parse_result_count = parser.parse_line((char*)line.c_str(),parse_result);
        output.append(parse_result,parse_result_count);
        getline(f,line);
    }
    f.close();
}

/**
 * Reads one full line from buffer.
 *
 * If buffer has at least one line, returns the start of the next line and
 * \a cur points to a null-terminated string.
 *
 * Otherwise, returns \a cur.
 */
char* Loader::read_full_line(char* cur, const char* buf_start, const int buf_len)
{
    char* oldcur = cur;

    while(cur >= buf_start && cur < (buf_start + buf_len))
    {
        if ((*cur) == '\n')
        {
            *cur = 0;
            return ++cur;
        }
        cur++;
    }
    return oldcur;
}









