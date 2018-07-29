

#ifndef TABLE_H
#define TABLE_H

#include<string>
#include<vector>
#include "Buffer.h"
#include "schema.h"

/*
 *TupleBufferCursor
*/
class PageCursor
{
    public:
        /**
        * Return the next page
        * Or NULL if no next page exists 
         */
        virtual TupleBuffer *read_next();

        /**Return Schema object for all pages*/
        virtual Schema *schema();

        /**
         * Resets the reading point to the start of the page
         */
        virtual void reset();

        /**
         * closes the cursor
         */
        virtual void close();

        virtual ~PageCursor();
};

/*
 * ntainer for a linked list of LinkedTupleBuffer
 */
class Table : public PageCursor
{
    public:
      Table() : schema_(NULL), data_head_(NULL), cur_(NULL){}
      virtual ~Table();

      enum LoadErrorT
      {
          LOAD_OK = 0
      };

      virtual LoadErrorT load(const string &filepattern, const string &separators);

      virtual void init(Schema *s, unsigned int size);

      LinkedTupleBuffer *get_root();

      LinkedTupleBuffer *read_next();

      void reset();

      virtual void close();

      Schema *schema();

      void print_table();

    protected:
      Schema *schema_;
      LinkedTupleBuffer *data_head_;
      LinkedTupleBuffer *cur_;
};

class WriteTable : public Table 
{
    public:
        WriteTable() : last_(NULL), size_(0){}

        virtual ~WriteTable();

        void init(Schema *s, unsigned int size);
        /**
         * Load a txt file, where each line is a tuple and each field
         * is separated by any character in the \a sepatators string.
         */
        LoadErrorT load(const string &filepattern, const string &separators);

        /**
         * Append the input to this table,
         * creat new buckets as nucessary
         */
        /*append the vector input*/
        virtual void append(const vector<string> &input);
        /*append the @count num input*/
        virtual void append(const char **data, unsigned int count);
        /*update the existed tuple*/
        virtual void append(const void *const src);

        /**
         * append the table to this object
         * caller must check that schemas are same/
         */
        void concatenate(const WriteTable &table);

    protected:
      LinkedTupleBuffer *last_;
      unsigned int size_;
};

#endif //TABLE_H
