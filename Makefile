CXX = g++

CXXFLAGS += -O3

CFLAGS = 'pkg-config --cflangs lcm'

OBJS = main.o Manager.o \
        schema.o Buffer.o\
        table.o loader.o parser.o\


TARGET = DataBase
$(TARGET) : $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

Manager.o : Buffer.h table.h schema.h
schema.o : schema.h
Buffer.o : Buffer.h
loader.o : loader.h
table.o : table.h Buffer.h schema.h loader.h
main.o : table.h Manager.h command.h schema.h

.PHONY : clean
clean:
	rm -f edit $(OBJS)
