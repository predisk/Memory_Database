CXX = g++

CXXFLAGS += -O3
CFLAGS = 'pkg-config --cflangs lcm'

OBJS = main.o table.o Manager.o command.o loader.o parser.o\
        Buffer.o schema.o

TARGET = DataBase

$(TARGET) : $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

Buffer.o : Buffer.h
schema.o : schema.h
table.o:Buffer.h schema.h
loader.o:loader.h
parser.o:parser.h
Manager.o:Manager.h table.h Buffer.h schema.h
main.o : schema.h table.h Manager.h command.h

.PHONY : clean
clean:
	rm -rf edit $(OBJS)
