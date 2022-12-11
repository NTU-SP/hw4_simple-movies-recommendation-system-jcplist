CC = gcc
CFLAGS = -Wall -O3
LIB = -pthread -lm

all: tserver.o pserver.o lib.o
	$(CC) $(CFLAGS) tserver.o lib.o -o tserver $(LIB)
	$(CC) $(CFLAGS) pserver.o lib.o -o pserver $(LIB)

thread: tserver.o lib.o
	$(CC) $(CFLAGS) tserver.o lib.o -o tserver $(LIB)

process: pserver.o lib.o
	$(CC) $(CFLAGS) pserver.o lib.o -o pserver $(LIB)

tserver.o: server.c
	$(CC) $(CFLAGS) $(LIB) -c server.c -o tserver.o

pserver.o: server.c 
	$(CC) $(CFLAGS) $(LIB) -c -D PROCESS server.c -o pserver.o

lib.o: lib.c
	$(CC) $(CFLAGS) -c lib.c

clean:
	rm -rf *.out *.o *server
