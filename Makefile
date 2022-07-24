CFLAGS=-Og -g -Wall -Wextra -Wno-sign-compare -Wno-pointer-sign
LDLIBS=-llzma

all: xzra

clean:
	rm -f xzra
