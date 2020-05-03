PREFIX=/usr/local
DESTDIR=
LIBDIR=${PREFIX}/lib
INCDIR=${PREFIX}/include

CFLAGS+=-g -Wall -O2 -DDEBUG -fPIC
LIBS=-lev -lcurl
AR=ar
AR_FLAGS=rc
RANLIB=ranlib

ifeq (1, $(WITH_JANSSON))
LIBS+=-ljansson
CFLAGS+=-DWITH_JANSSON
else
LIBS+=-ljson-c
endif

all: evbuffsock libnsq test

evbuffsock:
	cd libevbuffsock && ${MAKE}

libnsq: libnsq.a

%.o: %.c
	$(CC) -o $@ -c $< $(CFLAGS)

libnsq.a: libevbuffsock/buffered_socket.o libevbuffsock/buffer.o command.o reader.o nsqd_connection.o http.o message.o nsqlookupd.o json.o
	$(AR) $(AR_FLAGS) $@ $^
	$(RANLIB) $@

test: test-nsqd-sub test-lookupd-sub

test-nsqd.o: test_sub.c
	$(CC) -o $@ -c $< $(CFLAGS) -DNSQD_STANDALONE

test-nsqd-sub: test-nsqd.o libnsq.a
	$(CC) -o $@ $^ $(LIBS)

test-lookupd-sub: test_sub.o libnsq.a
	$(CC) -o $@ $^ $(LIBS)

clean: evbuffsock-clean libnsq-clean
	
evbuffsock-clean:
	cd libevbuffsock && ${MAKE} clean
	
libnsq-clean:
	rm -rf libnsq.a test-nsqd-sub test-lookupd-sub test.dSYM *.o

.PHONY: install clean all test

install:
	install -m 755 -d ${DESTDIR}${INCDIR}
	install -m 755 -d ${DESTDIR}${LIBDIR}
	install -m 755 libnsq.a ${DESTDIR}${LIBDIR}/libnsq.a
	install -m 755 nsq.h ${DESTDIR}${INCDIR}/nsq.h
