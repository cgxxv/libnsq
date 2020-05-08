#ifndef __nsq_h
#define __nsq_h

#define DEFAULT_LOOKUPD_INTERVAL     5.
#define DEFAULT_COMMAND_BUF_LEN      4096
#define DEFAULT_COMMAND_BUF_CAPACITY 4096
#define DEFAULT_READ_BUF_LEN_PER     4096
#define DEFAULT_READ_BUF_LEN         16 * 1024
#define DEFAULT_READ_BUF_CAPACITY    16 * 1024
#define DEFAULT_WRITE_BUF_LEN        16 * 1024
#define DEFAULT_WRITE_BUF_CAPACITY   16 * 1024
#define NSQ_PROTOCOL_MAGIC_BUF       "  V2"

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <curl/curl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <ev.h>

#include "libevbuffsock/evbuffsock.h"
#include "utlist.h"
#include "http.h"
#include "json.h"

typedef enum {NSQ_FRAME_TYPE_RESPONSE, NSQ_FRAME_TYPE_ERROR, NSQ_FRAME_TYPE_MESSAGE} frame_type;
typedef enum {NSQ_PARAM_TYPE_INT, NSQ_PARAM_TYPE_CHAR} nsqCmdParamType;
typedef enum {NSQ_LOOKUPD_MODE_READ, NSQ_LOOKUPD_MODE_WRITE} nsqLookupdMode;
typedef struct Buffer nsqBuf;
typedef struct BufferedSocket nsqBufdSock;

typedef struct NSQMessage {
    int64_t timestamp;
    uint16_t attempts;
    char id[16+1];
    size_t body_length;
    char *body;
} nsqMsg;

nsqMsg *nsq_decode_message(const char *data, size_t data_length);
void free_nsq_message(nsqMsg *msg);

typedef struct NSQLookupdEndpoint {
    char *address;
    int port;
    struct NSQLookupdEndpoint *next;
} nsqLookupdEndpoint;

int nsq_lookupd_connect_producer(nsqLookupdEndpoint *lookupd, const int count, const char *topic,
    httpClient *httpc, void *arg, int mode);
void nsq_lookupd_request_cb(httpRequest *req, httpResponse *resp, void *arg, int mode);
nsqLookupdEndpoint *new_nsqlookupd_endpoint(const char *address, int port);
void free_nsqlookupd_endpoint(nsqLookupdEndpoint *nsqlookupd_endpoint);

typedef struct NSQRWCfg {
    ev_tstamp lookupd_interval;
    size_t command_buf_len;
    size_t command_buf_capacity;
    size_t read_buf_len;
    size_t read_buf_capacity;
    size_t write_buf_len;
    size_t write_buf_capacity;
} nsqRWCfg;

nsqRWCfg *new_nsq_rw_cfg();

typedef struct NSQDConnection {
    char *address;
    int port;
    struct BufferedSocket *bs;
    struct Buffer *command_buf;
    uint32_t current_msg_size;
    uint32_t current_frame_type;
    char *current_data;
    struct ev_loop *loop;
    ev_timer *reconnect_timer;
    void (*connect_callback)(struct NSQDConnection *conn, void *arg);
    void (*close_callback)(struct NSQDConnection *conn, void *arg);
    void (*msg_callback)(struct NSQDConnection *conn, nsqMsg *msg, void *arg);
    void *arg;
    struct NSQDConnection *next;
} nsqdConn;

nsqdConn *new_nsqd_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(nsqdConn *conn, void *arg),
    void (*close_callback)(nsqdConn *conn, void *arg),
    void (*msg_callback)(nsqdConn *conn, nsqMsg *msg, void *arg),
    nsqRWCfg *cfg, void *arg);
void free_nsqd_connection(nsqdConn *conn);
int nsqd_connection_connect(nsqdConn *conn);
size_t nsqd_connection_read_buffer(nsqBufdSock *buffsock, nsqdConn *conn);
int nsqd_connection_connect_socket(nsqdConn *conn);
void nsqd_connection_disconnect_socket(nsqBufdSock *buffsock);
void nsqd_connection_disconnect(nsqdConn *conn);
void nsqd_connection_init_timer(nsqdConn *conn, void (*reconnect_callback)(EV_P_ ev_timer *w, int revents));
void nsqd_connection_stop_timer(nsqdConn *conn);

typedef struct NSQReader {
    char *topic;
    char *channel;
    void *ctx; //context for call back
    int max_in_flight;
    nsqdConn *conns;
    struct NSQDConnInfo *infos;
    nsqLookupdEndpoint *lookupd;
    struct ev_timer *lookupd_poll_timer;
    struct ev_loop *loop;
    nsqRWCfg *cfg;
    httpClient *httpc;
    void (*connect_callback)(struct NSQReader *rdr, nsqdConn *conn);
    void (*close_callback)(struct NSQReader *rdr, nsqdConn *conn);
    void (*msg_callback)(struct NSQReader *rdr, nsqdConn *conn, nsqMsg *msg, void *ctx);
} nsqReader;

void nsq_reader_loop_producers(nsq_json_t *producers, nsqReader *arg);
nsqReader *new_nsq_reader(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    nsqRWCfg *cfg,
    void (*connect_callback)(nsqReader *rdr, nsqdConn *conn),
    void (*close_callback)(nsqReader *rdr, nsqdConn *conn),
    void (*msg_callback)(nsqReader *rdr, nsqdConn *conn, nsqMsg *msg, void *ctx));
void free_nsq_reader(nsqReader *rdr);
int nsq_reader_connect_to_nsqd(nsqReader *rdr, const char *address, int port);
int nsq_reader_connect_to_nsqlookupd(nsqReader *rdr);
int nsq_reader_add_nsqlookupd_endpoint(nsqReader *rdr, const char *address, int port);
void nsq_reader_set_loop(nsqReader *rdr, struct ev_loop *loop);
void nsq_run(struct ev_loop *loop);

typedef struct NSQWriter {
    char *topic;
    void *ctx;
    nsqdConn *conns;
    struct NSQDConnInfo *infos;
    nsqLookupdEndpoint *lookupd;
    struct ev_loop *loop;
    nsqRWCfg *cfg;
    httpClient *httpc;
} nsqWriter;

void nsq_writer_loop_producers(nsq_json_t *producers, nsqWriter *arg);
nsqWriter *new_nsq_writer(struct ev_loop *loop, const char *topic, void *ctx, nsqRWCfg *cfg);
void free_nsq_writer(nsqWriter *writer);
void nsq_writer_close(nsqdConn *conn, nsqWriter *writer);
int nsq_writer_connect_to_nsqd(nsqWriter *writer, const char *address, int port);
int nsq_writer_connect_to_nsqlookupd(nsqWriter *writer);
int nsq_writer_add_nsqlookupd_endpoint(nsqWriter *writer, const char *address, int port);
void nsq_write_msg_to_nsqd(nsqWriter *writer, const char *body);
void nsq_write_defered_msg_to_nsqd(nsqWriter *writer, const char *body, int defer_time_sec);
void nsq_write_multiple_msg_to_nsqd(nsqWriter *writer, const char **body, const int body_size);

typedef struct NSQCmdParams {
    void *v;
    nsqCmdParamType t;
} nsqCmdParams;

void *nsq_buf_malloc(size_t buf_size, size_t n, size_t l);
void nsq_buffer_add(nsqBuf *buf, const char *name, const nsqCmdParams params[], size_t psize, const char *body, const size_t body_length);
void nsq_subscribe(nsqBuf *buf, const char *topic, const char *channel);
void nsq_ready(nsqBuf *buf, int count);
void nsq_finish(nsqBuf *buf, const char *id);
void nsq_requeue(nsqBuf *buf, const char *id, int timeout_ms);
void nsq_nop(nsqBuf *buf);
void nsq_publish(nsqBuf *buf, const char *topic, const char *body);
void nsq_defer_publish(nsqBuf *buf, const char *topic, const char *body, int defer_time_sec);
void nsq_multi_publish(nsqBuf *buf, const char *topic, const char **body, const size_t body_size);
void nsq_touch(nsqBuf *buf, const char *id);
void nsq_cleanly_close_connection(nsqBuf *buf);
void nsq_auth(nsqBuf *buf, const char *secret);
void nsq_identify(nsqBuf *buf, const char *json_body);

#endif
