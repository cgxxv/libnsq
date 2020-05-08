#include "nsq.h"

static void nsq_reader_connect_cb(nsqdConn *conn, void *arg)
{
    nsqReader *rdr = (nsqReader *)arg;

    _DEBUG("%s: %p", __FUNCTION__, rdr);

    if (rdr->connect_callback) {
        rdr->connect_callback(rdr, conn);
    }

    // subscribe
    buffer_reset(conn->command_buf);
    nsq_subscribe(conn->command_buf, rdr->topic, rdr->channel);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);

    // send initial RDY
    buffer_reset(conn->command_buf);
    nsq_ready(conn->command_buf, rdr->max_in_flight);
    buffered_socket_write_buffer(conn->bs, conn->command_buf);
}

static void nsq_reader_msg_cb(nsqdConn *conn, nsqMsg *msg, void *arg)
{
    nsqReader *rdr = (nsqReader *)arg;

    _DEBUG("%s: %p %p", __FUNCTION__, msg, rdr);

    if (rdr->msg_callback) {
        msg->id[sizeof(msg->id)-1] = '\0';
        rdr->msg_callback(rdr, conn, msg, rdr->ctx);
    }
}

static void nsq_reader_close_cb(nsqdConn *conn, void *arg)
{
    nsqReader *rdr = (nsqReader *)arg;

    _DEBUG("%s: %p", __FUNCTION__, rdr);

    if (rdr->close_callback) {
        rdr->close_callback(rdr, conn);
    }

    LL_DELETE(rdr->conns, conn);

    // There is no lookupd, try to reconnect to nsqd directly
    if (rdr->lookupd == NULL) {
        ev_timer_again(conn->loop, conn->reconnect_timer);
    } else {
        free_nsqd_connection(conn);
    }
}

static void nsq_reader_reconnect_cb(EV_P_ struct ev_timer *w, int revents)
{
    nsqdConn *conn = (nsqdConn *)w->data;
    nsqReader *rdr = (nsqReader *)conn->arg;

    if (rdr->lookupd == NULL) {
        _DEBUG("%s: There is no lookupd, try to reconnect to nsqd directly", __FUNCTION__);
        nsq_reader_connect_to_nsqd(rdr, conn->address, conn->port);
    }

    free_nsqd_connection(conn);
}

static void nsq_reader_lookupd_poll_cb(EV_P_ struct ev_timer *w, int revents)
{
    nsqReader *rdr = (nsqReader *)w->data;
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    int count = 0, idx = -1;

    LL_FOREACH(rdr->lookupd, nsqlookupd_endpoint) {
        count++;
    }
    if(count == 0) {
        goto end;
    }
    
    idx = nsq_lookupd_connect_producer(rdr->lookupd, count, rdr->topic, rdr->httpc, rdr, NSQ_LOOKUPD_MODE_READ);

    _DEBUG("%s: rdr %p, lookupd count (%d), connected the (%d) nsqlookupd", __FUNCTION__, rdr, count, idx);

end:
    ev_timer_again(rdr->loop, rdr->lookupd_poll_timer);
}

void nsq_reader_loop_producers(nsq_json_t *producers, nsqReader *rdr)
{
    nsqdConn *conn;
    nsq_json_t *producer, *broadcast_address_obj, *tcp_port_obj;
    const char *broadcast_address;
    int i, found, tcp_port;

    for (i = 0; i < nsq_json_array_length(producers); i++) {
        producer = nsq_json_array_get(producers, i);
        nsq_json_object_get(producer, "broadcast_address", &broadcast_address_obj);
        nsq_json_object_get(producer, "tcp_port", &tcp_port_obj);

        broadcast_address = nsq_json_string_value(broadcast_address_obj);
        tcp_port = nsq_json_int_value(tcp_port_obj);

        _DEBUG("%s: broadcast_address %s, port %d", __FUNCTION__, broadcast_address, tcp_port);

        found = 0;
        LL_FOREACH(rdr->conns, conn) {
            if (strcmp(conn->bs->address, broadcast_address) == 0
                && conn->bs->port == tcp_port) {
                found = 1;
                break;
            }
        }

        if (!found) {
            nsq_reader_connect_to_nsqd(rdr, broadcast_address, tcp_port);
        }
    }
}

nsqReader *new_nsq_reader(struct ev_loop *loop, const char *topic, const char *channel, void *ctx,
    nsqRWCfg *cfg,
    void (*connect_callback)(nsqReader *rdr, nsqdConn *conn),
    void (*close_callback)(nsqReader *rdr, nsqdConn *conn),
    void (*msg_callback)(nsqReader *rdr, nsqdConn *conn, nsqMsg *msg, void *ctx))
{
    nsqReader *rdr;

    rdr = (nsqReader *)malloc(sizeof(nsqReader));
    rdr->cfg = new_nsq_rw_cfg();
    rdr->topic = strdup(topic);
    rdr->channel = strdup(channel);
    rdr->max_in_flight = 1;
    rdr->connect_callback = connect_callback;
    rdr->close_callback = close_callback;
    rdr->msg_callback = msg_callback;
    rdr->ctx = ctx;
    rdr->conns = NULL;
    rdr->lookupd = NULL;
    rdr->loop = loop;
    rdr->lookupd_poll_timer = malloc(sizeof(struct ev_timer));

    rdr->httpc = new_http_client(rdr->loop);

    return rdr;
}

void free_nsq_reader(nsqReader *rdr)
{
    nsqdConn *conn;
    nsqLookupdEndpoint *nsqlookupd_endpoint;

    if (rdr) {
        // TODO: this should probably trigger disconnections and then keep
        // trying to clean up until everything upstream is finished
        LL_FOREACH(rdr->conns, conn) {
            nsqd_connection_disconnect(conn);
        }
        LL_FOREACH(rdr->lookupd, nsqlookupd_endpoint) {
            free_nsqlookupd_endpoint(nsqlookupd_endpoint);
        }
        free(rdr->topic);
        free(rdr->channel);
        free(rdr->cfg);
        free(rdr->lookupd_poll_timer);
        free(rdr->httpc->timer_event);
        free(rdr->httpc);
        free(rdr);
    }
}

int nsq_reader_add_nsqlookupd_endpoint(nsqReader *rdr, const char *address, int port)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    nsqdConn *conn;

    if (rdr->lookupd == NULL) {
        // Stop reconnect timers, use lookupd timer instead
        LL_FOREACH(rdr->conns, conn) {
            nsqd_connection_stop_timer(conn);
        }

        ev_timer_init(rdr->lookupd_poll_timer, nsq_reader_lookupd_poll_cb, 0., rdr->cfg->lookupd_interval);
        rdr->lookupd_poll_timer->data = rdr;
        ev_timer_again(rdr->loop, rdr->lookupd_poll_timer);
    }

    nsqlookupd_endpoint = new_nsqlookupd_endpoint(address, port);
    LL_APPEND(rdr->lookupd, nsqlookupd_endpoint);

    return 1;
}

int nsq_reader_connect_to_nsqd(nsqReader *rdr, const char *address, int port)
{
    nsqdConn *conn;
    int rc;

    conn = new_nsqd_connection(rdr->loop, address, port,
        nsq_reader_connect_cb, nsq_reader_close_cb, nsq_reader_msg_cb, rdr->cfg, rdr);
    rc = nsqd_connection_connect(conn);
    if (rc > 0) {
        LL_APPEND(rdr->conns, conn);
    }

    if (rdr->lookupd == NULL) {
        nsqd_connection_init_timer(conn, nsq_reader_reconnect_cb);
    }

    return rc;
}

void nsq_run(struct ev_loop *loop)
{
    srand((uint32_t)time(NULL));
    ev_loop(loop, 0);
}
