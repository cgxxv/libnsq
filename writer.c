#include "nsq.h"

void nsq_writer_loop_producers(nsq_json_t *producers, nsqWriter *writer)
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
        LL_FOREACH(writer->conns, conn) {
            if (strcmp(conn->bs->address, broadcast_address) == 0
                && conn->bs->port == tcp_port) {
                found = 1;
                break;
            }
        }

        if (!found) {
            nsq_writer_connect_to_nsqd(writer, broadcast_address, tcp_port);
        }
    }
}

nsqWriter *new_nsq_writer(struct ev_loop *loop, const char *topic, void *ctx, nsqRWCfg *cfg)
{
    nsqWriter *writer = malloc(sizeof(nsqWriter));
    
    writer->topic = strdup(topic);
    writer->ctx = ctx;
    writer->conns = NULL;
    writer->lookupd = NULL;
    writer->cfg = new_nsq_rw_cfg();
    writer->loop = loop;

    writer->httpc = new_http_client(loop);

    return writer;
}

void free_nsq_writer(nsqWriter *writer)
{
    nsqdConn *conn;
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    
    if (writer) {
        LL_FOREACH(writer->conns, conn) {
            nsqd_connection_disconnect_socket(conn->bs);
        }
        LL_FOREACH(writer->lookupd, nsqlookupd_endpoint) {
            free_nsqlookupd_endpoint(nsqlookupd_endpoint);
        }
        
        free(writer->topic);
        free(writer->cfg);
        free(writer->httpc->timer_event);
        free(writer->httpc);
        free(writer);
    }
}

void nsq_writer_close(nsqdConn *conn, nsqWriter *writer)
{
    _DEBUG("%s: %p", __FUNCTION__, writer);

    LL_DELETE(writer->conns, conn);

    // There is no lookupd, try to reconnect to nsqd directly
//    if (writer->lookupd == NULL) {
//        ev_timer_again(conn->loop, conn->reconnect_timer);
//    } else {
//        free_nsqd_connection(conn);
//    }
}

static void nsq_writer_reconnect_cb(EV_P_ struct ev_timer *w, int revents)
{
    nsqdConn *conn = (nsqdConn *)w->data;
    nsqWriter *writer = (nsqWriter *)conn->arg;

    if (writer->lookupd == NULL) {
        _DEBUG("%s: There is no lookupd, try to reconnect to nsqd directly\n", __FUNCTION__);
        nsq_writer_connect_to_nsqd(writer, conn->address, conn->port);
    }

    free_nsqd_connection(conn);
}

int nsq_writer_connect_to_nsqd(nsqWriter *writer, const char *address, int port)
{
    nsqdConn *conn;
    int rc;
    
    conn = new_nsqd_connection(writer->loop, address, port, NULL, NULL, NULL,
        writer->cfg, NULL);
    rc = nsqd_connection_connect_socket(conn);
    if (rc > 0) {
        LL_APPEND(writer->conns, conn);
    }

    return rc;
}

static int nsq_writer_connect_lookupd(nsqWriter *writer)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    int count = 0, idx = -1;

    LL_FOREACH(writer->lookupd, nsqlookupd_endpoint) {
        count++;
    }
    if(count == 0) {
        return 0;
    }

    idx = nsq_lookupd_connect_producer(writer->lookupd, count, writer->topic, writer->httpc, writer, NSQ_LOOKUPD_MODE_WRITE);

    _DEBUG("%s: writer %p, lookupd count (%d), connected the (%d) nsqlookupd", __FUNCTION__, writer, count, idx);

    return idx;
}

int nsq_writer_add_nsqlookupd_endpoint(nsqWriter *writer, const char *address, int port)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    int idx = -1;

    nsqlookupd_endpoint = new_nsqlookupd_endpoint(address, port);
    LL_APPEND(writer->lookupd, nsqlookupd_endpoint);

    idx = nsq_writer_connect_lookupd(writer);

    _DEBUG("%s:%d connected the (%d) nsqlookupd", __FILE__, __LINE__, idx);

    return 1;
}

void nsq_write_msg_to_nsqd(nsqWriter *writer, const char *body)
{
    _DEBUG("%s: %p", __FUNCTION__, body);
    
    nsqdConn *conn;

    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_publish(conn->command_buf, writer->topic, body);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        size_t n = nsqd_connection_read_buffer(conn->bs, conn);
        _DEBUG("%s: response size %ld", __FUNCTION__, n);
    }
}

void nsq_write_defered_msg_to_nsqd(nsqWriter *writer, const char *body, int defer_time_sec)
{
    _DEBUG("%s: %p, %d", __FUNCTION__, body, defer_time_sec);
    
    nsqdConn *conn;
    
    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_defer_publish(conn->command_buf, writer->topic, body, defer_time_sec);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        size_t n = nsqd_connection_read_buffer(conn->bs, conn);
        _DEBUG("%s: response size %ld", __FUNCTION__, n);
    }
}

void nsq_write_multiple_msg_to_nsqd(nsqWriter *writer, const char **body, const int body_size)
{
    _DEBUG("%s: %p, %d", __FUNCTION__, body, body_size);

    nsqdConn *conn;

    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_multi_publish(conn->command_buf, writer->topic, body, body_size);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        size_t n = nsqd_connection_read_buffer(conn->bs, conn);
        _DEBUG("%s: response size %ld", __FUNCTION__, n);
    }
}
