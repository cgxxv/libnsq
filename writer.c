#include "nsq.h"

#include "utlist.h"

static int nsq_writer_connect_lookupd(nsqio *writer)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    int count = 0, idx = -1;

    LL_FOREACH(writer->lookupd, nsqlookupd_endpoint) {
        count++;
    }
    if(count == 0) {
        return 0;
    }

    idx = nsq_lookupd_connect_producer(writer->lookupd, count, writer->topic, writer->httpc, writer);

    _DEBUG("%s: writer %p, lookupd count (%d), connected the (%d) nsqlookupd\n", __FUNCTION__, writer, count, idx);

    return idx;
}

int nsq_writer_connect_to_nsqd(nsqio *writer, const char *address, int port)
{
    nsqdConn *conn;
    int rc;
    
    conn = new_nsqd_connection(writer->loop, address, port, NULL, NULL, NULL, writer);
    rc = nsqd_connection_connect_socket(conn);
    if (rc > 0) {
        LL_APPEND(writer->conns, conn);
    }

    return rc;
}

int nsq_writer_add_nsqlookupd_endpoint(nsqio *writer, const char *address, int port)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    int idx = -1;

    nsqlookupd_endpoint = new_nsqlookupd_endpoint(address, port);
    LL_APPEND(writer->lookupd, nsqlookupd_endpoint);

    idx = nsq_writer_connect_lookupd(writer);

    _DEBUG("%s:%d connected the (%d) nsqlookupd\n", __FILE__, __LINE__, idx);

    return 1;
}

void nsq_write_msg_to_nsqd(nsqio *writer, const char *body)
{
    _DEBUG("%s: %p\n", __FUNCTION__, body);
    
    nsqdConn *conn;

    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_publish(conn->command_buf, writer->topic, body);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        nsqd_connection_read_buffer(conn->bs, conn);
    }
}

void nsq_write_defered_msg_to_nsqd(nsqio *writer, const char *body, int defer_time_sec)
{
    _DEBUG("%s: %p, %d\n", __FUNCTION__, body, defer_time_sec);
    
    nsqdConn *conn;
    
    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_defer_publish(conn->command_buf, writer->topic, body, defer_time_sec);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        nsqd_connection_read_buffer(conn->bs, conn);
    }
}

void nsq_write_multiple_msg_to_nsqd(nsqio *writer, const char **body, const int body_size)
{
    _DEBUG("%s: %p, %d\n", __FUNCTION__, body, body_size);

    nsqdConn *conn;

    LL_FOREACH(writer->conns, conn) {
        buffer_reset(conn->command_buf);
        nsq_multi_publish(conn->command_buf, writer->topic, body, body_size);
        buffer_write_fd(conn->command_buf, conn->bs->fd);

        nsqd_connection_read_buffer(conn->bs, conn);
    }
}

