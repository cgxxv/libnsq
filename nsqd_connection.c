#include "nsq.h"

static void nsqd_connection_read_size(nsqBufdSock *buffsock, void *arg);
static void nsqd_connection_read_data(nsqBufdSock *buffsock, void *arg);

static void nsqd_connection_connect_cb(nsqBufdSock *buffsock, void *arg)
{
    nsqdConn *conn = (nsqdConn *)arg;

    _DEBUG("%s: %p", __FUNCTION__, arg);

    // send magic
    buffered_socket_write(conn->bs, NSQ_PROTOCOL_MAGIC_BUF, strlen(NSQ_PROTOCOL_MAGIC_BUF));

    if (conn->connect_callback) {
        conn->connect_callback(conn, conn->arg);
    }

    buffered_socket_read_bytes(buffsock, 4, nsqd_connection_read_size, conn);
}

static size_t nsqd_connection_write_magic(nsqBufdSock *buffsock)
{
    size_t n = 0;

    // send magic
    buffer_add(buffsock->write_buf, NSQ_PROTOCOL_MAGIC_BUF, strlen(NSQ_PROTOCOL_MAGIC_BUF));
    if ((n = buffer_write_fd(buffsock->write_buf, buffsock->fd)) == -1) {
        _DEBUG("%s: wirte magic flag error, errno: %d", __FUNCTION__, errno);
        return -1;
    }

    int error;
    socklen_t errsz = sizeof(error);

    if (getsockopt(buffsock->fd, SOL_SOCKET, SO_ERROR, (void *)&error, &errsz) == -1) {
        _DEBUG("%s: getsockopt failed for \"%s:%d\" on %d",
               __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);
        buffered_socket_close(buffsock);
        return -1;
    }

    if (error) {
        _DEBUG("%s: \"%s\" for \"%s:%d\" on %d",
               __FUNCTION__, strerror(error), buffsock->address, buffsock->port, buffsock->fd);
        buffered_socket_close(buffsock);
        return -1;
    }

    _DEBUG("%s: connected to \"%s:%d\" on %d",
           __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);

    return n;
}

static void nsqd_connection_read_size(nsqBufdSock *buffsock, void *arg)
{
    nsqdConn *conn = (nsqdConn *)arg;
    uint32_t *msg_size_be;

    _DEBUG("%s: %p", __FUNCTION__, arg);

    msg_size_be = (uint32_t *)buffsock->read_buf->data;
    buffer_drain(buffsock->read_buf, 4);

    // convert message length header from big-endian
    conn->current_msg_size = ntohl(*msg_size_be);

    _DEBUG("%s: msg_size = %d bytes %p", __FUNCTION__, conn->current_msg_size, buffsock->read_buf->data);

    buffered_socket_read_bytes(buffsock, conn->current_msg_size, nsqd_connection_read_data, conn);
}

static void nsqd_connection_read_data(nsqBufdSock *buffsock, void *arg)
{
    nsqdConn *conn = (nsqdConn *)arg;
    nsqMsg *msg;

    conn->current_frame_type = ntohl(*((uint32_t *)buffsock->read_buf->data));
    buffer_drain(buffsock->read_buf, 4);
    conn->current_msg_size -= 4;

    _DEBUG("%s: frame type %d, current_msg_size: %d, data: %p", __FUNCTION__, conn->current_frame_type,
        conn->current_msg_size, buffsock->read_buf->data);

    conn->current_data = buffsock->read_buf->data;
    switch (conn->current_frame_type) {
        case NSQ_FRAME_TYPE_RESPONSE:
            if (strncmp(conn->current_data, "_heartbeat_", 11) == 0) {
                buffer_reset(conn->command_buf);
                nsq_nop(conn->command_buf);
                buffered_socket_write_buffer(conn->bs, conn->command_buf);
            } else {
                printf("\033[32m%s:%d response = %s\033[0m\n", __FILE__, __LINE__, conn->current_data);
            }
            break;
        case NSQ_FRAME_TYPE_MESSAGE:
            msg = nsq_decode_message(conn->current_data, conn->current_msg_size);
            if (conn->msg_callback) {
                conn->msg_callback(conn, msg, conn->arg);
            }
            break;
        case NSQ_FRAME_TYPE_ERROR:
            printf("\033[31m%s:%d error (%s)\033[0m\n", __FILE__, __LINE__, conn->current_data);
            break;
    }

    buffer_drain(buffsock->read_buf, conn->current_msg_size);

    buffered_socket_read_bytes(buffsock, 4, nsqd_connection_read_size, conn);
}

static void nsqd_connection_close_cb(nsqBufdSock *buffsock, void *arg)
{
    nsqdConn *conn = (nsqdConn *)arg;

    _DEBUG("%s: %p", __FUNCTION__, arg);

    if (conn->close_callback) {
        conn->close_callback(conn, conn->arg);
    }
}

static void nsqd_connection_error_cb(nsqBufdSock *buffsock, void *arg)
{
    nsqdConn *conn = (nsqdConn *)arg;

    _DEBUG("%s: conn %p", __FUNCTION__, conn);
}

nsqdConn *new_nsqd_connection(struct ev_loop *loop, const char *address, int port,
    void (*connect_callback)(nsqdConn *conn, void *arg),
    void (*close_callback)(nsqdConn *conn, void *arg),
    void (*msg_callback)(nsqdConn *conn, nsqMsg *msg, void *arg),
    void *arg)
{
    nsqio *nio = (nsqio *)arg;
    nsqdConn *conn;

    conn = (nsqdConn *)malloc(sizeof(nsqdConn));
    conn->address = strdup(address);
    conn->port = port;
    conn->command_buf = new_buffer(nio->cfg->command_buf_len, nio->cfg->command_buf_capacity);
    conn->current_msg_size = 0;
    conn->connect_callback = connect_callback;
    conn->close_callback = close_callback;
    conn->msg_callback = msg_callback;
    conn->arg = arg;
    conn->loop = loop;
    conn->reconnect_timer = NULL;

    conn->bs = new_buffered_socket(loop, address, port,
        nio->cfg->read_buf_len, nio->cfg->read_buf_capacity,
        nio->cfg->write_buf_len, nio->cfg->write_buf_capacity,
        nsqd_connection_connect_cb, nsqd_connection_close_cb,
        NULL, NULL, nsqd_connection_error_cb,
        conn);

    return conn;
}

void free_nsqd_connection(nsqdConn *conn)
{
    if (conn) {
        nsqd_connection_stop_timer(conn);
        free(conn->address);
        free_buffer(conn->command_buf);
        free_buffered_socket(conn->bs);
        free(conn);
    }
}

int nsqd_connection_connect(nsqdConn *conn)
{
    return buffered_socket_connect(conn->bs);
}

size_t nsqd_connection_read_buffer(nsqBufdSock *buffsock, nsqdConn *conn)
{
    size_t n = 0;

    while (1) {
        if (BUFFER_AVAILABLE(buffsock->read_buf) < DEFAULT_READ_BUF_LEN_PER &&
            !buffer_expand(buffsock->read_buf, DEFAULT_READ_BUF_LEN_PER)) {
            break;
        }
        
        n += recv(buffsock->fd, buffsock->read_buf->data + buffsock->read_buf->offset, DEFAULT_READ_BUF_LEN_PER, 0);
        buffsock->read_buf->offset += n;
        
        if (n <= buffsock->read_buf->length) {
            break;
        }
    }

    // convert message length header from big-endian
    conn->current_msg_size = ntohl(*((uint32_t *)buffsock->read_buf->data));
    buffer_drain(buffsock->read_buf, 4);

    conn->current_frame_type = ntohl(*((uint32_t *)buffsock->read_buf->data));
    buffer_drain(buffsock->read_buf, 4);

    conn->current_msg_size -= 4;
    conn->current_data = buffsock->read_buf->data;

    _DEBUG("%s: frame type %d, current_msg_size: %d, data: %s", __FUNCTION__, conn->current_frame_type,
        conn->current_msg_size, conn->current_data);

    switch (conn->current_frame_type) {
        case NSQ_FRAME_TYPE_RESPONSE:
            printf("\033[32m%s:%d response = %s\033[0m\n", __FILE__, __LINE__, conn->current_data);
            break;
        case NSQ_FRAME_TYPE_ERROR:
            printf("\033[31m%s:%d error (%s)\033[0m\n", __FILE__, __LINE__, conn->current_data);
            break;
    }

    buffer_drain(buffsock->read_buf, conn->current_msg_size);
    
    return n;
}

int nsqd_connection_connect_socket(nsqdConn *conn)
{
    nsqBufdSock *buffsock = conn->bs;

    struct addrinfo ai, *aitop;
    char strport[32];
    struct sockaddr *sa;
    int slen;
    long flags;

    memset(&ai, 0, sizeof(struct addrinfo));
    ai.ai_family = AF_INET;
    ai.ai_socktype = SOCK_STREAM;
    snprintf(strport, sizeof(strport), "%d", buffsock->port);
    if (getaddrinfo(buffsock->address, strport, &ai, &aitop) != 0) {
        _DEBUG("%s: getaddrinfo() failed\n", __FUNCTION__);
        return 0;
    }
    sa = aitop->ai_addr;
    slen = aitop->ai_addrlen;

    if ((buffsock->fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        _DEBUG("%s: socket() failed\n", __FUNCTION__);
        return 0;
    }

    if ((flags = fcntl(buffsock->fd, F_GETFL, NULL)) < 0) {
        close(buffsock->fd);
        _DEBUG("%s: fcntl(%d, F_GETFL) failed\n", __FUNCTION__, buffsock->fd);
        return 0;
    }
    if (fcntl(buffsock->fd, F_SETFL, flags | O_APPEND) == -1) {
        close(buffsock->fd);
        _DEBUG("%s: fcntl(%d, F_SETFL) failed\n", __FUNCTION__, buffsock->fd);
        return 0;
    }

    if (connect(buffsock->fd, sa, slen) == -1) {
        if (errno != EINPROGRESS) {
            close(buffsock->fd);
            _DEBUG("%s: connect() failed\n", __FUNCTION__);
            return 0;
        }
    }

    freeaddrinfo(aitop);
    
    if (nsqd_connection_write_magic(buffsock) == -1) {
        _DEBUG("%s: write magic error", __FUNCTION__);
        return 0;
    }
    
    buffsock->state = BS_CONNECTED;

    return buffsock->fd;
}

void nsqd_connection_disconnect_socket(nsqBufdSock *buffsock)
{
    if (buffsock->state == BS_DISCONNECTED) {
        return;
    }

    _DEBUG("%s: closing \"%s:%d\" on %d",
           __FUNCTION__, buffsock->address, buffsock->port, buffsock->fd);

    if (buffsock->fd != -1) {
        close(buffsock->fd);
        buffsock->fd = -1;
    }

    buffsock->state = BS_DISCONNECTED;
}

void nsqd_connection_disconnect(nsqdConn *conn)
{
    buffered_socket_close(conn->bs);
}

void nsqd_connection_init_timer(nsqdConn *conn,
        void (*reconnect_callback)(EV_P_ ev_timer *w, int revents))
{
    nsqio *nio = (nsqio *)conn->arg;
    conn->reconnect_timer = (ev_timer *)malloc(sizeof(ev_timer));
    ev_timer_init(conn->reconnect_timer, reconnect_callback, nio->cfg->lookupd_interval, nio->cfg->lookupd_interval);
    conn->reconnect_timer->data = conn;
}

void nsqd_connection_stop_timer(nsqdConn *conn)
{
    if (conn && conn->reconnect_timer) {
        ev_timer_stop(conn->loop, conn->reconnect_timer);
        free(conn->reconnect_timer);
    }
}
