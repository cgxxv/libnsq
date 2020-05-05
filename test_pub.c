#include "nsq.h"

int main(int argc, char **argv)
{
    if (argc < 3) {
        printf("not enough args from command line\n");
        return 1;
    }
    
    printf("publish: %s, %s, %s\n", argv[0], argv[1], argv[2]);
    
    nsqWriter *writer;
    struct ev_loop *loop;
    void *ctx = NULL;
    
    loop = ev_default_loop(0);
    writer = new_nsq_writer(loop, argv[2], NULL, ctx);

#ifdef NSQD_STANDALONE
    nsq_writer_connect_to_nsqd(writer, argv[1], 4150);
#else
    nsq_writer_add_nsqlookupd_endpoint(writer, argv[1], 4160);
#endif

    nsq_write_msg_to_nsqd(writer, "Hello, I am in the official libnsq");
    nsq_write_defered_msg_to_nsqd(writer, "This is a defered message", 5);

    const char *body[3] = {
        "Hello, I am nsq client",
        "There are so many libraries developed by c",
        "Yeah, c is very beautiful",
    };
    nsq_write_multiple_msg_to_nsqd(writer, body, 3);

    free_nsq_writer(writer);

    return 0;
}
