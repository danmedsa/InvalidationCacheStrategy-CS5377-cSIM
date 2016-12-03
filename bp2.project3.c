//
// Created by Daniel Medina Sada on 11/12/16.
//

#include <stdio.h>
#include <stdlib.h>
#include "csim.h"

#define SIM_TIME 100.0
#define NUM_NODES 1L
#define NUM_SERVERS 1L
#define INVALIDATION_TIME 20.0
#define QUERY_MEAN 10.0
#define UPDATE_TIME 10.0
#define TIME_OUT 5.0
#define T_DELAY 0.5
#define TRANS_TIME 0.1
#define REQUEST 1L
#define REPLY 2L

#define TRACE
#define MSG_ARRAY_SIZE 200
#define MSG_DATA_ITEMS MSG_ARRAY_SIZE/2
#define DB_DATA_ITEMS 1000
#define CACHE_SIZE 100


typedef struct msg *msg_t;
struct msg {
    long type;
    long from;
    long to;
    TIME start_time;
    msg_t link;
    double *dataTime;
};

typedef struct q_msg *q_msg_t;
struct q_msg {
    long type;
    long from;
    long to;
    TIME start_time;
    q_msg_t link;
    double data;
};

typedef struct DataItem *IR;

struct DataItem{
    int data;
    double timeStamp;
};

typedef struct Cache Cache;
struct Cache{
    int data;
    int validity;
    double timeStamp;
};

// TODO: Needs description
msg_t msg_queue;
q_msg_t query_queue;

struct client {
    FACILITY cpu;
    MBOX input;
    Cache *cache;       // Cache Data - Time Stamp - Validity
};

struct server {
    FACILITY cpu;
    MBOX input;
    double *db;
};


struct client nodes[NUM_NODES];
struct server s_nodes[NUM_SERVERS];
FACILITY network[NUM_SERVERS][NUM_NODES];
TABLE resp_tm;

void init();
void proc(long);
void gen_query(long, int);
q_msg_t new_query_msg(long, int);
void server();
void clients(int);
void update_server_data();
void query_manager(int);
void send_msg(msg_t);
void send_query_msg(q_msg_t);
void form_reply(msg_t);
void read_ir(char*, msg_t, long);
void read_updates(char*, msg_t, long);
void decode_query_msg(char*, q_msg_t);
void return_msg(msg_t);
msg_t new_msg(long);
void server_listener();
void server_timer_trigger();
void check_cache(long, int);
double check_db_item_timeStamp(int);
void add_to_IR(int, double);
void add_to_updates(int, double);
void add_to_cache(int, int, double);


EVENT trigger_IR;
EVENT setup;
EVENT IR_timer;
EVENT prepare_msg;
EVENT update_ev;
EVENT client_query;
//EVENT writing;

IR IR_msg;
IR IR_start;


int IR_size;

void sim() {
    create("sim");
    setup = event("setup");
    trigger_IR = event("IR");
    IR_timer = event("Timer");
    prepare_msg = event("Prepare_Msg");
    update_ev = event("Update_Ev");
    client_query = event("Client Query");
//    writing = event("Writing to IR");
    init();
    hold(SIM_TIME);
    report();
}

void init() {
    long i, j;
    char str[24];

    max_facilities((NUM_NODES + NUM_SERVERS) * (NUM_NODES + NUM_SERVERS + NUM_NODES + NUM_SERVERS));
    max_servers((NUM_NODES + NUM_SERVERS) * (NUM_NODES + NUM_SERVERS) + (NUM_NODES + NUM_SERVERS));
    max_mailboxes((NUM_NODES + NUM_SERVERS));
    max_events(2 * (NUM_NODES + NUM_SERVERS) * (NUM_NODES + NUM_SERVERS));

    resp_tm = table("msg rsp tm");
    msg_queue = NIL;
    query_queue = NIL;


    IR_msg = malloc(sizeof(struct DataItem)*DB_DATA_ITEMS); //Alloc Queried Items MAX SIZE 1000
    IR_start = IR_msg;

    IR_size = 0;


    // Initialize facility and mailbox for each node
    for(i = 0; i < NUM_NODES + NUM_SERVERS; i++) {
        sprintf(str, "cpu.%d", i);
        nodes[i].cpu = facility(str);
        sprintf(str, "input.%d", i);
        nodes[i].input = mailbox(str);
    }

    // Initialize network facilities
    for(i = 0; i < NUM_SERVERS; i++) {
        for(j = 0; j < NUM_NODES; j++) {
            sprintf(str, "nt%d.%d", i, j);
            network[i][j] = facility(str);
        }
    }

    // Start each server
    for(i = 0; i < NUM_SERVERS; i++) {
        server();
    }

    // Start each client
    for(i = 0; i < NUM_NODES; i++) {
        clients(i);
        if (i == NUM_NODES - 1){
            printf("-------Set Setup --------\n");

            set(setup);
        }
    }
}

void clients(int i){
//    static double new_cache[3*CACHE_SIZE] = {0};
    int n;
//    printf("Initial Cache:\n");
//    for(n = 0; n < CACHE_SIZE; n++){
//        printf("%.0lf - %.2lf - %.0lf | ", new_cache[n], new_cache[n+1], new_cache[n+2]);
//    }
//    printf("\n");
    nodes[i].cache = malloc(sizeof(Cache)* CACHE_SIZE);
    for(n = 0; n < CACHE_SIZE; n++){
        printf("%.0lf - %.2lf - %.0lf | ", nodes[i].cache[n].data, nodes[i].cache[n].timeStamp, nodes[i].cache[n].validity);
    }
    printf("\n");
    proc(i);
    query_manager(i);
}

void server(){
    create("server");
    static double new_db[DB_DATA_ITEMS] = {};
    s_nodes->db = new_db;
    int i;
    printf("Server Database: \n");
    for (i = 0; i < DB_DATA_ITEMS; ++i) {
        printf("%.2lf ", s_nodes->db[i]);
    }
    printf("\n");
    printf("-------Wait Setup --------\n");

    wait(setup);
    //update_server_data(s_nodes[0].db);        //TODO: Update DB Items
    update_server_data();
    server_timer_trigger();
    server_listener();
}

void server_timer_trigger(){
    create("trigger");
    while(clock < SIM_TIME) {
        timed_wait(IR_timer, INVALIDATION_TIME);
        printf("Timer Triggered\n");
        msg_t msg;
        int n;
        for(n = 0; n < NUM_NODES; n++) {
            msg = new_msg(n);
            set(prepare_msg);       //Msg Ready to send
            printf("-------Set Prepare IR --------\n");

            send_msg(msg);
            printf(" IR Message Sent to Client %d\n", n);
        }
        printf("-------Set Trigger IR --------\n");
        set(trigger_IR);
        for(n = 0; n < 100; n++){
            printf("%d - %.2lf",IR_msg[n].data, IR_msg[n].timeStamp);
        }
        IR_msg +- IR_size;
        IR_size = 0;
    }
}

void server_listener(){
    q_msg_t msg;
    long type;
//    IR data;
//    data.data = -1;
//    data.timeStamp = -1.0;
//    IR_msg[0] = data;

    IR_msg->data = -1;
    IR_msg->timeStamp = -1.0;

    create("Server Listener");
    printf("Server Listener Initialized\n");
    while(clock < SIM_TIME) {
        printf("-------Wait Client Query --------\n");

        wait(client_query);           // query sent event
        printf("%6.3lf - Server Listener Triggered\n", clock);
        receive(nodes[0].input, &msg);
        printf("------- Reset Client Query --------\n");
        clear(client_query);

        decode_query_msg("server received msg", msg);

        type = msg->type;
        // Add data to IR
        double stamp = check_db_item_timeStamp(msg->data);
        add_to_IR(msg->data,stamp);


    }
}

// Main loop for each process
void proc(long n) {
    msg_t msg;
    long s, type;

    create("proc");
    while(clock < SIM_TIME) {
        printf("-------Wait Trigger IR --------\n");
        wait(trigger_IR);
        printf("Client %d Listener Triggered\n", n);
        receive(nodes[n].input, &msg);
        printf("-------Wait Prepare Message --------\n");
        wait(prepare_msg);
        printf("------- Reset Prepare Msg --------\n");
        clear(prepare_msg);

        read_ir("client received IR", msg, n);
        //TODO read_updates("client received Updates", msg, n);

        type = msg->type;
        printf("------- Reset Trigger IR --------\n");
        clear(trigger_IR);

    }
}

// Generate and Respond Query
void query_manager(int i){
    create("Query");
    printf("Query Manager Initialized\n");
    while (clock < SIM_TIME) {
        //Respond Query
//	wait(IR_timer);
        double wait_t = exponential(QUERY_MEAN);
        hold(wait_t);
        printf("Query Triggered - %.2lf\n", wait_t);
        int odds = rand() % 101;      //0% - 100%
        //printf("Rand Number %d\n", odds);
        if (odds > 20) {
            //Hot Data Item     db[0-49]
            int data = rand() % 50;
            printf("Client %d - Checking Cache - Hot Data Item %d - %.2lf\n",i,data,clock);

            //Check Cache
            check_cache(i,data);
            //TODO: wait for Response

        } else {
            //Cold Data Item    db[50-999]
            int data = rand() % 950 + 50;
            printf("Client %d - Checking Cache - Cold Data Item %d - %.2lf\n",i,data,clock);

            //Check Cache
            check_cache(i,data);
            //TODO: Wait for response


        }
    }
}

void check_cache(long i, int data){
    int n;
    int hit = 0;
    for(n = 0; n < CACHE_SIZE; n++){
        if (nodes[i].cache[n].data == data & nodes[i].cache[n].validity == 1){
            // Query Responded
            hit = 1;
            if(data > 50){
                printf("Query found in Cache - Cold Data Item \n");
            }else{
                printf("Query found in Cache - Hot Data Item \n");
            }
        }
    }
    if(hit == 0){
        gen_query(i, data);
        if(data > 50){
            printf("Query Not Found - sent to Server - Cold Data Item \n");
        }else{
            printf("Query Not Found - sent to Server - Hot Data Item \n");
        }
    }
}

// Sends the given message from the defined sender to the defined receiver
void send_msg(msg_t msg) {
    long from, to;

    from = msg->from;
    to = msg->to;
    use(nodes[from].cpu, T_DELAY);		// Message generation delay
    reserve(network[from][to]);
    hold(TRANS_TIME);

    send(nodes[to].input, msg);
    release(network[from][to]);
}

// Creates a new message from "from" to a randomly generated recipient
msg_t new_msg(long to) {
    msg_t msg;

    // No message, create new one
    if(msg_queue == NIL) {
        msg = (msg_t)do_malloc(sizeof(struct msg));

    } else {
        // Take first message in queue
        msg = msg_queue;
        msg_queue = msg_queue->link;
    }

    static double new_dataTime[MSG_ARRAY_SIZE] = {};
    new_dataTime[0] = clock;

    int i;
    for(i = 0; i < IR_size; i++){
        new_dataTime[i + MSG_DATA_ITEMS] = i + 1;
    }

    // Set message properties
    msg->to = to; //i;
    msg->from = 0; //from;
    msg->type = REQUEST;
    msg->start_time = clock;
    msg->dataTime = new_dataTime;
    return msg;
}

//Generate Query
void gen_query(long client, int data_item){
    create("Query Generation");
    // New Query MSG
    q_msg_t msg = new_query_msg(client, data_item);
    printf("Query Message Created from: %ld ",msg->from);
    printf("to: %ld ", msg->to);
    printf("With Data: %.2lf\n", msg->data);
    send_query_msg(msg);
    printf("-------Set Client Query --------\n");

    set(client_query);
    terminate();
}

//Generate New Query Message from client to server
q_msg_t new_query_msg(long from, int data_item) {
    q_msg_t msg;

    // No message, create new one
    if(query_queue == NIL) {
        msg = (q_msg_t)do_malloc(sizeof(struct q_msg));
    } else {
        // Take first message in queue
        msg = query_queue;
        query_queue = query_queue->link;
    }


    // Set message properties
    msg->to = 0; //i;
    msg->from = from; //from;
    msg->type = REQUEST;
    msg->start_time = clock;
    msg->data = data_item;
    return msg;
}

// Send from client to Server
void send_query_msg(q_msg_t msg) {
    long from, to;

    from = msg->from;
    to = msg->to;

    printf("MESSAGE FROM: %ld  TO: %ld\n", from, to);

    use(nodes[to].cpu, T_DELAY);		// Message generation delay
    reserve(network[to][from]);
    hold(TRANS_TIME);

//    send(s_nodes[0].input, msg);
    send(nodes[to].input, msg);
   release(network[to][from]);
}

double check_db_item_timeStamp(int data){
    return s_nodes[0].db[data];
}

void add_to_IR(int data, double timeStamp){
    create("addToIR");
    //int i = 0;
    while(IR_msg->timeStamp != -1.0){
        //i++;
        IR_msg++;
    }

    printf("DataItem: %d -", data);
    printf("TimeStamp: %.2lf\n", timeStamp);


    IR_msg->data = data;
    IR_msg->timeStamp = timeStamp;
    IR_size++;

    IR_msg++;
    IR_msg->data = -1;
    IR_msg->timeStamp = -1.0;
    IR_msg--;

    terminate();
}

//void add_to_updates(int data, double timeStamp){
//    create("addToUpdates");
//    //int i = 0;
//    while(update_msg->timeStamp != -1.0){
//        //i++;
//        update_msg++;
//    }
//
//    printf("DataItem: %d -", data);
//    printf("TimeStamp: %.2lf\n", timeStamp);
//
//
//    //update_msg->data = data;
//    //update_msg->timeStamp = timeStamp;
//    //update_msg++;
//
//    update_msg++;
//    update_msg->data = -1;
//    update_msg->timeStamp = -1.0;
//    update_msg--;
//
//    terminate();
//}

void add_to_cache(int client, int data, double timeStamp){
    int i;
    while(nodes[client].cache[i].data != 0){
        if(nodes[client].cache[i].data == data){
            nodes[client].cache[i].timeStamp = timeStamp;
            nodes[client].cache[i].validity = 1;
        }
        i++;
    }
    nodes[client].cache[i].data = data;
    nodes[client].cache[i].timeStamp = timeStamp;
    nodes[client].cache[i].validity = 0;
}

// Logs all messages
void read_ir(char* str, msg_t msg, long n) {
    printf("%6.3f client %2ld: %s - IR: type = %s, from = Server, to = %ld -- IR Message\n",
           clock, n, str, (msg->type == REQUEST) ? "req" : "rep", msg->from);


    IR_msg = IR_msg - IR_size;
    //printf("IR MSG SIZE: %d\n",);
    while(IR_msg->data != -1){
        printf("-  Data: %.2lf ",IR_msg->data);
        printf("Data item: %.0lf\n", IR_msg->timeStamp);
        int n;
        for(n = 0; n < NUM_NODES; n++) {
            int k;
            printf("--Cache: \n| ");
            add_to_cache(n,IR_msg->data,IR_msg->timeStamp);

            for (k = 0; k < CACHE_SIZE; k++) {
                printf("%d - %.2lf - %d | ", nodes[n].cache[k].data, nodes[n].cache[k].timeStamp, nodes[n].cache[k].validity);
            }
        }
        IR_msg++;

    }

    printf("\n");
}

// Logs all messages
void read_updates(char* str, msg_t msg, long n) {
    printf("%6.3f client %2ld: %s - IR: type = %s, from = Server, to = %ld -- Update Report\n",
           clock, n, str, (msg->type == REQUEST) ? "req" : "rep", msg->from);

    int i;

    //Updates traverse = update_start;

    for (i = 0; i < IR_size; ++i) {
    //    printf("-  Data: %.2lf ",traverse->data);
    //    printf("Data item: %.0lf ", traverse->timeStamp);
    //    traverse++;
    }

    printf("\n");
}


void decode_query_msg(char* str, q_msg_t msg) {
    printf("%6.3f Server: %s - msg: type = %s, from = %ld\n",
           clock, str, (msg->type == REQUEST) ? "req" : "rep", msg->from);

    int i;
    printf("Data Queried: %.0lf\n",msg->data);
}

void update_server_data() {
    create("Update Server Values");
    printf("Update Server Data - Initialized\n");


    while (clock < SIM_TIME) {
        // Update Values in Server
        printf("------- Wait UPDATE TIME and Trigger --------\n");
        timed_wait(update_ev, UPDATE_TIME);
        int odds = rand() % 3;
        if(odds == 1){              // Update Hot Data Items 33%
            int data = rand() % 50;
            s_nodes[0].db[data] = clock;
            printf("Hot Data Item Update - Item %d - %.2f\n",data,clock);
            add_to_IR(data,clock);

        }else{                      // Update Cold Data Items 67%
            int data = rand() % 950 + 50;
            s_nodes[0].db[data] = clock;
            add_to_IR(data,clock);
            printf("Cold Data Item Update - Item %d - %.2f\n",data,clock);
        }

    }
}
