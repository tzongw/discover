service gate {
    oneway void set_context(1: string conn_id, 2: map<string, string> context)
    oneway void unset_context(1: string conn_id, 2: set<string> context)
    oneway void remove_conn(1: string conn_id)
    oneway void send_text(1: string conn_id, 2: string message)
    oneway void send_binary(1: string conn_id, 2: binary message)
    oneway void join_group(1: string conn_id, 2: string group)
    oneway void leave_group(1: string conn_id, 2: string group)
    oneway void broadcast_binary(1: string group, 2: set<string> exclude, 3: binary message)
    oneway void broadcast_text(1: string group, 2: set<string> exclude, 3: string message)
}

service user {
    oneway void login(1: string address, 2: string conn_id, 3: map<string, string> params)
    oneway void ping(1: string address, 2: string conn_id, 3: map<string, string>  context)
    oneway void disconnect(1: string address, 2: string conn_id, 3: map<string, string> context)
    oneway void recv_binary(1: string address, 2: string conn_id, 3: map<string, string> context, 4: binary message)
    oneway void recv_text(1: string address, 2: string conn_id, 3: map<string, string> context, 4: string message)
}