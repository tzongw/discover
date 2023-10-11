service timer {
    void call_later(1: string key, 2: string service_name, 3: string data, 4: double delay)
    void call_repeat(1: string key, 2: string service_name, 3: string data, 4: double interval)
    void remove_timer(1: string key, 2: string service_name)
    void _delete_timer(1: string key, 2: string service_name)
}

service timeout {
    void timeout(1: string key, 2: string data)
}

service gate {
    oneway void set_context(1: string conn_id, 2: string key 3: string value)
    oneway void unset_context(1: string conn_id, 2: string key 3: string value)
    oneway void remove_conn(1: string conn_id)
    oneway void send_text(1: string conn_id, 2: string message)
    oneway void send_binary(1: string conn_id, 2: binary message)
    oneway void join_group(1: string conn_id, 2: string group)
    oneway void leave_group(1: string conn_id, 2: string group)
    oneway void broadcast_binary(1: string group, 2: set<string> exclude, 3: binary message)
    oneway void broadcast_text(1: string group, 2: set<string> exclude, 3: string message)
}

service user extends timeout {
    oneway void login(1: string address, 2: string conn_id, 3: map<string, string> params)
    oneway void ping(1: string address, 2: string conn_id, 3: map<string, string> context)
    oneway void disconnect(1: string address, 2: string conn_id, 3: map<string, string> context)
    oneway void recv_binary(1: string address, 2: string conn_id, 3: map<string, string> context, 4: binary message)
    oneway void recv_text(1: string address, 2: string conn_id, 3: map<string, string> context, 4: string message)
}