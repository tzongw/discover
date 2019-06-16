service gate {
    oneway void set_context(1: string conn_id, 2: string context)
    oneway void unset_context(1: string conn_id, 2: set<string> context)
    oneway void remove_conn(1: string conn_id)
}

service user {
    oneway void login(1: string address, 2: string conn_id, 3: map<string, string> params)
    oneway void ping(1: string address, 2: string conn_id, 3: string context)
    oneway void disconnect(1: string address, 2: string conn_id, 3: string context)
}