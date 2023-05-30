-module(shackle_tcp_server).
-include("shackle_internal.hrl").

-compile(inline).
-compile({inline_size, 512}).

-export([
    start_link/4
]).

-behavior(metal).
-export([
    init/3,
    handle_msg/2,
    terminate/2
]).

-record(state, {
    client           :: client(),
    init_options     :: init_options(),
    ip               :: inet:ip_address() | inet:hostname(),
    name             :: server_name(),
    parent           :: pid(),
    pool_name        :: pool_name(),
    port             :: inet:port_number(),
    reconnect_state  :: undefined | reconnect_state(),
    socket           :: undefined | inet:socket(),
    socket_options   :: [gen_tcp:connect_option()],
    timer_ref        :: undefined | reference()
}).

-type init_opts() :: {pool_name(), client(), client_options()}.
-type state() :: #state {}.

%% public
-spec start_link(server_name(), pool_name(), client(), client_options()) ->
    {ok, pid()}.

start_link(Name, PoolName, Client, ClientOptions) ->
    Args = {PoolName, Client, ClientOptions},
    metal:start_link(?MODULE, Name, Args).

%% metal callbacks
-spec init(server_name(), pid(), init_opts()) ->
    no_return().

init(Name, Parent, Opts) ->
    {PoolName, Client, ClientOptions} = Opts,
    self() ! ?MSG_CONNECT,
    ok = shackle_backlog:new(Name),

    InitOptions = ?LOOKUP(init_options, ClientOptions,
        ?DEFAULT_INIT_OPTS),
    Ip = ?LOOKUP(ip, ClientOptions, ?DEFAULT_IP),
    Port = ?LOOKUP(port, ClientOptions),
    ReconnectState = ?SERVER_UTILS:reconnect_state(ClientOptions),
    SocketOptions = ?LOOKUP(socket_options, ClientOptions,
        ?DEFAULT_SOCKET_OPTS),

    {ok, {#state {
        client = Client,
        init_options = InitOptions,
        ip = Ip,
        name = Name,
        parent = Parent,
        pool_name = PoolName,
        port = Port,
        reconnect_state = ReconnectState,
        socket_options = SocketOptions
    }, undefined}}.

-spec handle_msg(term(), {state(), client_state()}) ->
    {ok, term()}.

handle_msg({_, #cast {} = Cast}, {#state {
        socket = undefined,
        name = Name
    } = State, ClientState}) ->

    ?SERVER_UTILS:reply(Name, {error, no_socket}, Cast),
    {ok, {State, ClientState}};
handle_msg({Request, #cast {
        timeout = Timeout
    } = Cast}, {#state {
        client = Client,
        name = Name,
        pool_name = PoolName,
        socket = Socket
    } = State, ClientState}) ->

    io:format("shackle_tcp_server: Client:handle_request BEFORE. {Request, ClientState}: {~p,~p}~n", [Request, ClientState]),
    try Client:handle_request(Request, ClientState) of
        {ok, ExtRequestId, Data, ClientState2} ->
            case gen_tcp:send(Socket, Data) of
                ok ->
                    io:format("shackle_tcp_server: Client:handle_request  AFTER. {ExtRequestId, Data, ClientState2}: {~p,~p,~p}~n", [Request, Data, ClientState2]),
                    Timeout_setup = fun ({Id, IdCast}) ->
                        Msg = {timeout, Id},
                        TimerRef = erlang:send_after(Timeout, self(), Msg),
                        shackle_queue:add(Id, IdCast, TimerRef)
                    end,
                    case is_list(ExtRequestId) of
                        true ->
                            IdCasts = lists:zip(ExtRequestId, Cast),
                            lists:foreach(Timeout_setup, IdCasts);
                        false ->
                            Timeout_setup({ExtRequestId, Cast})
                    end,
                    {ok, {State, ClientState2}};
                {error, Reason} ->
                    ?WARN(PoolName, "send error: ~p", [Reason]),
                    gen_tcp:close(Socket),
                    ?SERVER_UTILS:reply(Name, {error, socket_closed}, Cast),
                    close(State, ClientState2)
            end
    catch
        ?EXCEPTION(E, R, Stacktrace) ->
            ?WARN(PoolName, "handle_request crash: ~p:~p~n~p~n",
                [E, R, ?GET_STACK(Stacktrace)]),
            ?SERVER_UTILS:reply(Name, {error, client_crash}, Cast),
            {ok, {State, ClientState}}
    end;

handle_msg({Requests, Ids}, {State, ClientState}) ->
    io:format("shackle_tcp_server:handle_msg: {Requests, Ids}: {~p, ~p}~n", [Requests, Ids]),
    {ok, {State, ClientState}};

handle_msg({tcp, Socket, Data}, {#state {
        client = Client,
        name = Name,
        pool_name = PoolName,
        socket = Socket
    } = State, ClientState}) ->

    try Client:handle_data(Data, ClientState) of
        {ok, Replies, ClientState2} ->
            ?SERVER_UTILS:process_responses(Replies, Name),
            {ok, {State, ClientState2}};
        {error, Reason, ClientState2} ->
            ?WARN(PoolName, "handle_data error: ~p", [Reason]),
            gen_tcp:close(Socket),
            close(State, ClientState2)
    catch
        ?EXCEPTION(E, R, Stacktrace) ->
            ?WARN(PoolName, "handle_data crash: ~p:~p~n~p~n",
                [E, R, ?GET_STACK(Stacktrace)]),
            gen_tcp:close(Socket),
            close(State, ClientState)
    end;
handle_msg({timeout, ExtRequestId}, {#state {
        name = Name
    } = State, ClientState}) ->

    case shackle_queue:remove(Name, ExtRequestId) of
        {ok, Cast, _TimerRef} ->
            ?SERVER_UTILS:reply(Name, {error, timeout}, Cast);
        {error, not_found} ->
            ok
    end,
    {ok, {State, ClientState}};
handle_msg({tcp_closed, Socket}, {#state {
        socket = Socket,
        pool_name = PoolName
    } = State, ClientState}) ->

    ?WARN(PoolName, "connection closed", []),
    close(State, ClientState);
handle_msg({tcp_error, Socket, Reason}, {#state {
        socket = Socket,
        pool_name = PoolName
    } = State, ClientState}) ->

    ?WARN(PoolName, "connection error: ~p", [Reason]),
    gen_tcp:close(Socket),
    close(State, ClientState);
handle_msg(?MSG_CONNECT, {#state {
        client = Client,
        init_options = Init,
        ip = Ip,
        pool_name = PoolName,
        port = Port,
        reconnect_state = ReconnectState,
        socket_options = SocketOptions
    } = State, ClientState}) ->

    case connect(PoolName, Ip, Port, SocketOptions) of
        {ok, Socket} ->
            case ?SERVER_UTILS:client(Client, PoolName, Init, inet, Socket) of
                {ok, ClientState2} ->
                    ReconnectState2 =
                        ?SERVER_UTILS:reconnect_state_reset(ReconnectState),

                    {ok, {State#state {
                        reconnect_state = ReconnectState2,
                        socket = Socket
                    }, ClientState2}};
                {error, _Reason, ClientState2} ->
                    gen_tcp:close(Socket),
                    reconnect(State, ClientState2)
            end;
        {error, _Reason} ->
            reconnect(State, ClientState)
    end;
handle_msg(Msg, {#state {
        pool_name = PoolName
    } = State, ClientState}) ->

    ?WARN(PoolName, "unknown msg: ~p", [Msg]),
    {ok, {State, ClientState}}.

-spec terminate(term(), term()) ->
    ok.

terminate(_Reason, {#state {
        client = Client,
        name = Name,
        pool_name = PoolName,
        timer_ref = TimerRef
    }, ClientState}) ->

    ?SERVER_UTILS:cancel_timer(TimerRef),
    try Client:terminate(ClientState)
    catch
        ?EXCEPTION(E, R, Stacktrace) ->
            ?WARN(PoolName, "terminate crash: ~p:~p~n~p~n",
                [E, R, ?GET_STACK(Stacktrace)])
    end,
    ?SERVER_UTILS:reply_all(Name, {error, shutdown}),
    shackle_backlog:delete(Name),
    ok.

%% private
close(#state {name = Name} = State, ClientState) ->
    ?SERVER_UTILS:reply_all(Name, {error, socket_closed}),
    reconnect(State, ClientState).

connect(PoolName, Ip, Port, SocketOptions) ->
    case inet:getaddrs(Ip, inet) of
        {ok, Addrs} ->
            Ip2 = shackle_utils:random_element(Addrs),
            case gen_tcp:connect(Ip2, Port, SocketOptions,
                ?DEFAULT_CONNECT_TIMEOUT) of
                {ok, Socket} ->
                    {ok, Socket};
                {error, Reason} ->
                    ?WARN(PoolName, "connect error: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?WARN(PoolName, "getaddrs error: ~p", [Reason]),
            {error, Reason}
    end.

reconnect(State, undefined) ->
    reconnect_timer(State, undefined);
reconnect(#state {
        client = Client,
        pool_name = PoolName
    } = State, ClientState) ->

    try Client:terminate(ClientState)
    catch
        ?EXCEPTION(E, R, Stacktrace) ->
            ?WARN(PoolName, "terminate crash: ~p:~p~n~p~n",
                [E, R, ?GET_STACK(Stacktrace)])
    end,
    reconnect_timer(State, ClientState).

reconnect_timer(#state {
        reconnect_state = undefined
    } = State, ClientState) ->

    {ok, {State#state {
        socket = undefined
    }, ClientState}};
reconnect_timer(#state {
        reconnect_state = ReconnectState
    } = State, ClientState)  ->

    ReconnectState2 = shackle_backoff:timeout(ReconnectState),
    #reconnect_state {current = Current} = ReconnectState2,
    TimerRef = erlang:send_after(Current, self(), ?MSG_CONNECT),

    {ok, {State#state {
        reconnect_state = ReconnectState2,
        socket = undefined,
        timer_ref = TimerRef
    }, ClientState}}.
