-module(shackle).
-include("shackle_internal.hrl").

-compile(inline).
-compile({inline_size, 512}).

%% public
-export([
    call/2,
    call/3,
    cast/2,
    cast/3,
    cast/4,
    receive_response/1
]).

%% public
-spec call(pool_name(), term()) ->
    term() | {error, term()}.

call(PoolName, Request) ->
    call(PoolName, Request, ?DEFAULT_TIMEOUT).

-spec call(atom(), term(), timeout()) ->
    term() | {error, atom()}.

call(PoolName, Request, Timeout) ->
    case cast(PoolName, Request, self(), Timeout) of
        {ok, RequestId} ->
            receive_response(RequestId);
        {error, Reason} ->
            {error, Reason}
    end.

-spec cast(pool_name(), term()) ->
    {ok, request_id()} | {error, atom()}.

cast(PoolName, Request) ->
    cast(PoolName, Request, self()).

-spec cast(pool_name(), term(), pid()) ->
    {ok, request_id()} | {error, atom()}.

cast(PoolName, Request, Pid) ->
    cast(PoolName, Request, Pid, ?DEFAULT_TIMEOUT).

-spec cast(pool_name(), term(), pid(), timeout()) ->
    {ok, request_id()} | {error, atom()}.

cast(PoolName, Request, Pid, Timeout) when is_list(Request) ->
    case cast_many(PoolName, Request, Pid, Timeout) of
        {ok, RequestIds} ->
            receive_response_many(RequestIds);
        {error, Reason} ->
            {error, Reason}
    end;
cast(PoolName, Request, Pid, Timeout) ->
    Timestamp = os:timestamp(),
    case shackle_pool:server(PoolName) of
        {ok, Client, Server} ->
            RequestId = {Server, make_ref()},
            Server ! {Request, #cast {
                client = Client,
                pid = Pid,
                request_id = RequestId,
                timeout = Timeout,
                timestamp = Timestamp
            }},
            {ok, RequestId};
        {error, Reason} ->
            {error, Reason}
    end.

cast_many(PoolName, Requests, Pid, Timeout) ->
    Timestamp = os:timestamp(),
    case shackle_pool:server(PoolName) of
        {ok, Client, Server} ->
            {Rs, Casts, Ids} = lists:foldl(
                fun (X, {Rs, Casts, Ids}) ->
                    Id = {Server, make_ref()},
                    Cast = #cast {
                        client = Client,
                        pid = Pid,
                        request_id = Id,
                        timeout = Timeout,
                        timestamp = Timestamp
                    },
                    {[X|Rs], [Cast|Casts], [Id|Ids]}
                end,
                {[], [], []}, Requests),
            Server ! {lists:reverse(Rs), lists:reverse(Casts)},
            {ok, lists:reverse(Ids)};
        {error, Reason} ->
            {error, Reason}
    end.

-spec receive_response(request_id()) ->
    term() | {error, term()}.

receive_response(RequestId) ->
    receive
        {#cast {request_id = RequestId}, Reply} ->
            Reply
    end.

receive_response_many(RequestIds) ->
    receive_response_many(RequestIds, []).

receive_response_many([], Acc) ->
    {ok, lists:reverse(Acc)};
receive_response_many(RequestIds, Acc) ->
    receive
        {#cast {request_id = RequestId}, Reply} ->
            case lists:member(RequestId, RequestIds) of
                true ->
                    receive_response_many(lists:delete(RequestId, RequestIds), [Reply|Acc]);
                false ->
                    ok
            end
    end.
