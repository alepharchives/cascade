% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

%
% Mechanics for streaming deletes against the DB as
% we are inserting new documents in.
%

-module(cascade_trimmer).
-behaviour(gen_server).
-include("couch_db.hrl").

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-export([start_link/1]).


init([]) ->
    {ok, nil}.

terminate(Reason, _State) ->
    io:format("trimmer dying: ~p~n", [Reason]),
    ok.

handle_call(Arg, From, State) ->
    io:format("Handling a call: ~p ~p ~p~n", [Arg, From, State]),
    {reply, ok, State}.

handle_cast(Arg, State) ->
    io:format("Handling a cast: ~p ~p~n", [Arg, State]),
    {noreply, State}.

handle_info(Arg, State) ->
    io:format("Handling info: ~p ~p~n", [Arg, State]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% A trimmer is responsible for removing documents from a derived
% database. I'm relying quite heavily on the sortedness of inputs.
%
% This function returns a function that is called with each inserted
% key/value doc and uses the sortedness to know which docs to delete.
% It spawns a Pid that does a couch_view:fold to account for the clousure
% aspect of flow control. That's alot of words for what it really does.
%
new_index_trimmer(Db) ->
    Self = self(),
    Trim = spawn_link(fun() -> index_trimmer(Db, Self) end),
    fun
        (wait, continue) ->
            ?OUT("Waiting for exit~n"),
            receive {Trim, exit} -> ok end;
        (exhausted, continue) ->
            ?OUT("Waiting for ready: exhausted~n"),
            receive
                {Trim, ready} ->
                    Trim ! exhausted,
                    continue;
                {Trim, exit} ->
                    ok
            end;
        ({Collation, Value}, continue) ->
            ?OUT("Waiting for ready: with next~n"),
            receive
                {Trim, ready} ->
                    Trim ! {Collation, Value},
                    continue;
                {Trim, exit} ->
                    ok   
            end;
        (_, Resp) ->
            io:format("Repeating resp: ~p~n", [Resp]),
            Resp
    end.

%
% Main loop for the bit that tracks the update Pid and drops docs
% that are no longer valid. I could dip deeper into the view
% mechanics to make this more better I think, but this seems to
% work all right.
%
index_trimmer(Db, Pid) ->
    Lookup = <<"_design/lookup">>,
    Index = <<"index">>,
    AccInit = wait_for_next(Pid),
    {ok, Idx, _Grp} = couch_view:get_map_view(Db, Lookup, Index, nil),
    StreamFun = fun({{Key, DocId}, Value}, _Reds, {Ids, NewInfo}) ->
        io:format("-> ~p~n", [NewInfo]),
        {Action, NewAccVal} = consume_key(Pid, {Key, Value}, NewInfo),
        Ids2 = case Action of
            remove -> [DocId | Ids];
            ignore -> Ids
        end,
        Ids3 = case length(Ids2) >= 1000 of
            true -> purge_ids(Db, Ids2, []);
            false -> Ids2
        end,
        {ok, {Ids3, NewAccVal}}
    end,
    ?OUT("Starting trim fold~n"),
    {ok, {Ids, _}} = couch_view:fold(Idx, nil, fwd, StreamFun, {[], AccInit}),
    purge_ids(Db, Ids, []),
    ?OUT("Sending exit notification.~n"),
    Pid ! {self(), exit}.

%
% Remove a set of DocIds from the database without causing conflicts.
% I think there's a way to make this whole work flow better.
%
purge_ids(_Db, [], []) ->
    [];
purge_ids(Db, [], IdDocs) ->
    Resp = couch_db:update_docs(Db, IdDocs, []),
    io:format("Removing: ~p -> ~p~n", [IdDocs, Resp]),
    [];
purge_ids(Db, [Id | Rest], IdDocs) ->
    {ok, Doc} = couch_db:open_doc(Db, Id, []),
    purge_ids(Db, Rest, [Doc#doc{deleted=true} | IdDocs]).

%
% Wait for the insertion process to send us the next thing it wants. We'll
% delete anything between the last thing that was kept in the derived database
% and the new insertion value. By some fuzzy proof in my brain this is right.
%
wait_for_next(Pid) ->
    ?OUT("Sending ready notification~n"),
    Pid ! {self(), ready},
    ?OUT("Waiting for info.~n"),
    receive Info -> Info end.

%
% Helper function to say what action to take for each doc in the current
% databse.
%
consume_key(_, _, exhausted) ->
    {remove, exhausted};
consume_key(Pid, {Key1, Val1}, {Key2, Val2}) ->
    case compare_keys(Key1, Key2) of
    lesser -> {remove, {Key2, Val2}};
    equal ->
        case Val1 == Val2 of
            true -> {ignore, wait_for_next(Pid)};
            false -> {remove, wait_for_next(Pid)}
        end;
    greater ->
        {remove, wait_for_next(Pid)}
    end.

% Helper function
compare_keys(Key1, Key2) ->
    case couch_view:less_json(Key1, Key2) of
        true -> lesser;
        false ->
            case couch_view:less_json(Key2, Key1) of
                true -> greater;
                false -> equal
            end
    end.

