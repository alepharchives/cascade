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

-module(cascade).

-export([handle_cascade_req/2]).

% I copy this locally for now. Remember to update.
-include("couch_db.hrl").

-define(PL, proplists).
-define(OUT(V), io:format(V, [])).

-record(stage, {
    dbname,
    language,
    map,
    reduce=nil,
    source=nil,
    depth=0,
    sig=nil
}).

%
% Called from CouchDB's url handling. I need to update this
% now that I know about _design handlers.
%
handle_cascade_req(#httpd{method='GET', path_parts=
        [_DbName, <<"_cascade">>, DocId, <<"_stage">>, StageBin]} = Req, Db) ->
    Stage = list_to_integer(binary_to_list(StageBin)),
    DesignId = <<"_design/", DocId/binary>>,
    Stages = open_stages(Db, DesignId),
    build_stages(Db, DesignId, Stages),
    output_stage(Req, lists:nth(Stage + 1, Stages));
handle_cascade_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "GET,HEAD").

output_stage(Req, Stage) ->
    Db = open_db(Stage),
    couch_httpd_view:design_doc_view(
        Req, Db, <<"cascade">>, <<"cascade">>, nil
    ).

%
% The main loop. This is rather ungood because I'm not protecting against
% multiple clients yet. The pattern from couchdb_view.erl is where I'm
% heading.
%
build_stages(PrevDb, PrevStage, []) ->
    write_design_doc(PrevDb, PrevStage);
build_stages(PrevDb, PrevStage, [NextStage|Rest]) ->
    {Type, SrcView, SrcGrp} = open_stage(PrevDb, PrevStage),
    TgtDb = open_db(NextStage),
    ok = case Type of
        map -> build_map_stage(SrcView, SrcGrp, TgtDb);
        reduce -> build_reduce_stage(SrcView, SrcGrp, TgtDb)
    end,
    build_stages(TgtDb, NextStage, Rest).

%
% Generate a new database from the output of a map-only view.
%
build_map_stage(SrcView, _SrcGrp, TgtDb) ->
    Trim = new_index_trimmer(TgtDb),
    CopyFun = fun({{Key, DocId}, Value}, _Reds, PrevResp) ->
        NextResp = Trim({[Key, DocId], Value}, PrevResp),
        io:format("Next resp: ~p~n", [NextResp]),
        Fields = {[
            {<<"key">>, Key},
            {<<"id">>, DocId},
            {<<"value">>, Value}
        ]},
        Doc = #doc{id=?l2b(signature(Fields)), body=Fields},
        catch couch_db:update_doc(TgtDb, Doc, []),
        {ok, NextResp}
    end,
    {ok, Resp} = couch_view:fold(SrcView, nil, fwd, CopyFun, continue),
    Resp2 = Trim(exhausted, Resp),
    Trim(wait, Resp2).

%
% Generate a new database from a view with a Reduce view. There's
% currently no option for a group_level setting.
%
build_reduce_stage(SrcView, _SrcGrp, TgtDb) ->
    Trim = new_index_trimmer(TgtDb),
    GroupFun = fun({K1, _}, {K2, _}) -> K1 == K2 end,
    CopyFun = fun(Key, Value, PrevResp) ->
        NextResp = Trim({Key, Value}, PrevResp),
        io:format("Next resp: ~p~n", [NextResp]),
        Fields = {[
            {<<"key">>, Key},
            {<<"value">>, Value}
        ]},
        Doc = #doc{id=?l2b(signature(Fields)), body=Fields},
        catch couch_db:update_doc(TgtDb, Doc, []),
        {ok, NextResp}
    end,
    {ok, Resp} = couch_view:fold_reduce(
        SrcView, fwd, {nil, nil}, {{}, nil}, GroupFun, CopyFun, continue
    ),
    Resp2 = Trim(exhausted, Resp),
    Trim(wait, Resp2).

%
% Grab the source view which we are copying to the derivative
% database.
%
open_stage(Db, Stage) when is_binary(Stage) -> % root view
    open_stage(Db, Stage, <<"cascade">>);
open_stage(Db, Stage) ->
    Doc = write_design_doc(Db, Stage),
    open_stage(Db, Doc#doc.id, <<"cascade">>).

open_stage(Db, DocId, ViewId) ->
    case couch_view:get_map_view(Db, DocId, ViewId, nil) of
        {ok, View, Group} -> {map, View, Group};
        {not_found, Reason} ->
            case couch_view:get_reduce_view(Db, DocId, ViewId, nil) of
                {ok, ReduceView, Group} -> {reduce, ReduceView, Group};
                _ -> throw({not_found, Reason})
            end
    end.

%
% Open the databse for a derivative database. Make sure we have the
% appropriate infrastructure for our single scan updates.
%
open_db(Stage) ->
    RawDbName = io_lib:format("~s-~s", [Stage#stage.dbname, Stage#stage.sig]),
    DbName = ?l2b(lists:flatten(RawDbName)),
    Options = [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}],
    Db3 = case (catch couch_db:create(DbName, Options)) of
        {ok, Db} -> Db;
        Error ->
            case (catch couch_db:open(DbName, Options)) of
                {ok, Db2} -> Db2;
                _ -> throw(Error)
            end
    end,
    write_lookup_design_doc(Db3),
    Db3.

%
% In derived databases we use an index stream to know which rows we
% need to delete. Detects whether we're a map or reduce derivative.
%
write_lookup_design_doc(Db) ->
    IndexFun = <<"function(doc) { "
        "if(doc.id) emit([doc.key, doc.id], doc.value); "
        "else emit(doc.key, doc.value); "
        "}">>,
    Fields = {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[{<<"index">>, {[{<<"map">>, IndexFun}]} }]} }
    ]},
    Doc = #doc{id = <<"_design/lookup">>, body=Fields},
    catch couch_db:update_doc(Db, Doc, []).

%
% Write the _design doc that will be used to read from the derived database.
% Uses information from the source _design doc's cascade attribute.
%
write_design_doc(Db, Stage) ->
    DesDoc = stage_to_design_doc(Stage),
    DesDoc2 = case couch_db:open_doc(Db, DesDoc#doc.id, []) of
        {ok, Doc} -> DesDoc#doc{revs=Doc#doc.revs};
        {not_found, missing} -> DesDoc;
        Error -> throw(Error)
    end,
    case couch_db:update_doc(Db, DesDoc2, []) of
        {ok, _} -> ok;
        Error2 -> throw(Error2)
    end,
    DesDoc2.

%
% Build a design doc that can be used to read a stage of the cascaded
% workflow. Waterfal reminded me too much of that song from the nineties
% by TLC.
%
stage_to_design_doc(Stage) ->
    View = case Stage#stage.reduce of
        nil ->
            {[
                {<<"map">>, Stage#stage.map}
            ]};
        _ ->
            {[
                {<<"map">>, Stage#stage.map},
                {<<"reduce">>, Stage#stage.reduce}
            ]}
    end,
    Fields = {[
        {<<"language">>, Stage#stage.language},
        {<<"views">>, {[{<<"cascade">>, View}]}}
    ]},
    #doc{id = <<"_design/cascade">>, body=Fields}.

%
% Read the stage definitions from a _design doc in the source
% database. Builds the #stage{} records needed to create the
% series of derived databases.
%
open_stages(Db, DesignId) ->
    DesDoc = case couch_db:open_doc(Db, DesignId, []) of
        {ok, Doc} -> Doc;
        Error -> throw(Error)
    end,
    #doc{body={Fields}} = DesDoc,
    RawStages = ?PL:get_value(<<"cascade">>, Fields, []),
    Stages = lists:foldl(fun({RawStage}, Acc) ->
        NewStage = #stage{
            dbname=couch_db:name(Db),
            language=?PL:get_value(<<"language">>, RawStage, <<"javascript">>),
            map=?PL:get_value(<<"map">>, RawStage),
            reduce=?PL:get_value(<<"reduce">>, RawStage, nil),
            depth=length(Acc)
        },
        NewStage2 = case Acc of
            [] -> NewStage;
            [Prev|_] -> NewStage#stage{source=Prev#stage.sig}
        end,
        NewStage3 = NewStage2#stage{sig=signature(NewStage)},
        [NewStage3 | Acc]
    end, [], RawStages),
    lists:reverse(Stages).

%
% Generate an MD5 of an arbitrary term.
%
signature(Term) ->
    <<SigInt:128/integer>> = erlang:md5(term_to_binary(Term)),
    string:to_lower(lists:flatten(io_lib:format("~.36B", [SigInt]))).

%
% Mechanics for streaming deletes against the DB as
% we are inserting new documents in.
%

%
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

