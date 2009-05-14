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

-module(cascade_reader).

-export([handle_cascade_req/2]).

-include("cascade.hrl").

new_map_reader(DestPid, SrcView) ->
    spawn_link(fun() -> read_map_view(DestPid, SrcView) end).

new_red_reader(DestPid, SrcView) ->
    spawn_link(fun() -> read_red_view(DestPid, SrcView) end).

fetch_rows(Pid, NumRows) ->
    Pid ! {self(), bufsize, NumRows},
    receive
        {Pid, rows, Rows} ->
            Rows;
    after 1000
        throw({timeout, no_reader_activity})
    end.

read_map_view(DestPid, SrcView) ->
    FoldFun = fun
        (Row, _Reds, {Num, Rows}) when Num > 0 ->
            {ok, {Num-1, [Row | Rows])}};
        (Row, _Reds, {0, []}) ->
            {ok, {wait_for_request(DestPid), [Row]}};
        (Row, _Reds, {0, Rows}) ->
            send_response(DestPid, lists:reverse(Rows))
            {ok, {wait_for_request(DestPid), [Row]}}
    end,
    case couch_view:fold(SrcView, nil, fwd, FoldFun, {0, []}) of
        {ok, {_, []}} ->
            ok;
        {ok, {_, Rows}} ->
            send_response(DestPid, lists:reverse(Rows))
    end,
    exit(normal).

read_reduce_view(DestPid, SrcView) ->
    GroupFun = fun({K1, _}, {K2, _}) -> K1 == K2 end,
    FoldFun = fun
        (Key, Value, {Num, Rows}) ->
            {ok, {Num-1, [{Key, Value} | Rows]}};
        (Key, Value, {0, []}) ->
            {ok, {wait_for_request(DestPid), [{Key, Value}]}};
        (Key, Value, {0, Rows}) ->
            send_response(DestPid, lists:reverse(Rows))
            {ok, {wait_for_request(DestPid), [{Key, Value}]}}
    end,
    StartKey = {nil, nil},
    EndKey = {{}, nil},
    case couch_view:fold_reduce(
            SrcView, fwd, StartKey, EndKey, GroupFun, FoldFun, {0, []}
    ) of
        {ok, {_, []}} ->
            ok;
        {ok, {_, Rows}} ->
            send_response(DestPid, lists:reverse(Rows))
    end,
    exit(normal).

wait_for_resquest(Pid) ->
    receive
        {Pid, bufsize, Num} ->
            Num
    after 1000
        exit({timeout, no_writer_activity})
    end.

send_response(Pid, Rows) ->
    Pid ! {self(), rows, Rows}
