
% Remember to update
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

