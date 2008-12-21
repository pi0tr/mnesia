-module(mnesia_test).
-compile(export_all).
-include_lib("stdlib/include/qlc.hrl").

-record(artist,
              {name,
                genre,
                instrument}).
                
-record(song,
              {title,
               date,
               composer}).
               
-record(album,
              {title,
                artist,
                year}).
                
-record(appears_on,
              {song,
              album}).
              
-record(store,
              {album,
              quantity,
              price}).
              
-record(item,
              {album,
              quantity,
              price}).
              
-record(order,
              {number,
              date,
              address,
              payment_type,
              items}).
             

init() ->
  mnesia:create_schema([node()]),
  mnesia:start(),
  mnesia:create_table(artist, 
      [{attributes, record_info(fields, artist)} ]),
  mnesia:create_table(song, 
      [{attributes, record_info(fields, song)}]),
  mnesia:create_table(album, 
      [{attributes, record_info(fields, album)}]),
  mnesia:create_table(appears_on, 
      [{type, bag}, {attributes, record_info(fields, appears_on)}]),
  mnesia:create_table(store, 
      [{attributes, record_info(fields, store)}]),
  mnesia:create_table(order, 
      [{attributes, record_info(fields, order)}]),
  mnesia:stop().

tables() ->
  [artist, song, album, appears_on, store, order].

change_storage_type(Storage) ->
  lists:foreach(fun(X) -> mnesia:change_table_copy_type(X, node(), Storage) end, 
                      tables() ).

reset_data() ->
  {ok, Data} = file:consult("data.txt"),
  F = fun() ->
    lists:foreach(fun mnesia:clear_table/1, tables()),
    lists:foreach(fun mnesia:write/1, Data),
    lists:foreach(fun(X) -> mnesia:delete({order, X}) end, mnesia:all_keys(order) )
  end,
  mnesia:transaction(F).

exec(Q) ->
  F = fun() ->
    qlc:e(Q)
  end,
  mnesia:transaction(F).

artists(genre, G) ->
  Q = qlc:q( [A || A <- mnesia:table(artist), 
                    A#artist.genre =:= G] ),
  exec(Q);
artists(instrument, I) ->
  exec( qlc:q( [A || A <- mnesia:table(artist), 
                    A#artist.instrument =:= I] ) ).
  
albums(song, S) ->
  Q = qlc:q( [{B#album.title, B#album.year} 
            || A <- mnesia:table(appears_on), B <- mnesia:table(album),
            A#appears_on.album =:= B#album.title, 
            A#appears_on.song =:= S] ),
  exec(Q).
  
store() ->
  {atomic, Store} = exec(
    qlc:q( [{A, S#store.quantity, S#store.price} || S <- mnesia:table(store),
                A <- mnesia:table(album),
                S#store.album =:= A#album.title] )
  ),
  Store.
  
update_store(quantity, Album, Change) ->
  F = fun() ->
    [S] = mnesia:read(store, Album, write),
    Quantity = S#store.quantity + Change,
    if 
      Quantity >= 0 ->
        New = S#store{quantity = Quantity},
        mnesia:write(New);
      true ->
        mnesia:abort({quantity, Quantity, Album})
    end
  end,
  mnesia:transaction(F);
update_store(price, Album, NewPrice) ->
  F = fun() ->
    [S] = mnesia:read(store, Album, write),
    New = S#store{price = NewPrice},
    mnesia:write(New)
  end,
  mnesia:transaction(F).
  
price(Album) ->
  {atomic, [Price]} = exec(
    qlc:q( [S#store.price || S <- mnesia:table(store),
                S#store.album =:= Album] )
  ),
  Price.
  
create_order(Number, Address, PaymentType, Items) ->
  F = fun() ->
    NewOrder = #order{number = Number, 
                          date = erlang:date(),
                          address = Address,
                          payment_type = PaymentType,
                          items = create_order_items(Items) 
                      },
    mnesia:write(NewOrder),
    update_store_quantity(Items)
  end,
  mnesia:transaction(F).
  
create_order_items([Item|Rest]) ->
  {Album, Quantity} = Item,
  NewItem = #item{album = Album, 
                          quantity = Quantity, 
                          price = Quantity * price(Album) 
              },
  [NewItem|create_order_items(Rest)];
create_order_items([]) -> [].

update_store_quantity(Items) ->
  F = fun(X) ->
    {Album, Quantity} = X,
    {atomic, ok} = update_store(quantity, Album, -Quantity)
  end,
  lists:foreach(F, Items).

orders() ->
  {atomic, Orders} = exec( qlc:q( [O || O <- mnesia:table(order) ] )),
  Orders.
  

 
