open Core_kernel
open Async_kernel
open Async_rpc_kernel

module Update = struct
  type ('state, 'update, 'error, 'metadata) t =
    | Attempting_new_connection
    | Connection_success of 'metadata
    | Lost_connection
    | Failed_to_connect of Error.t
    | Rpc_error of 'error
    | Update of 'update
    | State of 'state
end

type ('state, 'update, 'error, 'metadata) t =
  { updates_writer    : ('state, 'update, 'error, 'metadata) Update.t Pipe.Writer.t
  ; connection        : Rpc.Connection.t Durable.t
  ; resubscribe_delay : Time_ns.Span.t
  ; dispatch :
      Rpc.Connection.t ->
      ('state * 'update Pipe.Reader.t * 'metadata, 'error)
        Result.t Or_error.t Deferred.t
  }

let subscription_active t = not (Pipe.is_closed t.updates_writer)

(* [subscription_active] will only be false if the client closes the reader returned by
   [create*] *)
let write t update =
  if subscription_active t
  then Pipe.write_without_pushback t.updates_writer update
;;

let try_to_get_fresh_pipe t =
  write t Attempting_new_connection;
  match%map Durable.with_ t.connection ~f:t.dispatch with
  | Error err -> Error (`Failed_to_connect err)
  | Ok result ->
    match result with
    | Error e -> Error (`Rpc_error e)
    | Ok result -> Ok result
;;

let rec subscribe t =
  if not (subscription_active t)
  then return `Subscription_no_longer_active
  else begin
    match%bind try_to_get_fresh_pipe t with
    | Error err ->
      (match err with
       | `Failed_to_connect e -> write t (Failed_to_connect e);
       | `Rpc_error e -> write t (Rpc_error e));
      let%bind () = Clock_ns.after t.resubscribe_delay in
      subscribe t
    | Ok (state, pipe, id) ->
      write t (Connection_success id);
      write t (State state);
      return (`Ok pipe)
  end
;;

let rec handle_update_pipe t deferred_pipe =
  deferred_pipe
  >>> function
  | `Subscription_no_longer_active -> ()
  | `Ok pipe ->
    (* Pipe.transfer_is determined when [pipe] is closed (as when we lose our connection),
       or when [t.updates_writeer] is closed (as when the client closes the reader
       returned by [create*] *)
    Pipe.transfer pipe t.updates_writer ~f:(fun update -> Update update)
    >>> fun () ->
    write t Lost_connection;
    handle_update_pipe t (subscribe t)
;;

module Expert = struct
  let create_internal connection ~dispatch ~resubscribe_delay =
    let updates_reader, updates_writer = Pipe.create () in
    let resubscribe_delay =
      Time_ns.Span.of_sec (Time.Span.to_sec resubscribe_delay)
    in
    let t =
      { updates_writer
      ; connection
      ; resubscribe_delay
      ; dispatch
      }
    in
    updates_reader, t
  ;;

  let create connection ~dispatch ~resubscribe_delay =
    let updates_reader, t = create_internal connection ~dispatch ~resubscribe_delay in
    handle_update_pipe t (subscribe t);
    updates_reader
  ;;

  let create_or_fail connection ~dispatch ~resubscribe_delay =
    let updates_reader, t = create_internal connection ~dispatch ~resubscribe_delay in
    match%map try_to_get_fresh_pipe t with
    | Error (`Failed_to_connect e) -> Error e
    | Error (`Rpc_error e) -> Ok (Error e)
    | Ok (new_state, fresh_pipe, id) ->
      write t (Connection_success id);
      write t (State new_state);
      handle_update_pipe t (return (`Ok fresh_pipe));
      Ok (Ok updates_reader)
  ;;
end

let create connection rpc ~query ~resubscribe_delay =
  let dispatch conn = Rpc.State_rpc.dispatch rpc conn query in
  Expert.create connection ~dispatch ~resubscribe_delay
;;

let create_or_fail connection rpc ~query ~resubscribe_delay =
  let dispatch conn = Rpc.State_rpc.dispatch rpc conn query in
  Expert.create_or_fail connection ~dispatch ~resubscribe_delay
;;
