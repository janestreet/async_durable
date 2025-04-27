open Core
open Async_kernel

module Durable = struct
  type 'a t =
    | Void
    | Building of 'a Deferred.Or_error.t
    | Built of 'a
end

type 'a t =
  { mutable durable : 'a Durable.t
  ; to_create : unit -> 'a Deferred.Or_error.t
  ; to_check_broken : 'a -> bool
  ; mutable is_intact : bool
  ; is_intact_bus : (bool -> unit) Bus.Read_write.t
  ; to_rebuild : ('a -> 'a Deferred.Or_error.t) option
  }

let create ~to_create ~is_broken:to_check_broken ?to_rebuild () =
  { durable = Void
  ; to_create
  ; to_check_broken
  ; is_intact = false
  ; is_intact_bus =
      Bus.create_exn
        Arity1
        ~on_subscription_after_first_write:Allow_and_send_last_value
        ~on_callback_raise:ignore
  ; to_rebuild
  }
;;

let is_broken_and_update_mvar t durable =
  let is_broken = t.to_check_broken durable in
  if Bool.(t.is_intact = is_broken) then Bus.write t.is_intact_bus (not is_broken);
  t.is_intact <- not is_broken;
  is_broken
;;

let create_or_fail ~to_create ~is_broken ?to_rebuild () =
  let t = create ~to_create ~is_broken ?to_rebuild () in
  t.to_create ()
  >>=? fun dur ->
  if is_broken_and_update_mvar t dur
  then return (Or_error.error_string "Initial durable value is broken.")
  else (
    t.durable <- Built dur;
    return (Ok t))
;;

let get_durable t =
  let build building =
    let building =
      Deferred.Or_error.try_with_join ~here:[%here] (fun () ->
        let%map result = building () in
        assert (
          match t.durable with
          | Building _ -> true
          | _ -> false);
        t.durable
        <- (match result with
            (* Errors that show up here will also be returned by [get_durable]. We aren't
             losing any information *)
            | Error _ -> Void
            | Ok durable -> Built durable);
        result)
    in
    t.durable <- Building building;
    building
  in
  match t.durable with
  | Void -> build t.to_create
  | Building durable -> durable
  | Built durable ->
    if is_broken_and_update_mvar t durable
    then
      build
        (match t.to_rebuild with
         | None -> t.to_create
         | Some to_rebuild -> fun () -> to_rebuild durable)
    else return (Ok durable)
;;

let with_ t ~f =
  get_durable t
  >>=? fun durable ->
  if is_broken_and_update_mvar t durable
  then
    return
      (Or_error.error_string
         "Durable value was broken immediately after being created or rebuilt.")
  else f durable
;;

let is_intact_bus t = Bus.read_only t.is_intact_bus

module%test _ = struct
  let go () = Async_kernel_scheduler.Expert.run_cycles_until_no_jobs_remain ()
  let create_counter = ref 0
  let fix_counter = ref 0

  module Fragile = struct
    type t = { mutable is_broken : bool }

    let is_broken t = t.is_broken
    let break t = t.is_broken <- true
    let create_ () = return (Ok { is_broken = false })

    let create () =
      create_counter := !create_counter + 1;
      create_ ()
    ;;

    let fix _t =
      fix_counter := !fix_counter + 1;
      create_ ()
    ;;
  end

  let reset () =
    create_counter := 0;
    fix_counter := 0
  ;;

  let create ~use_fix ~now =
    let to_rebuild = if use_fix then Some Fragile.fix else None in
    if now
    then
      create_or_fail ~to_create:Fragile.create ~is_broken:Fragile.is_broken ?to_rebuild ()
      >>| ok_exn
    else
      return
        (create ~to_create:Fragile.create ~is_broken:Fragile.is_broken ?to_rebuild ())
  ;;

  let poke t = ignore (with_ t ~f:(fun _t -> return (Ok ())))

  let%expect_test _ =
    let pass = ref false in
    (create ~use_fix:false ~now:true
     >>> fun t ->
     match t.durable with
     | Built _ -> pass := true
     | _ -> ());
    go ();
    print_s [%message (!pass : bool)];
    [%expect {| (!pass true) |}]
  ;;

  let build_break_poke ~use_fix ~now =
    reset ();
    (create ~use_fix ~now
     >>> fun t ->
     with_ t ~f:(fun fragile ->
       Fragile.break fragile;
       return (Ok ()))
     >>> fun result ->
     Or_error.ok_exn result;
     poke t;
     poke t;
     poke t);
    go ()
  ;;

  let%expect_test _ =
    build_break_poke ~use_fix:true ~now:true;
    print_s [%message (!create_counter : int) (!fix_counter : int)];
    [%expect {| ((!create_counter 1) (!fix_counter 1)) |}]
  ;;

  let%expect_test _ =
    build_break_poke ~use_fix:true ~now:false;
    print_s [%message (!create_counter : int) (!fix_counter : int)];
    [%expect {| ((!create_counter 1) (!fix_counter 1)) |}]
  ;;

  let%expect_test _ =
    build_break_poke ~use_fix:false ~now:true;
    print_s [%message (!create_counter : int) (!fix_counter : int)];
    [%expect {| ((!create_counter 2) (!fix_counter 0)) |}]
  ;;

  let%expect_test _ =
    build_break_poke ~use_fix:false ~now:false;
    print_s [%message (!create_counter : int) (!fix_counter : int)];
    [%expect {| ((!create_counter 2) (!fix_counter 0)) |}]
  ;;
end
