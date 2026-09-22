#![cfg(test)]

use std::{
	env::temp_dir,
	fs::{create_dir_all, remove_dir_all, write},
	net::TcpListener,
	process::id as process_id,
	time::Duration,
};

use futures::future::join;
use serde_json::{Value, json, value::to_raw_value};
use tokio::time::{sleep, timeout};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Err, Result, err,
	matrix::pdu::Content,
	ruma::{MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId, UInt, UserId},
};
use tuwunel_service::{
	Services,
	rooms::timeline::{BatchEvent, BatchOptions},
	users::Register,
};

#[test]
fn bridge_batch_is_atomic_ordered_and_idempotent() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let root = temp_dir().join(format!("tuwunel-bridge-batch-{}", process_id()));
	let db_path = root.join("db");
	let appservice_dir = root.join("appservices");
	create_dir_all(&appservice_dir)?;
	write(
		appservice_dir.join("test.yaml"),
		"id: test\nurl: null\nas_token: bridge-appservice-token\nhs_token: \
		 test-hs-token\nsender_localpart: bridgebot\nnamespaces:\n  users:\n    - regex: \
		 '^@bridge_.*:localhost$'\n      exclusive: true\n  aliases: []\n  rooms: []\n",
	)?;
	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.option.extend([
		format!("database_path={db_path:?}"),
		format!("appservice_dir={appservice_dir:?}"),
		"address=[\"127.0.0.1\"]".to_owned(),
		format!("port={port}"),
		"listening=true".to_owned(),
		"allow_federation=false".to_owned(),
		"bridge_batch_send=true".to_owned(),
		"bridge_batch_send_appservices=[\"test\"]".to_owned(),
		"bridge_batch_send_local_senders=[\"@batch:localhost\"]".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");
		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();
			outcome.and(shutdown)
		};
		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;
		outcome
	});

	drop(runtime);
	remove_dir_all(&root).ok();
	result
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;
	let user = UserId::parse_with_server_name("batch", services.globals.server_name())?;
	let token = "bridge-batch-token-00000000000000000000";
	services
		.users
		.full_register(Register {
			user_id: Some(&user),
			password: Some("bridge-batch-password"),
			..Default::default()
		})
		.await?;
	services
		.users
		.create_device(&user, None, (Some(token), None), None, None, None)
		.await?;
	let room = create_room(services, base, token).await?;

	let ids = [event_id("first", services)?, event_id("second", services)?];
	let events = vec![
		batch_event(&room, &user, ids[0].clone(), 1)?,
		batch_event(&room, &user, ids[1].clone(), 2)?,
	];
	let sent = send_batch(base, &room, "bridge-appservice-token", &events, true, Some(&user))
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?
		.get("event_ids")
		.and_then(Value::as_array)
		.ok_or_else(|| err!("batch response omitted event_ids"))?
		.iter()
		.map(|id| {
			id.as_str()
				.ok_or_else(|| err!("batch response contained an invalid event ID"))?
				.try_into()
				.map_err(Into::into)
		})
		.collect::<Result<Vec<OwnedEventId>>>()?;
	if sent != ids {
		return Err!("batch response changed event IDs or order");
	}

	let first_count = services.timeline.get_pdu_count(&ids[0]).await?;
	let second_count = services.timeline.get_pdu_count(&ids[1]).await?;
	if first_count >= second_count {
		return Err!("forward batch timeline order is wrong");
	}
	if services
		.read_receipt
		.private_read_get_count(&room, &user)
		.await?
		.0 != second_count.into_unsigned()
	{
		return Err!("batch read marker was not committed with the timeline");
	}

	let retry = vec![
		batch_event(&room, &user, ids[0].clone(), 1)?,
		batch_event(&room, &user, ids[1].clone(), 2)?,
	];
	services
		.timeline
		.append_batch(&room, retry, BatchOptions {
			forward_if_no_messages: true,
			mark_read_by: Some(&user),
			..Default::default()
		})
		.await?;
	if services.timeline.get_pdu_count(&ids[1]).await? != second_count {
		return Err!("exact retry duplicated or moved an event");
	}
	let subset = vec![batch_event(&room, &user, ids[1].clone(), 2)?];
	if services
		.timeline
		.append_batch(&room, subset, BatchOptions {
			forward_if_no_messages: true,
			mark_read_by: Some(&user),
			..Default::default()
		})
		.await
		.is_ok()
	{
		return Err!("partial retry unexpectedly succeeded");
	}

	let reversed = vec![
		batch_event(&room, &user, ids[1].clone(), 2)?,
		batch_event(&room, &user, ids[0].clone(), 1)?,
	];
	if services
		.timeline
		.append_batch(&room, reversed, BatchOptions {
			forward_if_no_messages: true,
			mark_read_by: Some(&user),
			..Default::default()
		})
		.await
		.is_ok()
	{
		return Err!("reordered retry unexpectedly succeeded");
	}

	let mut conflicting = vec![
		batch_event(&room, &user, ids[0].clone(), 1)?,
		batch_event(&room, &user, ids[1].clone(), 2)?,
	];
	conflicting[0].unsigned = Some([("changed".to_owned(), json!(true))].into());
	if services
		.timeline
		.append_batch(&room, conflicting, BatchOptions {
			forward_if_no_messages: true,
			mark_read_by: Some(&user),
			..Default::default()
		})
		.await
		.is_ok()
	{
		return Err!("conflicting retry unexpectedly succeeded");
	}

	let notified_id = event_id("notified", services)?;
	let historical_sender =
		UserId::parse_with_server_name("bridge_historical", services.globals.server_name())?;
	services
		.timeline
		.append_batch(
			&room,
			vec![batch_event(&room, &historical_sender, notified_id.clone(), 3)?],
			BatchOptions {
				forward: true,
				send_notification: true,
				..Default::default()
			},
		)
		.await?;

	reqwest::Client::new()
		.put(format!("{base}/_matrix/client/v3/rooms/{room}/state/m.room.name"))
		.bearer_auth(token)
		.json(&json!({"name": "State changed after the forward batch"}))
		.send()
		.await?
		.error_for_status()?;

	let old_ids = [
		event_id("oldest", services)?,
		event_id("older", services)?,
		event_id("old-reaction", services)?,
	];
	let old = vec![
		batch_event(&room, &user, old_ids[0].clone(), 0)?,
		batch_event(&room, &user, old_ids[1].clone(), 0)?,
		reaction_event(&room, &user, old_ids[2].clone(), old_ids[0].clone())?,
	];
	services
		.timeline
		.append_batch(&room, old, BatchOptions::default())
		.await?;
	let oldest_count = services
		.timeline
		.get_pdu_count(&old_ids[0])
		.await?;
	let older_count = services
		.timeline
		.get_pdu_count(&old_ids[1])
		.await?;
	let reaction_count = services
		.timeline
		.get_pdu_count(&old_ids[2])
		.await?;
	if !(oldest_count < older_count
		&& older_count < reaction_count
		&& reaction_count < first_count)
	{
		return Err!("prepended batch timeline order is wrong");
	}
	if !services
		.pdu_metadata
		.event_has_relation(&old_ids[0], None, None, None)
		.await
	{
		return Err!("prepended reaction was not indexed");
	}

	if send_batch(base, &room, token, &[], true, None)
		.await?
		.status()
		.is_success()
	{
		return Err!("non-appservice token unexpectedly authorized");
	}

	let marker_before_failure = services
		.read_receipt
		.private_read_get_count(&room, &user)
		.await?;
	let invalid_ids = [event_id("uncommitted", services)?, event_id("invalid", services)?];
	let mut invalid = vec![
		batch_event(&room, &user, invalid_ids[0].clone(), 3)?,
		batch_event(&room, &user, invalid_ids[1].clone(), 4)?,
	];
	invalid[1].event_type = "m.room.message".into();
	if send_batch(base, &room, "bridge-appservice-token", &invalid, false, Some(&user))
		.await?
		.status()
		.is_success()
	{
		return Err!("invalid batch unexpectedly succeeded");
	}
	if services
		.timeline
		.get_pdu(&invalid_ids[0])
		.await
		.is_ok()
	{
		return Err!("failed batch exposed its valid prefix");
	}
	if services
		.read_receipt
		.private_read_get_count(&room, &user)
		.await?
		!= marker_before_failure
	{
		return Err!("failed batch changed the read marker");
	}

	Ok(())
}

async fn send_batch(
	base: &str,
	room_id: &OwnedRoomId,
	token: &str,
	events: &[BatchEvent],
	forward_if_no_messages: bool,
	mark_read_by: Option<&OwnedUserId>,
) -> Result<reqwest::Response> {
	let events: Vec<_> = events
		.iter()
		.map(|event| {
			json!({
				"event_id": event.event_id,
				"sender": event.sender,
				"type": event.event_type,
				"origin_server_ts": event.origin_server_ts,
				"room_id": event.room_id,
				"content": event.content,
				"unsigned": event.unsigned,
				"state_key": event.state_key,
				"redacts": event.redacts,
			})
		})
		.collect();
	let url =
		format!("{base}/_matrix/client/unstable/com.beeper.backfill/rooms/{room_id}/batch_send");
	Ok(reqwest::Client::new()
		.post(url)
		.bearer_auth(token)
		.json(&json!({
			"events": events,
			"forward_if_no_messages": forward_if_no_messages,
			"mark_read_by": mark_read_by,
		}))
		.send()
		.await?)
}

fn batch_event(
	room_id: &OwnedRoomId,
	sender: &OwnedUserId,
	event_id: OwnedEventId,
	timestamp: u64,
) -> Result<BatchEvent> {
	let content: Content = to_raw_value(&json!({
		"algorithm": "m.megolm.v1.aes-sha2",
		"ciphertext": "test",
		"device_id": "TEST",
		"sender_key": "test",
		"session_id": "test"
	}))?
	.into();

	Ok(BatchEvent {
		event_id,
		sender: sender.clone(),
		event_type: "m.room.encrypted".into(),
		origin_server_ts: MilliSecondsSinceUnixEpoch(
			UInt::new(timestamp).ok_or_else(|| err!("timestamp out of range"))?,
		),
		room_id: Some(room_id.clone()),
		content,
		unsigned: None,
		state_key: None,
		redacts: None,
	})
}

fn reaction_event(
	room_id: &OwnedRoomId,
	sender: &OwnedUserId,
	event_id: OwnedEventId,
	target: OwnedEventId,
) -> Result<BatchEvent> {
	let mut event = batch_event(room_id, sender, event_id, 0)?;
	event.event_type = "m.reaction".into();
	event.content = to_raw_value(&json!({
		"m.relates_to": {
			"event_id": target,
			"key": "test",
			"rel_type": "m.annotation"
		}
	}))?
	.into();
	Ok(event)
}

fn event_id(localpart: &str, services: &Services) -> Result<OwnedEventId> {
	Ok(format!("${localpart}:{}", services.globals.server_name()).try_into()?)
}

async fn wait_until_ready(services: &Services, base: &str) -> Result {
	let url = format!("{base}/_matrix/client/versions");
	timeout(Duration::from_secs(10), async {
		loop {
			if services
				.client
				.clients
				.default
				.get(&url)
				.send()
				.await
				.is_ok()
			{
				break;
			}
			sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.map_err(|_| err!("server listener did not become ready"))?;
	Ok(())
}

async fn create_room(services: &Services, base: &str, token: &str) -> Result<OwnedRoomId> {
	let response = services
		.client
		.clients
		.default
		.post(format!("{base}/_matrix/client/v3/createRoom"))
		.bearer_auth(token)
		.json(&json!({}))
		.send()
		.await?
		.error_for_status()?
		.json::<Value>()
		.await?;
	let room_id = response
		.get("room_id")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("createRoom response omitted room_id"))?;
	Ok(room_id.try_into()?)
}
