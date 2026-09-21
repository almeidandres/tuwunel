use std::collections::{BTreeMap, HashMap};

use futures::{StreamExt, TryStreamExt, pin_mut};
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	events::{TimelineEventType, receipt::ReceiptThread},
	uint,
};
use serde::Deserialize;
use tuwunel_core::{
	Err, Result, err, implement,
	matrix::{
		event::Event,
		pdu::{Content, PduBuilder, PduCount, PduEvent, PduId, RawPduId},
	},
	utils::hash::sha256,
};

use super::ExtractRelatesToEventId;
use crate::rooms::read_receipt::PrivateRead;

#[derive(Debug, Deserialize)]
pub struct BatchEvent {
	pub event_id: OwnedEventId,
	pub sender: OwnedUserId,
	#[serde(rename = "type")]
	pub event_type: TimelineEventType,
	pub origin_server_ts: MilliSecondsSinceUnixEpoch,
	#[serde(default)]
	pub room_id: Option<OwnedRoomId>,
	pub content: Content,
	#[serde(default)]
	pub unsigned: Option<BTreeMap<String, serde_json::Value>>,
	#[serde(default)]
	pub state_key: Option<String>,
	#[serde(default)]
	pub redacts: Option<OwnedEventId>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchOptions<'a> {
	pub forward: bool,
	pub forward_if_no_messages: bool,
	pub send_notification: bool,
	pub mark_read_by: Option<&'a UserId>,
}

#[implement(super::Service)]
#[expect(clippy::too_many_lines)]
pub async fn append_batch(
	&self,
	room_id: &RoomId,
	events: Vec<BatchEvent>,
	options: BatchOptions<'_>,
) -> Result<Vec<OwnedEventId>> {
	if events.is_empty() {
		return Ok(Vec::new());
	}

	if options.forward && options.forward_if_no_messages {
		return Err!(Request(InvalidParam(
			"forward and forward_if_no_messages are mutually exclusive"
		)));
	}
	validate_events(room_id, &events)?;
	let batch_digest = batch_digest(&events, options);

	let batch_lock = self.mutex_batch.lock().await;
	let state_lock = self.services.state.mutex.lock(room_id).await;
	if self
		.exact_replay(room_id, &events, options)
		.await?
		.is_some()
	{
		return Ok(events
			.into_iter()
			.map(|event| event.event_id)
			.collect());
	}

	let first_message = self.first_message_event(room_id).await?;
	let forward = options.forward || (options.forward_if_no_messages && first_message.is_none());
	let current_state = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await?;
	let (mut prev_events, event_state) = if forward {
		let prev_events = self
			.services
			.state
			.get_forward_extremities(room_id)
			.take(20)
			.map(ToOwned::to_owned)
			.collect()
			.await;
		(prev_events, current_state)
	} else {
		let anchor = first_message.ok_or_else(|| {
			err!(Request(InvalidParam("Cannot prepend into a room without an existing message.")))
		})?;
		let anchor_state = self
			.services
			.state
			.pdu_shortstatehash(&anchor.event_id)
			.await?;
		if anchor_state != current_state {
			return Err!(Request(InvalidParam(
				"Historical state changes are not supported yet."
			)));
		}
		(anchor.prev_events, anchor_state)
	};

	let mut planned = Vec::with_capacity(events.len());
	for event in &events {
		let builder = PduBuilder {
			event_type: event.event_type.clone(),
			content: event.content.clone(),
			unsigned: event.unsigned.clone(),
			state_key: None,
			redacts: event.redacts.clone(),
			timestamp: Some(event.origin_server_ts),
		};
		let depth = planned
			.last()
			.map(|(pdu, _): &(PduEvent, _)| pdu.depth.saturating_add(uint!(1)));
		let (pdu, json) = self
			.create_hash_and_sign_event_with_prev(
				builder,
				&event.sender,
				room_id,
				&state_lock,
				Some(prev_events),
				Some(event.event_id.clone()),
				depth,
				true,
			)
			.await?;

		prev_events = std::iter::once(pdu.event_id.clone()).collect();
		planned.push((pdu, json));
	}

	let insert_lock = self.mutex_insert.lock(room_id).await;
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await?;
	let permits: Vec<_> = planned
		.iter()
		.map(|_| self.services.globals.next_count())
		.collect();
	let pdu_ids: Vec<RawPduId> = (0..permits.len())
		.map(|index| -> Result<_> {
			let count = if forward {
				PduCount::Normal(*permits[index])
			} else {
				let permit = permits
					.iter()
					.rev()
					.nth(index)
					.expect("index derived from permit length");
				let count: i64 = (**permit).try_into()?;
				PduCount::Backfilled(
					count
						.checked_neg()
						.ok_or_else(|| err!("timeline count cannot be negated"))?,
				)
			};
			Ok(PduId { shortroomid, count }.into())
		})
		.collect::<Result<_>>()?;

	let read_count = if options.mark_read_by.is_none() {
		None
	} else if forward {
		Some(
			pdu_ids
				.last()
				.expect("non-empty batch")
				.pdu_count(),
		)
	} else {
		let event_ids: Vec<_> = self
			.services
			.state
			.get_forward_extremities(room_id)
			.map(ToOwned::to_owned)
			.collect()
			.await;
		let mut latest: Option<PduCount> = None;
		for event_id in event_ids {
			let count = self.get_pdu_count(&event_id).await?;
			if matches!(count, PduCount::Normal(_)) {
				latest = Some(latest.map_or(count, |current| current.max(count)));
			}
		}
		latest
	};

	let counts: HashMap<_, _> = planned
		.iter()
		.zip(&pdu_ids)
		.map(|((pdu, _), pdu_id)| (pdu.event_id.clone(), pdu_id.pdu_count()))
		.collect();
	let mut short_permits = Vec::new();
	let mut shorteventids = Vec::with_capacity(planned.len());
	for (pdu, _) in &planned {
		match self
			.services
			.short
			.get_shorteventid(&pdu.event_id)
			.await
		{
			| Ok(shorteventid) => shorteventids.push((shorteventid, false)),
			| Err(error) if error.is_not_found() => {
				let permit = self.services.globals.next_count();
				shorteventids.push((*permit, true));
				short_permits.push(permit);
			},
			| Err(error) => return Err(error),
		}
	}

	let mut txn = self.db.db.txn();
	for (((pdu, json), pdu_id), (shorteventid, is_new)) in
		planned.iter().zip(&pdu_ids).zip(shorteventids)
	{
		if is_new {
			self.services
				.short
				.stage_shorteventid(&mut txn, &pdu.event_id, shorteventid);
		}
		txn.put_raw(&self.db.eventid_bridgebatch, pdu.event_id.as_bytes(), batch_digest);
		self.services
			.state
			.stage_event_state(&mut txn, shorteventid, event_state);
		self.stage_pdu_json(&mut txn, pdu_id, pdu, json);

		if let Ok(content) = pdu.get_content::<ExtractRelatesToEventId>() {
			let target = match counts.get(&content.relates_to.event_id) {
				| Some(count) => Some(*count),
				| None => self
					.get_pdu_count(&content.relates_to.event_id)
					.await
					.ok(),
			};
			if let Some(target) = target {
				self.services
					.pdu_metadata
					.stage_relation(&mut txn, pdu_id.pdu_count(), target);
			}
		}
	}

	if let (Some(user_id), Some(PduCount::Normal(count))) = (options.mark_read_by, read_count) {
		self.services
			.read_receipt
			.stage_private_read(&mut txn, PrivateRead {
				room_id,
				user_id,
				count,
				ts: MilliSecondsSinceUnixEpoch::now(),
				thread: &ReceiptThread::Unthreaded,
				announce: false,
			})
			.await;
	}

	if forward {
		let final_event_id = &planned
			.last()
			.expect("non-empty batch")
			.0
			.event_id;
		self.services
			.state
			.stage_forward_extremities(
				&mut txn,
				room_id,
				std::iter::once(final_event_id.as_ref()),
			)
			.await;
	}
	txn.try_execute()?;

	drop(short_permits);
	drop(permits);
	drop(insert_lock);
	drop(state_lock);
	drop(batch_lock);

	if forward && options.send_notification {
		for ((pdu, _), pdu_id) in planned.iter().zip(&pdu_ids) {
			self.services
				.pusher
				.append_pdu(*pdu_id, pdu)
				.await?;
		}
	}

	Ok(planned
		.into_iter()
		.map(|(pdu, _)| pdu.event_id)
		.collect())
}

#[implement(super::Service)]
async fn first_message_event(&self, room_id: &RoomId) -> Result<Option<PduEvent>> {
	let pdus = self.pdus(None, room_id, None);
	pin_mut!(pdus);
	while let Some((_, pdu)) = pdus.try_next().await? {
		if pdu.state_key.is_none() {
			return Ok(Some(pdu));
		}
	}

	Ok(None)
}

#[implement(super::Service)]
async fn exact_replay(
	&self,
	room_id: &RoomId,
	events: &[BatchEvent],
	options: BatchOptions<'_>,
) -> Result<Option<PduCount>> {
	let expected_digest = batch_digest(events, options);
	let mut existing = Vec::with_capacity(events.len());
	for event in events {
		match self.get_pdu(&event.event_id).await {
			| Ok(pdu) if same_event(room_id, &pdu, event) => {
				let stored_digest = self
					.db
					.eventid_bridgebatch
					.get(event.event_id.as_bytes())
					.await?;
				if stored_digest.as_ref() != expected_digest {
					return Err!(Request(InvalidParam(
						"Existing events belong to a different batch."
					)));
				}
				existing.push((pdu, self.get_pdu_count(&event.event_id).await?));
			},
			| Ok(_) => {
				return Err!(Request(InvalidParam("Event ID already has different content.")));
			},
			| Err(error) if error.is_not_found() => {},
			| Err(error) => return Err(error),
		}
	}

	if !existing.is_empty() && existing.len() != events.len() {
		return Err!(Request(InvalidParam("Only part of the batch already exists.")));
	}
	if existing.is_empty() {
		return Ok(None);
	}

	let ordered = existing.windows(2).all(|pair| {
		pair[1].1 > pair[0].1
			&& pair[1]
				.0
				.prev_events
				.iter()
				.any(|event_id| event_id == &pair[0].0.event_id)
	});
	let direction_matches = match existing[0].1 {
		| PduCount::Normal(_) => options.forward || options.forward_if_no_messages,
		| PduCount::Backfilled(_) => !options.forward,
	};
	if !ordered || !direction_matches {
		return Err!(Request(InvalidParam(
			"Existing events do not match the requested batch order or direction."
		)));
	}

	Ok(Some(existing.last().expect("non-empty batch").1))
}

fn validate_events(room_id: &RoomId, events: &[BatchEvent]) -> Result {
	let mut ids = std::collections::HashSet::with_capacity(events.len());
	for event in events {
		if event.state_key.is_some() {
			return Err!(Request(InvalidParam("State events are not supported.")));
		}
		if !matches!(
			event.event_type,
			TimelineEventType::RoomEncrypted | TimelineEventType::Reaction
		) {
			return Err!(Request(InvalidParam("Unsupported bridge history event type.")));
		}
		if event
			.room_id
			.as_deref()
			.is_some_and(|id| id != room_id)
		{
			return Err!(Request(InvalidParam("Event room_id does not match the path.")));
		}
		if !ids.insert(&event.event_id) {
			return Err!(Request(InvalidParam("Duplicate event_id in batch.")));
		}
	}

	Ok(())
}

fn batch_digest(events: &[BatchEvent], options: BatchOptions<'_>) -> sha256::Digest {
	let flags = [
		u8::from(options.forward),
		u8::from(options.forward_if_no_messages),
		u8::from(options.send_notification),
	];
	sha256::delimited(
		std::iter::once(flags.as_slice())
			.chain(
				events
					.iter()
					.map(|event| event.event_id.as_bytes()),
			)
			.chain(
				options
					.mark_read_by
					.into_iter()
					.map(UserId::as_bytes),
			),
	)
}

fn same_event(room_id: &RoomId, pdu: &PduEvent, event: &BatchEvent) -> bool {
	let stored_unsigned = pdu
		.unsigned()
		.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw.get()).ok());
	let supplied_unsigned = event
		.unsigned
		.as_ref()
		.and_then(|unsigned| serde_json::to_value(unsigned).ok());

	pdu.room_id == room_id
		&& pdu.sender == event.sender
		&& pdu.kind == event.event_type
		&& pdu.origin_server_ts == event.origin_server_ts.get()
		&& pdu.content.json().get() == event.content.json().get()
		&& pdu.state_key.is_none()
		&& pdu.redacts == event.redacts
		&& stored_unsigned == supplied_unsigned
}
