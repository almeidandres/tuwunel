use axum::{
	Json,
	extract::{Path, State},
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as b64};
use futures::StreamExt;
use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId, UInt, api::client::membership::mutual_rooms};
use serde::{Deserialize, Serialize};
use tuwunel_core::{Err, Result, err};
use tuwunel_service::rooms::timeline::{BatchEvent, BatchOptions};

use crate::{ClientIp, Ruma};

#[derive(Debug, Deserialize)]
pub(crate) struct BatchSendRequest {
	#[serde(default)]
	forward_if_no_messages: bool,
	#[serde(default)]
	forward: bool,
	#[serde(default)]
	send_notification: bool,
	#[serde(default)]
	mark_read_by: Option<OwnedUserId>,
	#[serde(default)]
	events: Vec<BatchEvent>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchSendResponse {
	event_ids: Vec<OwnedEventId>,
}

pub(crate) async fn batch_send_route(
	State(services): State<crate::State>,
	Path(room_id): Path<OwnedRoomId>,
	bearer: Option<TypedHeader<Authorization<Bearer>>>,
	Json(body): Json<BatchSendRequest>,
) -> Result<Json<BatchSendResponse>> {
	if !services.config.bridge_batch_send {
		return Err!(Request(NotFound("Bridge batch sending is disabled.")));
	}

	let token = bearer
		.as_ref()
		.map(|TypedHeader(Authorization(bearer))| bearer.token())
		.ok_or_else(|| err!(Request(MissingToken("Missing access token."))))?;
	let appservice = services
		.appservice
		.find_from_access_token(token)
		.await
		.map_err(|_| err!(Request(Unauthorized("Appservice token required."))))?;

	if !services
		.config
		.bridge_batch_send_appservices
		.iter()
		.any(|id| id == appservice.registration.id.as_str())
	{
		return Err!(Request(Forbidden("Appservice is not allowed to batch send.")));
	}

	for event in &body.events {
		if !services.globals.user_is_local(&event.sender) {
			return Err!(Request(Forbidden("Event sender must be local.")));
		}
		let allowed_double_puppet = services
			.config
			.bridge_batch_send_local_senders
			.contains(&event.sender);
		if !appservice.is_user_match(&event.sender) && !allowed_double_puppet {
			return Err!(Request(Forbidden("Event sender is not allowed for this appservice.")));
		}
		if !services
			.state_cache
			.is_joined(&event.sender, &room_id)
			.await
		{
			return Err!(Request(Forbidden("Event sender is not joined to the room.")));
		}
	}

	if let Some(user_id) = body.mark_read_by.as_deref()
		&& (!services
			.config
			.bridge_batch_send_local_senders
			.iter()
			.any(|allowed| allowed == user_id)
			|| !services
				.state_cache
				.is_joined(user_id, &room_id)
				.await)
	{
		return Err!(Request(Forbidden("mark_read_by user is not allowed.")));
	}

	let event_ids = services
		.timeline
		.append_batch(&room_id, body.events, BatchOptions {
			forward: body.forward,
			forward_if_no_messages: body.forward_if_no_messages,
			send_notification: body.send_notification,
			mark_read_by: body.mark_read_by.as_deref(),
		})
		.await?;

	Ok(Json(BatchSendResponse { event_ids }))
}

/// Maximum number of rooms returned in a single `mutual_rooms` page.
const PAGE_SIZE: usize = 1000;

/// # `GET /_matrix/client/v1/mutual_rooms`
/// # `GET /_matrix/client/unstable/uk.half-shot.msc2666/user/mutual_rooms`
///
/// Gets all the rooms the sender shares with the specified user.
///
/// An implementation of [MSC2666](https://github.com/matrix-org/matrix-spec-proposals/pull/2666)
#[tracing::instrument(skip_all, fields(%client), name = "mutual_rooms")]
pub(crate) async fn get_mutual_rooms_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<mutual_rooms::v1::Request>,
) -> Result<mutual_rooms::v1::Response> {
	let sender_user = body.sender_user();

	if sender_user == body.user_id {
		return Err!(Request(InvalidParam("You cannot request rooms in common with yourself.")));
	}

	if body.user_id.validate_historical().is_err() {
		return Err!(Request(InvalidParam("The user_id is not a compliant user identifier.")));
	}

	let all: Vec<OwnedRoomId> = services
		.state_cache
		.get_shared_rooms(sender_user, &body.user_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let count = UInt::try_from(all.len()).unwrap_or(UInt::MAX);

	let start = match body.from.as_deref() {
		| None => 0,
		| Some(token) => {
			let cursor = decode_cursor(token)
				.ok_or_else(|| err!(Request(InvalidParam("Invalid `from` token."))))?;

			all.partition_point(|room_id| room_id.as_str() <= cursor.as_str())
		},
	};

	let end = start.saturating_add(PAGE_SIZE).min(all.len());
	let next_batch = (end < all.len()).then(|| b64.encode(all[end.saturating_sub(1)].as_str()));

	let joined = if start == 0 && end == all.len() {
		all
	} else {
		all[start..end].to_vec()
	};

	Ok(mutual_rooms::v1::Response { joined, count, next_batch })
}

/// Decodes a base64url pagination cursor to its room id.
fn decode_cursor(token: &str) -> Option<OwnedRoomId> {
	let bytes = b64.decode(token).ok()?;
	let room_id = str::from_utf8(&bytes).ok()?;

	OwnedRoomId::parse(room_id).ok()
}
