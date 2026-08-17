from __future__ import annotations

from datetime import datetime, UTC
import httpx

from .base import BaseConnector, RawDocument, SourceType, ensure_utc


class SlackConnector(BaseConnector):
    """Fetch threads from Slack Conversations API."""

    def __init__(self, bot_token: str | None) -> None:
        self.bot_token = bot_token

    def source_type(self) -> SourceType:
        return "slack"

    async def fetch(self, since: datetime | None) -> list[RawDocument]:
        if not self.bot_token:
            return []

        documents: list[RawDocument] = []
        oldest = ""
        if since is not None:
            oldest = str(ensure_utc(since).timestamp())

        async with httpx.AsyncClient(base_url="https://slack.com/api") as client:
            headers = {
                "Authorization": f"Bearer {self.bot_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            # Fetch a list of public channels
            chan_resp = await client.get("/conversations.list", params={"types": "public_channel", "limit": 100}, headers=headers)
            chan_resp.raise_for_status()
            chan_data = chan_resp.json()
            if not chan_data.get("ok"):
                return []

            for channel in chan_data.get("channels", []):
                channel_id = channel.get("id")
                
                # Fetch messages in the channel
                hist_params = {"channel": channel_id, "limit": 100}
                if oldest:
                    hist_params["oldest"] = oldest

                hist_resp = await client.get("/conversations.history", params=hist_params, headers=headers)
                hist_resp.raise_for_status()
                hist_data = hist_resp.json()
                if not hist_data.get("ok"):
                    continue

                for message in hist_data.get("messages", []):
                    # We are interested in threads
                    if "thread_ts" in message and message["ts"] == message["thread_ts"]:
                        thread_ts = message["thread_ts"]
                        
                        # Fetch thread replies
                        repl_resp = await client.get("/conversations.replies", params={"channel": channel_id, "ts": thread_ts}, headers=headers)
                        repl_resp.raise_for_status()
                        repl_data = repl_resp.json()
                        if not repl_data.get("ok"):
                            continue
                        
                        thread_messages = repl_data.get("messages", [])
                        if not thread_messages:
                            continue

                        # Combine all messages in the thread into one document
                        thread_text = "\\n\\n".join(
                            f"{m.get('user', 'unknown')}: {m.get('text', '')}"
                            for m in thread_messages
                        )
                        
                        # Derive times
                        first_msg_ts = float(thread_messages[0]["ts"])
                        last_msg_ts = float(thread_messages[-1]["ts"])
                        
                        created_at = datetime.fromtimestamp(first_msg_ts, tz=UTC)
                        updated_at = datetime.fromtimestamp(last_msg_ts, tz=UTC)
                        
                        author = thread_messages[0].get("user", "unknown")
                        
                        documents.append(RawDocument(
                            doc_id=f"slack-{channel_id}-{thread_ts}",
                            source_type=self.source_type(),
                            source_uri=f"slack://channel/{channel_id}/thread/{thread_ts}",
                            title=f"Slack Thread in {channel.get('name', channel_id)}",
                            raw_text=thread_text,
                            created_at=created_at,
                            updated_at=updated_at,
                            author=author,
                            metadata={
                                "resolution_status": "unknown",  # Will be classified by disentanglement
                                "is_accepted_answer": False
                            }
                        ))

        return documents
