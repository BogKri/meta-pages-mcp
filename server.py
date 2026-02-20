"""Meta Pages MCP Server — Multi-token Page/Community management.

Supports User Access Token, Page Access Tokens, and App Access Token
for complete Facebook/Instagram page management including Ad Comments.
"""

import os
import json
import logging
from typing import Optional, Dict, Any

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GRAPH_API_VERSION = "v22.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def _user_token() -> str:
    token = os.environ.get("META_SYSTEM_USER_TOKEN", "")
    if not token:
        raise ValueError("META_SYSTEM_USER_TOKEN env var is not set")
    return token


def _page_tokens() -> Dict[str, str]:
    raw = os.environ.get("META_PAGE_TOKENS", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.error("META_PAGE_TOKENS is not valid JSON")
        return {}


def _page_token(page_id: str) -> str:
    tokens = _page_tokens()
    token = tokens.get(page_id)
    if not token:
        return _exchange_page_token(page_id)
    return token


def _exchange_page_token(page_id: str) -> str:
    try:
        resp = httpx.get(
            f"{GRAPH_API_BASE}/{page_id}",
            params={"fields": "access_token", "access_token": _user_token()},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if token:
            return token
        raise ValueError(f"No access_token returned for page {page_id}")
    except Exception as e:
        raise ValueError(f"Could not get page token for {page_id}: {e}")


def _app_token() -> str:
    app_id = os.environ.get("META_APP_ID", "")
    app_secret = os.environ.get("META_APP_SECRET", "")
    if app_id and app_secret:
        return f"{app_id}|{app_secret}"
    raise ValueError("META_APP_ID and META_APP_SECRET env vars are not set")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _graph_get(endpoint: str, params: Dict[str, Any], token: str) -> Dict[str, Any]:
    params["access_token"] = token
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{GRAPH_API_BASE}/{endpoint}", params=params)
        data = resp.json()
        if "error" in data:
            return {"error": data["error"].get("message", str(data["error"]))}
        return data


async def _graph_post(endpoint: str, payload: Dict[str, Any], token: str) -> Dict[str, Any]:
    payload["access_token"] = token
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(f"{GRAPH_API_BASE}/{endpoint}", data=payload)
        data = resp.json()
        if "error" in data:
            return {"error": data["error"].get("message", str(data["error"]))}
        return data


async def _graph_delete(endpoint: str, token: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.delete(
            f"{GRAPH_API_BASE}/{endpoint}", params={"access_token": token}
        )
        data = resp.json()
        if "error" in data:
            return {"error": data["error"].get("message", str(data["error"]))}
        return data


def _fmt(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "meta_pages_mcp",
    host="0.0.0.0",
    port=int(os.environ.get("PORT", "10000")),
)

# Configure for stateless HTTP mode (matching working meta-ads-mcp config)
mcp.settings.stateless_http = True
mcp.settings.json_response = True


# ===================== PAGES — LIST & INFO =====================

@mcp.tool(
    name="meta_list_pages",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_list_pages() -> str:
    """List all Facebook Pages the user manages. Returns page IDs, names, and categories.
    Uses the User Access Token.
    """
    data = await _graph_get("me/accounts", {"fields": "id,name,category,fan_count,username", "limit": "100"}, _user_token())
    return _fmt(data)


@mcp.tool(
    name="meta_get_page_info",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_page_info(page_id: str) -> str:
    """Get detailed info about a Facebook Page.

    Args:
        page_id: The Facebook Page ID (e.g. '1320892154625477')
    """
    fields = "id,name,username,category,fan_count,followers_count,about,description,website,phone,emails,location,hours,verification_status,picture{url}"
    data = await _graph_get(page_id, {"fields": fields}, _page_token(page_id))
    return _fmt(data)


# ===================== PAGE POSTS =====================

@mcp.tool(
    name="meta_get_page_posts",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_page_posts(page_id: str, limit: int = 10) -> str:
    """Get posts from a Facebook Page feed.

    Args:
        page_id: The Facebook Page ID
        limit: Number of posts to return (1-100, default 10)
    """
    fields = "id,message,created_time,type,permalink_url,shares,likes.summary(true).limit(0),comments.summary(true).limit(0),attachments{media_type,url,media}"
    data = await _graph_get(
        f"{page_id}/feed",
        {"fields": fields, "limit": str(min(max(limit, 1), 100))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_create_page_post",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_create_page_post(page_id: str, message: str, link: Optional[str] = None) -> str:
    """Create a new post on a Facebook Page.

    Args:
        page_id: The Facebook Page ID
        message: The post text content
        link: Optional URL to attach to the post
    """
    payload: Dict[str, Any] = {"message": message}
    if link:
        payload["link"] = link
    data = await _graph_post(f"{page_id}/feed", payload, _page_token(page_id))
    return _fmt(data)


@mcp.tool(
    name="meta_delete_page_post",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True, "openWorldHint": True},
)
async def meta_delete_page_post(post_id: str, page_id: str) -> str:
    """Delete a post from a Facebook Page.

    Args:
        post_id: The post ID to delete (format: pageId_postId)
        page_id: The Facebook Page ID (needed for token lookup)
    """
    data = await _graph_delete(post_id, _page_token(page_id))
    return _fmt(data)


# ===================== COMMENTS =====================

@mcp.tool(
    name="meta_get_post_comments",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_post_comments(post_id: str, page_id: str, limit: int = 25) -> str:
    """Get comments on a Facebook Page post.

    Args:
        post_id: The post ID (format: pageId_postId)
        page_id: The Facebook Page ID (for token lookup)
        limit: Number of comments to return (1-100, default 25)
    """
    fields = "id,message,from{id,name},created_time,like_count,comment_count,attachment{media_type,url,media}"
    data = await _graph_get(
        f"{post_id}/comments",
        {"fields": fields, "limit": str(min(max(limit, 1), 100))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_reply_to_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_reply_to_comment(comment_id: str, page_id: str, message: str) -> str:
    """Reply to a comment on a Facebook Page post.

    Args:
        comment_id: The comment ID to reply to
        page_id: The Facebook Page ID (for token lookup)
        message: The reply text
    """
    data = await _graph_post(
        f"{comment_id}/comments",
        {"message": message},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_delete_comment",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True, "openWorldHint": True},
)
async def meta_delete_comment(comment_id: str, page_id: str) -> str:
    """Delete a comment on a Facebook Page post.

    Args:
        comment_id: The comment ID to delete
        page_id: The Facebook Page ID (for token lookup)
    """
    data = await _graph_delete(comment_id, _page_token(page_id))
    return _fmt(data)


@mcp.tool(
    name="meta_like_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_like_comment(comment_id: str, page_id: str) -> str:
    """Like a comment as the Page.

    Args:
        comment_id: The comment ID to like
        page_id: The Facebook Page ID (for token lookup)
    """
    data = await _graph_post(f"{comment_id}/likes", {}, _page_token(page_id))
    return _fmt(data)


@mcp.tool(
    name="meta_hide_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_hide_comment(comment_id: str, page_id: str, is_hidden: bool = True) -> str:
    """Hide or unhide a comment on a Facebook Page post.

    Args:
        comment_id: The comment ID
        page_id: The Facebook Page ID (for token lookup)
        is_hidden: True to hide, False to unhide (default True)
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{GRAPH_API_BASE}/{comment_id}",
            data={"is_hidden": str(is_hidden).lower(), "access_token": _page_token(page_id)},
        )
        return _fmt(resp.json())


# ===================== AD COMMENTS =====================

@mcp.tool(
    name="meta_get_ad_accounts",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ad_accounts() -> str:
    """List all ad accounts the user has access to. Returns account IDs, names, and status.
    Uses the User Access Token.
    """
    data = await _graph_get(
        "me/adaccounts",
        {"fields": "id,name,account_id,account_status,currency,timezone_name", "limit": "100"},
        _user_token(),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_ad_campaigns",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ad_campaigns(
    ad_account_id: str,
    status_filter: str = "ACTIVE",
    limit: int = 25,
) -> str:
    """Get campaigns for an ad account.

    Args:
        ad_account_id: The ad account ID (format: act_XXXXXXXXX)
        status_filter: Filter by effective status: ACTIVE, PAUSED, ARCHIVED, or ALL (default ACTIVE)
        limit: Number of campaigns to return (1-100, default 25)
    """
    params: Dict[str, str] = {
        "fields": "id,name,objective,status,effective_status,daily_budget,lifetime_budget,created_time",
        "limit": str(min(max(limit, 1), 100)),
    }
    if status_filter.upper() != "ALL":
        params["filtering"] = json.dumps([{"field": "effective_status", "operator": "IN", "value": [status_filter.upper()]}])
    data = await _graph_get(f"{ad_account_id}/campaigns", params, _user_token())
    return _fmt(data)


@mcp.tool(
    name="meta_get_ad_adsets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ad_adsets(
    ad_account_id: str,
    campaign_id: Optional[str] = None,
    status_filter: str = "ACTIVE",
    limit: int = 25,
) -> str:
    """Get ad sets for an ad account, optionally filtered by campaign.

    Args:
        ad_account_id: The ad account ID (format: act_XXXXXXXXX)
        campaign_id: Optional campaign ID to filter by
        status_filter: Filter by effective status: ACTIVE, PAUSED, ARCHIVED, or ALL (default ACTIVE)
        limit: Number of ad sets to return (1-100, default 25)
    """
    params: Dict[str, str] = {
        "fields": "id,name,campaign_id,status,effective_status,daily_budget,lifetime_budget,targeting,optimization_goal",
        "limit": str(min(max(limit, 1), 100)),
    }
    filtering = []
    if status_filter.upper() != "ALL":
        filtering.append({"field": "effective_status", "operator": "IN", "value": [status_filter.upper()]})
    if campaign_id:
        filtering.append({"field": "campaign.id", "operator": "EQUAL", "value": campaign_id})
    if filtering:
        params["filtering"] = json.dumps(filtering)
    data = await _graph_get(f"{ad_account_id}/adsets", params, _user_token())
    return _fmt(data)


@mcp.tool(
    name="meta_get_ads",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ads(
    ad_account_id: str,
    campaign_id: Optional[str] = None,
    adset_id: Optional[str] = None,
    status_filter: str = "ACTIVE",
    limit: int = 25,
) -> str:
    """Get ads for an ad account, optionally filtered by campaign or ad set.

    Args:
        ad_account_id: The ad account ID (format: act_XXXXXXXXX)
        campaign_id: Optional campaign ID to filter by
        adset_id: Optional ad set ID to filter by
        status_filter: Filter by effective status: ACTIVE, PAUSED, ARCHIVED, or ALL (default ACTIVE)
        limit: Number of ads to return (1-100, default 25)
    """
    params: Dict[str, str] = {
        "fields": "id,name,status,effective_status,campaign_id,adset_id,creative{id,effective_object_story_id}",
        "limit": str(min(max(limit, 1), 100)),
    }
    filtering = []
    if status_filter.upper() != "ALL":
        filtering.append({"field": "effective_status", "operator": "IN", "value": [status_filter.upper()]})
    if campaign_id:
        filtering.append({"field": "campaign.id", "operator": "EQUAL", "value": campaign_id})
    if adset_id:
        filtering.append({"field": "adset.id", "operator": "EQUAL", "value": adset_id})
    if filtering:
        params["filtering"] = json.dumps(filtering)
    data = await _graph_get(f"{ad_account_id}/ads", params, _user_token())
    return _fmt(data)


@mcp.tool(
    name="meta_get_ad_creative",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ad_creative(ad_id: str) -> str:
    """Get the creative details for an ad, including the effective_object_story_id
    which links to the underlying Page post used for ad comments.

    Args:
        ad_id: The ad ID
    """
    data = await _graph_get(
        ad_id,
        {"fields": "id,name,creative{id,effective_object_story_id,object_story_spec,thumbnail_url,title,body}"},
        _user_token(),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_ad_comments",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ad_comments(
    effective_object_story_id: str,
    page_id: str,
    limit: int = 25,
    filter_type: str = "toplevel",
) -> str:
    """Get comments on an ad post using its effective_object_story_id.

    The effective_object_story_id is the underlying Page post ID for the ad.
    You can get it from meta_get_ads or meta_get_ad_creative.

    Args:
        effective_object_story_id: The post ID from the ad creative (format: pageId_postId)
        page_id: The Facebook Page ID that owns the ad (for token lookup)
        limit: Number of comments to return (1-100, default 25)
        filter_type: Comment filter: toplevel (default), stream (all including replies)
    """
    fields = "id,message,from{id,name},created_time,like_count,comment_count,is_hidden,attachment{media_type,url,media}"
    params: Dict[str, str] = {
        "fields": fields,
        "limit": str(min(max(limit, 1), 100)),
        "filter": filter_type,
    }
    data = await _graph_get(
        f"{effective_object_story_id}/comments",
        params,
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_reply_to_ad_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_reply_to_ad_comment(comment_id: str, page_id: str, message: str) -> str:
    """Reply to a comment on an ad post as the Page.

    Args:
        comment_id: The comment ID to reply to
        page_id: The Facebook Page ID (for token lookup)
        message: The reply text
    """
    data = await _graph_post(
        f"{comment_id}/comments",
        {"message": message},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_hide_ad_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_hide_ad_comment(comment_id: str, page_id: str, is_hidden: bool = True) -> str:
    """Hide or unhide a comment on an ad post.

    Args:
        comment_id: The comment ID
        page_id: The Facebook Page ID (for token lookup)
        is_hidden: True to hide, False to unhide (default True)
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{GRAPH_API_BASE}/{comment_id}",
            data={"is_hidden": str(is_hidden).lower(), "access_token": _page_token(page_id)},
        )
        return _fmt(resp.json())


@mcp.tool(
    name="meta_delete_ad_comment",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True, "openWorldHint": True},
)
async def meta_delete_ad_comment(comment_id: str, page_id: str) -> str:
    """Delete a comment on an ad post.

    Args:
        comment_id: The comment ID to delete
        page_id: The Facebook Page ID (for token lookup)
    """
    data = await _graph_delete(comment_id, _page_token(page_id))
    return _fmt(data)


# ===================== INSTAGRAM =====================

@mcp.tool(
    name="meta_get_ig_accounts",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ig_accounts(page_id: str) -> str:
    """Get the Instagram Business Account connected to a Facebook Page.

    Args:
        page_id: The Facebook Page ID
    """
    data = await _graph_get(
        page_id,
        {"fields": "instagram_business_account{id,name,username,profile_picture_url,followers_count,media_count}"},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_ig_media",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ig_media(ig_account_id: str, page_id: str, limit: int = 10) -> str:
    """Get recent media from an Instagram Business Account.

    Args:
        ig_account_id: The Instagram Business Account ID
        page_id: The Facebook Page ID (for token lookup)
        limit: Number of posts to return (1-50, default 10)
    """
    fields = "id,caption,media_type,media_url,permalink,timestamp,like_count,comments_count,thumbnail_url"
    data = await _graph_get(
        f"{ig_account_id}/media",
        {"fields": fields, "limit": str(min(max(limit, 1), 50))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_ig_comments",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_ig_comments(media_id: str, page_id: str, limit: int = 25) -> str:
    """Get comments on an Instagram media item.

    Args:
        media_id: The Instagram media ID
        page_id: The Facebook Page ID (for token lookup)
        limit: Number of comments to return (1-100, default 25)
    """
    fields = "id,text,username,timestamp,like_count,replies{id,text,username,timestamp}"
    data = await _graph_get(
        f"{media_id}/comments",
        {"fields": fields, "limit": str(min(max(limit, 1), 100))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_reply_ig_comment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_reply_ig_comment(comment_id: str, page_id: str, message: str) -> str:
    """Reply to an Instagram comment.

    Args:
        comment_id: The Instagram comment ID to reply to
        page_id: The Facebook Page ID (for token lookup)
        message: The reply text
    """
    data = await _graph_post(
        f"{comment_id}/replies",
        {"message": message},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_delete_ig_comment",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True, "openWorldHint": True},
)
async def meta_delete_ig_comment(comment_id: str, page_id: str) -> str:
    """Delete an Instagram comment (only comments by your account or on your media).

    Args:
        comment_id: The Instagram comment ID to delete
        page_id: The Facebook Page ID (for token lookup)
    """
    data = await _graph_delete(comment_id, _page_token(page_id))
    return _fmt(data)


# ===================== MESSAGING =====================

@mcp.tool(
    name="meta_get_conversations",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_conversations(page_id: str, limit: int = 10) -> str:
    """Get conversations (messages) for a Facebook Page.

    Args:
        page_id: The Facebook Page ID
        limit: Number of conversations to return (1-50, default 10)
    """
    fields = "id,snippet,updated_time,message_count,participants,messages.limit(3){id,message,from,created_time}"
    data = await _graph_get(
        f"{page_id}/conversations",
        {"fields": fields, "limit": str(min(max(limit, 1), 50))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_conversation_messages",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_conversation_messages(conversation_id: str, page_id: str, limit: int = 20) -> str:
    """Get messages within a specific conversation.

    Args:
        conversation_id: The conversation/thread ID
        page_id: The Facebook Page ID (for token lookup)
        limit: Number of messages to return (1-100, default 20)
    """
    fields = "id,message,from,to,created_time,attachments{mime_type,name,size,url}"
    data = await _graph_get(
        f"{conversation_id}/messages",
        {"fields": fields, "limit": str(min(max(limit, 1), 100))},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_send_message",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_send_message(page_id: str, recipient_id: str, message: str) -> str:
    """Send a message from a Facebook Page to a user (within 24h messaging window).

    Args:
        page_id: The Facebook Page ID
        recipient_id: The recipient's PSID (Page-Scoped ID)
        message: The message text to send
    """
    payload = {
        "recipient": json.dumps({"id": recipient_id}),
        "message": json.dumps({"text": message}),
        "messaging_type": "RESPONSE",
    }
    data = await _graph_post(f"{page_id}/messages", payload, _page_token(page_id))
    return _fmt(data)


# ===================== LEADS =====================

@mcp.tool(
    name="meta_get_lead_forms",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_lead_forms(page_id: str) -> str:
    """Get lead generation forms for a Facebook Page.

    Args:
        page_id: The Facebook Page ID
    """
    fields = "id,name,status,created_time,leads_count,locale,questions{key,label,type}"
    data = await _graph_get(
        f"{page_id}/leadgen_forms",
        {"fields": fields, "limit": "50"},
        _page_token(page_id),
    )
    return _fmt(data)


@mcp.tool(
    name="meta_get_lead_data",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_lead_data(form_id: str, page_id: str, limit: int = 25) -> str:
    """Get submitted lead data from a lead generation form.

    Args:
        form_id: The lead form ID
        page_id: The Facebook Page ID (for token lookup)
        limit: Number of leads to return (1-100, default 25)
    """
    fields = "id,created_time,field_data,ad_id,ad_name,campaign_id,campaign_name"
    data = await _graph_get(
        f"{form_id}/leads",
        {"fields": fields, "limit": str(min(max(limit, 1), 100))},
        _page_token(page_id),
    )
    return _fmt(data)


# ===================== PAGE INSIGHTS =====================

@mcp.tool(
    name="meta_get_page_insights",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_page_insights(
    page_id: str,
    metrics: str = "page_impressions,page_engaged_users,page_fans,page_views_total",
    period: str = "day",
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> str:
    """Get Page-level insights/metrics.

    Args:
        page_id: The Facebook Page ID
        metrics: Comma-separated metric names. Options: page_impressions, page_engaged_users,
                 page_fans, page_views_total, page_post_engagements, page_consumptions,
                 page_fan_adds, page_fan_removes, page_actions_post_reactions_total
        period: Aggregation period: day, week, days_28 (default: day)
        since: Start date (YYYY-MM-DD format, optional)
        until: End date (YYYY-MM-DD format, optional)
    """
    params: Dict[str, str] = {"metric": metrics, "period": period}
    if since:
        params["since"] = since
    if until:
        params["until"] = until
    data = await _graph_get(f"{page_id}/insights", params, _page_token(page_id))
    return _fmt(data)


@mcp.tool(
    name="meta_get_post_insights",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_post_insights(post_id: str, page_id: str) -> str:
    """Get insights/metrics for a specific Page post.

    Args:
        post_id: The post ID (format: pageId_postId)
        page_id: The Facebook Page ID (for token lookup)
    """
    metrics = "post_impressions,post_impressions_unique,post_engaged_users,post_clicks,post_reactions_by_type_total"
    data = await _graph_get(
        f"{post_id}/insights",
        {"metric": metrics},
        _page_token(page_id),
    )
    return _fmt(data)


# ===================== WEBHOOKS =====================

@mcp.tool(
    name="meta_get_page_subscriptions",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_get_page_subscriptions(page_id: str) -> str:
    """Get current webhook subscriptions for a Facebook Page.

    Args:
        page_id: The Facebook Page ID
    """
    data = await _graph_get(f"{page_id}/subscribed_apps", {}, _page_token(page_id))
    return _fmt(data)


@mcp.tool(
    name="meta_subscribe_page_webhooks",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_subscribe_page_webhooks(
    page_id: str,
    subscribed_fields: str = "feed,messages,messaging_postbacks,message_deliveries,message_reads",
) -> str:
    """Subscribe a Page to webhook events for your app.

    Args:
        page_id: The Facebook Page ID
        subscribed_fields: Comma-separated webhook fields to subscribe to
    """
    data = await _graph_post(
        f"{page_id}/subscribed_apps",
        {"subscribed_fields": subscribed_fields},
        _page_token(page_id),
    )
    return _fmt(data)


# ===================== UTILITY =====================

@mcp.tool(
    name="meta_debug_token",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def meta_debug_token(token_type: str = "user") -> str:
    """Debug/inspect an access token to check permissions and expiry.

    Args:
        token_type: Which token to debug: 'user', 'app', or a page_id for a page token
    """
    if token_type == "user":
        input_token = _user_token()
    elif token_type == "app":
        input_token = _app_token()
    else:
        input_token = _page_token(token_type)

    try:
        app_token = _app_token()
    except ValueError:
        app_token = _user_token()

    data = await _graph_get("debug_token", {"input_token": input_token}, app_token)
    return _fmt(data)


@mcp.tool(
    name="meta_graph_api_call",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def meta_graph_api_call(
    endpoint: str,
    method: str = "GET",
    token_type: str = "user",
    page_id: Optional[str] = None,
    fields: Optional[str] = None,
    params: Optional[str] = None,
    body: Optional[str] = None,
) -> str:
    """Make a raw Graph API call. Escape hatch for any endpoint not covered by other tools.

    Args:
        endpoint: The Graph API endpoint (e.g. 'me', '123456/feed', 'me/accounts')
        method: HTTP method: GET, POST, or DELETE (default GET)
        token_type: Which token to use: 'user', 'app', or 'page' (default 'user')
        page_id: Required when token_type='page' — the Page ID for token lookup
        fields: Optional comma-separated fields to request
        params: Optional JSON string of additional query parameters
        body: Optional JSON string of POST body data
    """
    if token_type == "page":
        if not page_id:
            return _fmt({"error": "page_id is required when token_type='page'"})
        token = _page_token(page_id)
    elif token_type == "app":
        token = _app_token()
    else:
        token = _user_token()

    extra_params: Dict[str, str] = {}
    if fields:
        extra_params["fields"] = fields
    if params:
        try:
            extra_params.update(json.loads(params))
        except json.JSONDecodeError:
            return _fmt({"error": "params must be valid JSON"})

    if method.upper() == "GET":
        data = await _graph_get(endpoint, extra_params, token)
    elif method.upper() == "POST":
        payload: Dict[str, Any] = {}
        if body:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                return _fmt({"error": "body must be valid JSON"})
        payload.update(extra_params)
        data = await _graph_post(endpoint, payload, token)
    elif method.upper() == "DELETE":
        data = await _graph_delete(endpoint, token)
    else:
        data = {"error": f"Unsupported method: {method}. Use GET, POST, or DELETE."}

    return _fmt(data)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info(f"Starting Meta Pages MCP server on port {mcp.settings.port}")
    mcp.run(transport="streamable-http")
