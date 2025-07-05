from dataclasses import dataclass
from typing import Dict, Any, List
from typing import Optional, Union
from pathlib import Path
from .handler import ScoredDatasetHandler


@dataclass
class Post:
    """Represents a post from the Scored dataset."""

    id: int
    uuid: str
    author: str
    title: str
    content: str
    raw_content: str
    community: str
    created: int
    score: int
    score_up: int
    score_down: int
    comments: int
    type: str
    domain: str
    link: str
    preview: str
    media: Dict[str, Any]
    link_metadata: Dict[str, Any]
    is_locked: bool
    is_video: bool
    is_video_mp4: bool
    is_image: bool
    is_twitter: bool
    is_crosspost: bool
    is_admin: bool
    is_moderator: bool
    is_new_user: bool
    is_stickied: bool
    is_nsfw: bool
    is_deleted: bool
    is_removed: bool
    is_edited: bool
    pro_tier: int
    awards: int
    vote_state: int
    tweet_id: str
    sticky_comment: int
    suggested_sort: int
    post_flair_class: str
    post_flair_text: str
    comment_sort: str
    broad_ad_tier: int
    ad_tier: int
    last_comment_author: str
    last_comment_created: int
    video_link: str
    removal_source: str
    author_flair_class: str
    author_flair_text: str
    profile_picture: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Post":
        """Create a Post instance from a dictionary."""
        return cls(
            id=data.get("id", 0),
            uuid=data.get("uuid", ""),
            author=data.get("author", ""),
            title=data.get("title", ""),
            content=data.get("content", ""),
            raw_content=data.get("raw_content", ""),
            community=data.get("community", ""),
            created=data.get("created", 0),
            score=data.get("score", 0),
            score_up=data.get("score_up", 0),
            score_down=data.get("score_down", 0),
            comments=data.get("comments", 0),
            type=data.get("type", ""),
            domain=data.get("domain", ""),
            link=data.get("link", ""),
            preview=data.get("preview", ""),
            media=data.get("media", {}),
            link_metadata=data.get("link_metadata", {}),
            is_locked=data.get("is_locked", False),
            is_video=data.get("is_video", False),
            is_video_mp4=data.get("is_video_mp4", False),
            is_image=data.get("is_image", False),
            is_twitter=data.get("is_twitter", False),
            is_crosspost=data.get("is_crosspost", False),
            is_admin=data.get("is_admin", False),
            is_moderator=data.get("is_moderator", False),
            is_new_user=data.get("is_new_user", False),
            is_stickied=data.get("is_stickied", False),
            is_nsfw=data.get("is_nsfw", False),
            is_deleted=data.get("is_deleted", False),
            is_removed=data.get("is_removed", False),
            is_edited=data.get("is_edited", False),
            pro_tier=data.get("pro_tier", 0),
            awards=data.get("awards", 0),
            vote_state=data.get("vote_state", 0),
            tweet_id=data.get("tweet_id", ""),
            sticky_comment=data.get("sticky_comment", 0),
            suggested_sort=data.get("suggested_sort", 0),
            post_flair_class=data.get("post_flair_class", ""),
            post_flair_text=data.get("post_flair_text", ""),
            comment_sort=data.get("comment_sort", ""),
            broad_ad_tier=data.get("broad_ad_tier", 0),
            ad_tier=data.get("ad_tier", 0),
            last_comment_author=data.get("last_comment_author", ""),
            last_comment_created=data.get("last_comment_created", 0),
            video_link=data.get("video_link", ""),
            removal_source=data.get("removal_source", ""),
            author_flair_class=data.get("author_flair_class", ""),
            author_flair_text=data.get("author_flair_text", ""),
            profile_picture=data.get("profile_picture", ""),
        )


@dataclass
class Comment:
    """Represents a comment from the Scored dataset."""

    id: int
    uuid: str
    author: str
    content: str
    raw_content: str
    community: str
    created: int
    score: int
    score_up: int
    score_down: int
    in_reply_to_id: int
    comment_parent_id: int
    parent_id: int
    parent_uuid: str
    child_ids: List[int]
    is_admin: bool
    is_moderator: bool
    is_new_user: bool
    is_stickied: bool
    is_deleted: bool
    is_removed: bool
    is_edited: bool
    is_shown: bool
    pro_tier: int
    awards: int
    vote_state: int
    removal_source: str
    author_flair_class: str
    author_flair_text: str
    profile_picture: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Comment":
        """Create a Comment instance from a dictionary."""
        return cls(
            id=data.get("id", 0),
            uuid=data.get("uuid", ""),
            author=data.get("author", ""),
            content=data.get("content", ""),
            raw_content=data.get("raw_content", ""),
            community=data.get("community", ""),
            created=data.get("created", 0),
            score=data.get("score", 0),
            score_up=data.get("score_up", 0),
            score_down=data.get("score_down", 0),
            in_reply_to_id=data.get("in_reply_to_id", 0),
            comment_parent_id=data.get("comment_parent_id", 0),
            parent_id=data.get("parent_id", 0),
            parent_uuid=data.get("parent_uuid", ""),
            child_ids=data.get("child_ids", []),
            is_admin=data.get("is_admin", False),
            is_moderator=data.get("is_moderator", False),
            is_new_user=data.get("is_new_user", False),
            is_stickied=data.get("is_stickied", False),
            is_deleted=data.get("is_deleted", False),
            is_removed=data.get("is_removed", False),
            is_edited=data.get("is_edited", False),
            is_shown=data.get("is_shown", True),
            pro_tier=data.get("pro_tier", 0),
            awards=data.get("awards", 0),
            vote_state=data.get("vote_state", 0),
            removal_source=data.get("removal_source", ""),
            author_flair_class=data.get("author_flair_class", ""),
            author_flair_text=data.get("author_flair_text", ""),
            profile_picture=data.get("profile_picture", ""),
        )


class TimeSlicedHandler(ScoredDatasetHandler):
    def __init__(self, parent_handler, start_time, end_time):
        self.dataset_path = parent_handler.dataset_path
        self.time_range = (start_time, end_time)
        self._user_files = parent_handler._user_files

    def iter_all_data(self):
        for username, row in super().iter_all_data():
            # Filter posts and comments by time
            filtered_posts = []
            filtered_comments = []

            if "posts" in row and row["posts"]:
                for post in row["posts"]:
                    if (
                        self.time_range[0]
                        <= post.get("created", 0)
                        <= self.time_range[1]
                    ):
                        filtered_posts.append(post)

            if "comments" in row and row["comments"]:
                for comment in row["comments"]:
                    if (
                        self.time_range[0]
                        <= comment.get("created", 0)
                        <= self.time_range[1]
                    ):
                        filtered_comments.append(comment)

            if filtered_posts or filtered_comments:
                filtered_row = row.copy()
                filtered_row["posts"] = filtered_posts
                filtered_row["comments"] = filtered_comments
                yield username, filtered_row
