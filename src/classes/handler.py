# -*- coding: utf-8 -*-

from tqdm import tqdm
import json
import gzip
from .helpers import *
from ..algorithms.statistics import *
from ..algorithms.discussion_trees import *
from ..algorithms.network import *


from pathlib import Path
from typing import Iterator, Dict, List, Set, Optional, Tuple, Any, Callable, Union
import networkx as nx
from datetime import datetime
import re


class ScoredDatasetHandler:
    """
    A class to handle large Scored.co datasets efficiently.

    Handles datasets structured as:
    - Folder containing jsonl.gz files
    - Each file named after a user who posted at least one post
    - Each post may contain comments from other users

    Designed for datasets that don't fit in memory using lazy loading.
    """

    def __init__(self, dataset_path: str):
        """
        Initialize the dataset handler.

        Args:
            dataset_path: Path to the folder containing jsonl.gz files
        """
        self.dataset_path = Path(dataset_path)
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset path {dataset_path} does not exist")

        self._file_cache = {}
        self._user_files = None

    @property
    def user_files(self) -> List[Path]:
        """Get list of all user files in the dataset."""
        if self._user_files is None:
            self._user_files = list(self.dataset_path.glob("*.jsonl.gz"))
        return self._user_files

    @property
    def post_metadata(self) -> List[str]:
        """Get metadata keys for posts."""
        # iter on C's, the platform founder
        fields = set()
        for row in self.iter_posts(users={"C"}):
            post = row[1]
            if isinstance(post, Post):
                # Use dataclass fields
                fields.update(post.__dataclass_fields__.keys())
            else:
                # Fallback to dictionary keys
                for key in post.keys():
                    if key not in fields:
                        fields.add(key)
        return sorted(list(fields))

    @property
    def comment_metadata(self) -> List[str]:
        """Get metadata keys for comments."""
        # iter on C's, the platform founder
        fields = set()
        for row in self.iter_comments(users={"C"}):
            comment = row[1]
            if isinstance(comment, Comment):
                # Use dataclass fields
                fields.update(comment.__dataclass_fields__.keys())
            else:
                # Fallback to dictionary keys
                for key in comment.keys():
                    if key not in fields:
                        fields.add(key)
        return sorted(list(fields))

    def get_users(self) -> Set[str]:
        """Get set of all users (based on filenames)."""
        return {f.stem.replace(".jsonl", "") for f in self.user_files}

    def _read_file(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Read a single jsonl.gz file and yield rows."""
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    def iter_all_data(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Iterate over all data in the dataset.

        Yields:
            Tuple of (username, row_data)
        """
        for file_path in tqdm(self.user_files):
            username = file_path.stem.replace(".jsonl", "")
            for row in self._read_file(file_path):
                yield username, row

    def iter_posts(
        self,
        text_filter: Optional[Callable[[str], bool]] = None,
        metadata_filter: Optional[Callable[[Union[Post, Dict[str, Any]]], bool]] = None,
        users: Optional[Set[str]] = None,
        time_range: Optional[Tuple[int, int]] = None,
        return_objects: bool = False,
    ) -> Iterator[Tuple[str, Union[Post, Dict[str, Any]]]]:
        """
        Iterate over posts with optional filtering.

        Args:
            text_filter: Function to filter by text content
            metadata_filter: Function to filter by metadata
            users: Set of users to include
            time_range: Tuple of (start_timestamp, end_timestamp)
            return_objects: If True, return Post objects instead of dictionaries

        Yields:
            Tuple of (username, post_data)
        """
        for username, row in self.iter_all_data():
            if users and username not in users:
                continue

            if "posts" in row and row["posts"]:
                for post_data in row["posts"]:
                    post_obj = (
                        Post.from_dict(post_data) if return_objects else post_data
                    )

                    # Time filtering
                    if time_range:
                        post_time = (
                            post_obj.created
                            if return_objects
                            else post_data.get("created", 0)
                        )
                        if not (time_range[0] <= post_time <= time_range[1]):
                            continue

                    # Text filtering
                    if text_filter:
                        if return_objects:
                            text_content = post_obj.content + " " + post_obj.title
                        else:
                            text_content = (
                                post_data.get("content", "")
                                + " "
                                + post_data.get("title", "")
                            )
                        if not text_filter(text_content):
                            continue

                    # Metadata filtering
                    if metadata_filter and not metadata_filter(post_obj):
                        continue

                    yield username, post_obj

    def iter_comments(
        self,
        text_filter: Optional[Callable[[str], bool]] = None,
        metadata_filter: Optional[
            Callable[[Union[Comment, Dict[str, Any]]], bool]
        ] = None,
        users: Optional[Set[str]] = None,
        time_range: Optional[Tuple[int, int]] = None,
        return_objects: bool = False,
    ) -> Iterator[
        Tuple[str, Union[Comment, Dict[str, Any]], Union[Post, Dict[str, Any]]]
    ]:
        """
        Iterate over comments with optional filtering.

        Args:
            text_filter: Function to filter by text content
            metadata_filter: Function to filter by metadata
            users: Set of users to include
            time_range: Tuple of (start_timestamp, end_timestamp)
            return_objects: If True, return Comment and Post objects instead of dictionaries

        Yields:
            Tuple of (username, comment_data, parent_post_data)
        """
        for username, row in self.iter_all_data():
            if users and username not in users:
                continue

            if (
                "comments" in row
                and row["comments"]
                and "posts" in row
                and row["posts"]
            ):
                parent_post_data = row["posts"][0]
                parent_post_obj = (
                    Post.from_dict(parent_post_data)
                    if return_objects
                    else parent_post_data
                )

                for comment_data in row["comments"]:
                    comment_obj = (
                        Comment.from_dict(comment_data)
                        if return_objects
                        else comment_data
                    )

                    # Time filtering
                    if time_range:
                        comment_time = (
                            comment_obj.created
                            if return_objects
                            else comment_data.get("created", 0)
                        )
                        if not (time_range[0] <= comment_time <= time_range[1]):
                            continue

                    # Text filtering
                    if text_filter:
                        text_content = (
                            comment_obj.content
                            if return_objects
                            else comment_data.get("content", "")
                        )
                        if not text_filter(text_content):
                            continue

                    # Metadata filtering
                    if metadata_filter and not metadata_filter(comment_obj):
                        continue

                    yield username, comment_obj, parent_post_obj

    def interactions(self, row: Dict[str, Any]) -> Iterator[Tuple[str, str, int]]:
        """
        Extract interactions between users based on comments and posts.

        Args:
            row: Row data containing posts and comments
            level: "post" for post-level interactions, "comment" for comment-level

        Yields:
            Tuple of (replying_user, replied_to_user, timestamp)
        """
        return all_interactions(row)

    def higher_order_interactions(
        self, row: Dict[str, Any], level: str = "comment"
    ) -> Iterator[Tuple[str, str, int]]:
        """
        Extract higher-order interactions. Can be lists of users co-commenting under a post,
        or users replying to the same comment, depending on the parameter "level".

        Args:
            row: Row data containing posts and comments
            level: "post" for post-level interactions, "comment" for comment-level

        Yields:
            Tuple of (replying_user, replied_to_user, timestamp)
        """
        return all_higher_order_interactions(row, level)

    def build_interaction_network(
        handler,
        time_range: Optional[Tuple[int, int]] = None,
        users: Optional[Set[str]] = None,
        min_interactions: int = 1,
    ) -> nx.DiGraph:
        """
        Build a directed interaction network from the dataset.

        Args:
            time_range: Tuple of (start_timestamp, end_timestamp)
            users: Set of users to include
            min_interactions: Minimum number of interactions to include edge

        Returns:
            NetworkX directed graph
        """
        return interaction_network(handler, time_range, users, min_interactions)

    def get_all_user_stats(self) -> Dict[str, Dict[str, Any]]:

        return user_stats(self)

    def get_user_stats(self, username: str) -> Dict[str, Any]:
        """
        Get statistics for a specific user.

        Args:
            username: Username to analyze

        Returns:
            Dictionary with user statistics
        """
        all_stats = self.get_all_user_stats()
        return all_stats.get(username, {})

    def search_text(
        self, pattern: str, case_sensitive: bool = False
    ) -> Iterator[Tuple[str, str, Dict]]:
        """
        Search for text patterns in posts and comments.

        Args:
            pattern: Regular expression pattern to search for
            case_sensitive: Whether search should be case sensitive

        Yields:
            Tuple of (username, content_type, content_data)
        """
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

        for username, row in self.iter_all_data():
            # Search in posts
            if "posts" in row and row["posts"]:
                for post in row["posts"]:
                    content = post.get("content", "") + " " + post.get("title", "")
                    if regex.search(content):
                        yield username, "post", post

            # Search in comments
            if "comments" in row and row["comments"]:
                for comment in row["comments"]:
                    content = comment.get("content", "")
                    if regex.search(content):
                        yield username, "comment", comment

    def get_time_slice(self, start_time: int, end_time: int) -> "ScoredDatasetHandler":
        """
        Create a virtual time slice of the dataset.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            New ScoredDatasetHandler instance with time filtering
        """

        return TimeSlicedHandler(self, start_time, end_time)

    def get_community_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for each community in the dataset.

        Returns:
            Dictionary with community statistics
        """
        return community_stats(self)

    def build_discussion_tree(
        self, row: Dict[str, Any], node_attrs: bool = True
    ) -> nx.DiGraph:
        """
        Build a discussion tree from a row (post with comments) using NetworkX.

        Args:
            row: Row data containing posts and comments
            node_attrs: Whether to include detailed node attributes

        Returns:
            NetworkX DiGraph representing the discussion tree
        """
        return discussion_tree(row, node_attrs)


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


def timestamp_to_datetime(timestamp: int) -> datetime:
    """Convert timestamp to datetime object."""
    return datetime.fromtimestamp(timestamp / 1000)


def datetime_to_timestamp(dt: datetime) -> int:
    """Convert datetime to timestamp."""
    return int(dt.timestamp() * 1000)
