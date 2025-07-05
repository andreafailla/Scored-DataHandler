from typing import Dict, Any, Iterator, Tuple, List, Set, Optional
from collections import defaultdict
import networkx as nx


def all_interactions(row: Dict[str, Any]) -> Iterator[Tuple[str, str, int]]:
    """
    Extract interactions from a row (replies between users).

    Args:
        row: Row data containing posts and comments

    Yields:
        Tuple of (replying_user, replied_to_user, timestamp)
    """
    comments = row.get("comments", [])
    if not comments:
        return

    posts = row.get("posts", [])
    if not posts:
        return

    post = posts[0]
    post_author = post.get("author", "")

    # Map comment IDs to authors
    cid_to_author = {0: post_author}  # 0 is the main post author

    for comment in comments:
        cid = comment.get("id")
        author = comment.get("author", "")
        if cid is not None:
            cid_to_author[cid] = author

    # Extract interactions
    for comment in comments:
        when = comment.get("created", 0)
        replyto = comment.get("in_reply_to_id", 0)
        author = comment.get("author", "")

        if replyto in cid_to_author and cid_to_author[replyto] and author:
            yield (author, cid_to_author[replyto], when)


def all_higher_order_interactions(
    row: Dict[str, Any], level: str = "comment"
) -> Iterator[Tuple[List[str], int]]:
    """
    Extract higher-order interactions. Can be lists of users co-commenting under a post,
    or users replying to the same comment, depending on the parameter "level".
    """
    comments = row["comments"]
    if comments:
        post = row["posts"][0]
        post_author = post["author"]
        cid_to_author = dict()
        cid_to_author[0] = post["author"]
        when = post["created"]  # when depends on the post date
        parent_to_children = defaultdict(set)
        if level == "post":
            # For post-level interactions, we consider the post author
            parent_to_children[0].add(post_author)

            for comment in comments:
                author = comment["author"]
                parent_to_children[0].add(author)

        elif level == "comment":
            for comment in comments:
                author = comment["author"]
                pid = comment["comment_parent_id"]
                parent_to_children[pid].add(author)

        for v in parent_to_children.values():
            yield (sorted(list(v)), when)


def interaction_network(
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
    G = nx.DiGraph()
    edge_weights = defaultdict(int)

    for username, row in handler.iter_all_data():
        if users and username not in users:
            continue

        for replying_user, replied_to_user, timestamp in handler.interactions(row):
            # Time filtering
            if time_range and not (time_range[0] <= timestamp <= time_range[1]):
                continue

            # User filtering
            if users and (replying_user not in users or replied_to_user not in users):
                continue

            edge_weights[(replying_user, replied_to_user)] += 1

    # Add edges with weights >= min_interactions
    for (source, target), weight in edge_weights.items():
        if weight >= min_interactions:
            G.add_edge(source, target, weight=weight)

    return G
