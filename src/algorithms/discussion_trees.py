from typing import Dict
import networkx as nx


def discussion_tree(row: Dict, node_attrs: bool = True) -> nx.DiGraph:
    """
    Build a discussion tree from a row (post with comments) using NetworkX.

    Creates a directed graph where:
    - Root node is the original post (id=0)
    - Each comment is a node with its comment ID
    - Edges represent reply relationships (parent -> child)
    - Node attributes include: author, content, created time, score, etc.
    - Edge attributes include: reply_depth, time_diff_seconds

    Args:
        row: Row data containing posts and comments

    Returns:
        NetworkX DiGraph representing the discussion tree
    """
    G = nx.DiGraph()

    # Get the main post
    posts = row.get("posts", [])
    if not posts:
        return G  # Return empty graph if no posts

    post = posts[0]  # Assuming first post is the main post

    # Add root node (the post itself)
    post_id = 0
    if node_attrs:
        # Add post node with attributes
        G.add_node(
            post_id,
            node_type="post",
            author=post.get("author", ""),
            content=post.get("content", ""),
            title=post.get("title", ""),
            created=post.get("created", 0),
            score=post.get("score", 0),
            score_up=post.get("score_up", 0),
            score_down=post.get("score_down", 0),
            community=post.get("community", ""),
            is_deleted=post.get("is_deleted", False),
            is_removed=post.get("is_removed", False),
            is_stickied=post.get("is_stickied", False),
            is_nsfw=post.get("is_nsfw", False),
            is_locked=post.get("is_locked", False),
            awards=post.get("awards", 0),
        )
    else:
        # Add post node with minimal attributes
        G.add_node(
            post_id,
            author=post.get("author", ""),
            created=post.get("created", 0),
            node_type="post",
        )

    # Get comments
    comments = row.get("comments", [])
    if not comments:
        return G  # Return graph with just the post if no comments

    # Create mapping of comment ID to comment data
    comment_map = {}
    for comment in comments:
        comment_id = comment.get("id")
        if comment_id is not None:
            comment_map[comment_id] = comment

    # Add comment nodes and edges
    for comment in comments:
        comment_id = comment.get("id")
        if comment_id is None:
            continue

        if node_attrs:
            # Add comment node with attributes
            G.add_node(
                comment_id,
                node_type="comment",
                author=comment.get("author", ""),
                content=comment.get("content", ""),
                created=comment.get("created", 0),
                score=comment.get("score", 0),
                score_up=comment.get("score_up", 0),
                score_down=comment.get("score_down", 0),
                community=comment.get("community", ""),
                is_deleted=comment.get("is_deleted", False),
                is_removed=comment.get("is_removed", False),
                is_stickied=comment.get("is_stickied", False),
                awards=comment.get("awards", 0),
                in_reply_to_id=comment.get("in_reply_to_id", 0),
            )
        else:
            # Add comment node with minimal attributes

            G.add_node(
                comment_id,
                author=comment.get("author", ""),
                created=comment.get("created", 0),
                node_type="comment",
            )

    # Add edges
    for comment in comments:
        comment_id = comment.get("id")
        if comment_id is None:
            continue
        # Determine parent (what this comment is replying to)
        parent_id = comment.get("in_reply_to_id", 0)

        # Add edge from parent to this comment
        if parent_id in G.nodes():
            # Calculate time difference
            parent_created = G.nodes[parent_id].get("created", 0)
            comment_created = comment.get("created", 0)
            time_diff = (
                comment_created - parent_created
                if comment_created > 0 and parent_created > 0
                else 0
            )

            G.add_edge(
                parent_id,
                comment_id,
                time_diff_seconds=(
                    time_diff / 1000 if time_diff > 0 else 0
                ),  # Convert ms to seconds
            )
