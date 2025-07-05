from collections import defaultdict
from datetime import datetime
from typing import Dict, Any

from statistics import mean, median, stdev
from ..algorithms.filters import create_text_filter, create_metadata_filter


def user_stats(handler: object) -> Dict[str, Dict[str, Any]]:
    """
    Get comprehensive statistics for all users in the dataset with one pass.

    Returns:
        Dictionary mapping usernames to their statistics
    """
    import statistics

    all_stats = defaultdict(
        lambda: {
            "total_posts": 0,
            "total_comments": 0,
            "total_interactions_sent": 0,
            "total_interactions_received": 0,
            "communities": set(),
            "first_activity": float("inf"),
            "last_activity": 0,
            "posts_deleted": 0,
            "posts_removed": 0,
            "comments_deleted": 0,
            "comments_removed": 0,
            "post_scores": [],
            "comment_scores": [],
            "post_upvotes": [],
            "post_downvotes": [],
            "comment_upvotes": [],
            "comment_downvotes": [],
            "posts_stickied": 0,
            "comments_stickied": 0,
            "posts_nsfw": 0,
            "total_awards_received": 0,
        }
    )

    # Single pass through all data
    for username, row in handler.iter_all_data():
        user_stats = all_stats[username]

        # Process posts
        if "posts" in row and row["posts"]:
            for post in row["posts"]:
                user_stats["total_posts"] += 1
                user_stats["communities"].add(post.get("community", ""))

                # Timestamps
                created = post.get("created", 0)
                if created > 0:
                    user_stats["first_activity"] = min(
                        user_stats["first_activity"], created
                    )
                    user_stats["last_activity"] = max(
                        user_stats["last_activity"], created
                    )

                # Deletion/removal status
                if post.get("is_deleted", False):
                    user_stats["posts_deleted"] += 1
                if post.get("is_removed", False):
                    user_stats["posts_removed"] += 1

                # Scores and votes
                score = post.get("score", 0)
                score_up = post.get("score_up", 0)
                score_down = post.get("score_down", 0)

                user_stats["post_scores"].append(score)
                user_stats["post_upvotes"].append(score_up)
                user_stats["post_downvotes"].append(score_down)

                # Other flags
                if post.get("is_stickied", False):
                    user_stats["posts_stickied"] += 1
                if post.get("is_nsfw", False):
                    user_stats["posts_nsfw"] += 1

                # Awards
                user_stats["total_awards_received"] += post.get("awards", 0)

        # Process comments
        if "comments" in row and row["comments"]:
            for comment in row["comments"]:
                comment_author = comment.get("author", "")
                if comment_author:
                    comment_stats = all_stats[comment_author]

                    comment_stats["total_comments"] += 1
                    comment_stats["communities"].add(comment.get("community", ""))

                    # Timestamps
                    created = comment.get("created", 0)
                    if created > 0:
                        comment_stats["first_activity"] = min(
                            comment_stats["first_activity"], created
                        )
                        comment_stats["last_activity"] = max(
                            comment_stats["last_activity"], created
                        )

                    # Deletion/removal status
                    if comment.get("is_deleted", False):
                        comment_stats["comments_deleted"] += 1
                    if comment.get("is_removed", False):
                        comment_stats["comments_removed"] += 1

                    # Scores and votes
                    score = comment.get("score", 0)
                    score_up = comment.get("score_up", 0)
                    score_down = comment.get("score_down", 0)

                    comment_stats["comment_scores"].append(score)
                    comment_stats["comment_upvotes"].append(score_up)
                    comment_stats["comment_downvotes"].append(score_down)

                    # Other flags
                    if comment.get("is_stickied", False):
                        comment_stats["comments_stickied"] += 1

                    # Awards
                    comment_stats["total_awards_received"] += comment.get("awards", 0)

        # Process interactions
        for replying_user, replied_to_user, timestamp in handler.interactions(row):
            if replying_user in all_stats:
                all_stats[replying_user]["total_interactions_sent"] += 1
            if replied_to_user in all_stats:
                all_stats[replied_to_user]["total_interactions_received"] += 1

    # Post-process statistics
    final_stats = {}
    for username, stats in all_stats.items():
        processed_stats = {
            "username": username,
            "total_posts": stats["total_posts"],
            "total_comments": stats["total_comments"],
            "total_interactions_sent": stats["total_interactions_sent"],
            "total_interactions_received": stats["total_interactions_received"],
            "communities": list(stats["communities"]),
            "num_communities": len(stats["communities"]),
            "first_activity": (
                stats["first_activity"]
                if stats["first_activity"] != float("inf")
                else None
            ),
            "last_activity": (
                stats["last_activity"] if stats["last_activity"] > 0 else None
            ),
            "posts_deleted": stats["posts_deleted"],
            "posts_removed": stats["posts_removed"],
            "comments_deleted": stats["comments_deleted"],
            "comments_removed": stats["comments_removed"],
            "posts_stickied": stats["posts_stickied"],
            "comments_stickied": stats["comments_stickied"],
            "posts_nsfw": stats["posts_nsfw"],
            "total_awards_received": stats["total_awards_received"],
        }

        # Calculate activity span
        if processed_stats["first_activity"] and processed_stats["last_activity"]:
            processed_stats["activity_span_ms"] = (
                processed_stats["last_activity"] - processed_stats["first_activity"]
            )
            processed_stats["activity_span_days"] = processed_stats[
                "activity_span_ms"
            ] / (1000 * 60 * 60 * 24)
        else:
            processed_stats["activity_span_ms"] = None
            processed_stats["activity_span_days"] = None

        # Post score statistics
        if stats["post_scores"]:
            processed_stats["post_score_mean"] = statistics.mean(stats["post_scores"])
            processed_stats["post_score_median"] = statistics.median(
                stats["post_scores"]
            )
            processed_stats["post_score_min"] = min(stats["post_scores"])
            processed_stats["post_score_max"] = max(stats["post_scores"])
            processed_stats["post_score_std"] = (
                statistics.stdev(stats["post_scores"])
                if len(stats["post_scores"]) > 1
                else 0
            )
        else:
            processed_stats.update(
                {
                    "post_score_mean": None,
                    "post_score_median": None,
                    "post_score_min": None,
                    "post_score_max": None,
                    "post_score_std": None,
                }
            )

        # Comment score statistics
        if stats["comment_scores"]:
            processed_stats["comment_score_mean"] = statistics.mean(
                stats["comment_scores"]
            )
            processed_stats["comment_score_median"] = statistics.median(
                stats["comment_scores"]
            )
            processed_stats["comment_score_min"] = min(stats["comment_scores"])
            processed_stats["comment_score_max"] = max(stats["comment_scores"])
            processed_stats["comment_score_std"] = (
                statistics.stdev(stats["comment_scores"])
                if len(stats["comment_scores"]) > 1
                else 0
            )
        else:
            processed_stats.update(
                {
                    "comment_score_mean": None,
                    "comment_score_median": None,
                    "comment_score_min": None,
                    "comment_score_max": None,
                    "comment_score_std": None,
                }
            )

        # Upvote statistics
        if stats["post_upvotes"]:
            processed_stats["post_upvote_mean"] = statistics.mean(stats["post_upvotes"])
            processed_stats["post_upvote_median"] = statistics.median(
                stats["post_upvotes"]
            )
            processed_stats["post_upvote_total"] = sum(stats["post_upvotes"])
        else:
            processed_stats.update(
                {
                    "post_upvote_mean": None,
                    "post_upvote_median": None,
                    "post_upvote_total": 0,
                }
            )

        if stats["comment_upvotes"]:
            processed_stats["comment_upvote_mean"] = statistics.mean(
                stats["comment_upvotes"]
            )
            processed_stats["comment_upvote_median"] = statistics.median(
                stats["comment_upvotes"]
            )
            processed_stats["comment_upvote_total"] = sum(stats["comment_upvotes"])
        else:
            processed_stats.update(
                {
                    "comment_upvote_mean": None,
                    "comment_upvote_median": None,
                    "comment_upvote_total": 0,
                }
            )

        # Downvote statistics
        if stats["post_downvotes"]:
            processed_stats["post_downvote_mean"] = statistics.mean(
                stats["post_downvotes"]
            )
            processed_stats["post_downvote_median"] = statistics.median(
                stats["post_downvotes"]
            )
            processed_stats["post_downvote_total"] = sum(stats["post_downvotes"])
        else:
            processed_stats.update(
                {
                    "post_downvote_mean": None,
                    "post_downvote_median": None,
                    "post_downvote_total": 0,
                }
            )

        if stats["comment_downvotes"]:
            processed_stats["comment_downvote_mean"] = statistics.mean(
                stats["comment_downvotes"]
            )
            processed_stats["comment_downvote_median"] = statistics.median(
                stats["comment_downvotes"]
            )
            processed_stats["comment_downvote_total"] = sum(stats["comment_downvotes"])
        else:
            processed_stats.update(
                {
                    "comment_downvote_mean": None,
                    "comment_downvote_median": None,
                    "comment_downvote_total": 0,
                }
            )

        # Ratios and derived metrics
        total_content = (
            processed_stats["total_posts"] + processed_stats["total_comments"]
        )
        if total_content > 0:
            processed_stats["post_to_comment_ratio"] = (
                processed_stats["total_posts"] / total_content
            )
            processed_stats["deletion_rate"] = (
                processed_stats["posts_deleted"] + processed_stats["comments_deleted"]
            ) / total_content
            processed_stats["removal_rate"] = (
                processed_stats["posts_removed"] + processed_stats["comments_removed"]
            ) / total_content
        else:
            processed_stats["post_to_comment_ratio"] = None
            processed_stats["deletion_rate"] = None
            processed_stats["removal_rate"] = None

        # Interaction ratios
        if (
            processed_stats["total_interactions_sent"]
            + processed_stats["total_interactions_received"]
            > 0
        ):
            processed_stats["interaction_ratio"] = processed_stats[
                "total_interactions_sent"
            ] / (
                processed_stats["total_interactions_sent"]
                + processed_stats["total_interactions_received"]
            )
        else:
            processed_stats["interaction_ratio"] = None

        final_stats[username] = processed_stats

    return final_stats


def community_stats(handler: object) -> Dict[str, Dict[str, Any]]:
    """
    Get comprehensive statistics for each community in the dataset.

    Returns:
        Dictionary mapping community names to their statistics
    """
    import statistics

    community_stats = defaultdict(
        lambda: {
            "posts": 0,
            "comments": 0,
            "unique_users": set(),
            "unique_posters": set(),
            "unique_commenters": set(),
            "interactions": 0,
            "first_activity": float("inf"),
            "last_activity": 0,
            "posts_deleted": 0,
            "posts_removed": 0,
            "comments_deleted": 0,
            "comments_removed": 0,
            "post_scores": [],
            "comment_scores": [],
            "post_upvotes": [],
            "post_downvotes": [],
            "comment_upvotes": [],
            "comment_downvotes": [],
            "posts_stickied": 0,
            "comments_stickied": 0,
            "posts_nsfw": 0,
            "total_awards": 0,
            "posts_locked": 0,
            "active_users_by_month": defaultdict(set),
            "post_authors_by_month": defaultdict(set),
            "comment_authors_by_month": defaultdict(set),
        }
    )

    for username, row in handler.iter_all_data():
        # Process posts
        if "posts" in row and row["posts"]:
            for post in row["posts"]:
                community = post.get("community", "unknown")
                stats = community_stats[community]

                stats["posts"] += 1
                stats["unique_users"].add(username)
                stats["unique_posters"].add(username)

                # Timestamps
                created = post.get("created", 0)
                if created > 0:
                    stats["first_activity"] = min(stats["first_activity"], created)
                    stats["last_activity"] = max(stats["last_activity"], created)

                    # Monthly activity tracking
                    month_key = datetime.fromtimestamp(created / 1000).strftime("%Y-%m")
                    stats["active_users_by_month"][month_key].add(username)
                    stats["post_authors_by_month"][month_key].add(username)

                # Content flags
                if post.get("is_deleted", False):
                    stats["posts_deleted"] += 1
                if post.get("is_removed", False):
                    stats["posts_removed"] += 1
                if post.get("is_stickied", False):
                    stats["posts_stickied"] += 1
                if post.get("is_nsfw", False):
                    stats["posts_nsfw"] += 1
                if post.get("is_locked", False):
                    stats["posts_locked"] += 1

                # Scores and votes
                score = post.get("score", 0)
                score_up = post.get("score_up", 0)
                score_down = post.get("score_down", 0)

                stats["post_scores"].append(score)
                stats["post_upvotes"].append(score_up)
                stats["post_downvotes"].append(score_down)

                # Awards
                stats["total_awards"] += post.get("awards", 0)

        # Process comments
        if "comments" in row and row["comments"]:
            for comment in row["comments"]:
                community = comment.get("community", "unknown")
                comment_author = comment.get("author", "")
                stats = community_stats[community]

                stats["comments"] += 1
                if comment_author:
                    stats["unique_users"].add(comment_author)
                    stats["unique_commenters"].add(comment_author)

                # Timestamps
                created = comment.get("created", 0)
                if created > 0:
                    stats["first_activity"] = min(stats["first_activity"], created)
                    stats["last_activity"] = max(stats["last_activity"], created)

                    # Monthly activity tracking
                    month_key = datetime.fromtimestamp(created / 1000).strftime("%Y-%m")
                    if comment_author:
                        stats["active_users_by_month"][month_key].add(comment_author)
                        stats["comment_authors_by_month"][month_key].add(comment_author)

                # Content flags
                if comment.get("is_deleted", False):
                    stats["comments_deleted"] += 1
                if comment.get("is_removed", False):
                    stats["comments_removed"] += 1
                if comment.get("is_stickied", False):
                    stats["comments_stickied"] += 1

                # Scores and votes
                score = comment.get("score", 0)
                score_up = comment.get("score_up", 0)
                score_down = comment.get("score_down", 0)

                stats["comment_scores"].append(score)
                stats["comment_upvotes"].append(score_up)
                stats["comment_downvotes"].append(score_down)

                # Awards
                stats["total_awards"] += comment.get("awards", 0)

        # Count interactions
        for _ in handler.interactions(row):
            if "posts" in row and row["posts"]:
                community = row["posts"][0].get("community", "unknown")
                community_stats[community]["interactions"] += 1

    # Post-process statistics
    final_stats = {}
    for community, stats in community_stats.items():
        processed_stats = {
            "community": community,
            "total_posts": stats["posts"],
            "total_comments": stats["comments"],
            "total_content": stats["posts"] + stats["comments"],
            "unique_users": len(stats["unique_users"]),
            "unique_posters": len(stats["unique_posters"]),
            "unique_commenters": len(stats["unique_commenters"]),
            "interactions": stats["interactions"],
            "first_activity": (
                stats["first_activity"]
                if stats["first_activity"] != float("inf")
                else None
            ),
            "last_activity": (
                stats["last_activity"] if stats["last_activity"] > 0 else None
            ),
            "posts_deleted": stats["posts_deleted"],
            "posts_removed": stats["posts_removed"],
            "comments_deleted": stats["comments_deleted"],
            "comments_removed": stats["comments_removed"],
            "posts_stickied": stats["posts_stickied"],
            "comments_stickied": stats["comments_stickied"],
            "posts_nsfw": stats["posts_nsfw"],
            "posts_locked": stats["posts_locked"],
            "total_awards": stats["total_awards"],
        }

        # Activity span
        if processed_stats["first_activity"] and processed_stats["last_activity"]:
            processed_stats["activity_span_ms"] = (
                processed_stats["last_activity"] - processed_stats["first_activity"]
            )
            processed_stats["activity_span_days"] = processed_stats[
                "activity_span_ms"
            ] / (1000 * 60 * 60 * 24)
        else:
            processed_stats["activity_span_ms"] = None
            processed_stats["activity_span_days"] = None

        # Monthly activity stats
        monthly_users = [
            len(users) for users in stats["active_users_by_month"].values()
        ]
        monthly_posters = [
            len(users) for users in stats["post_authors_by_month"].values()
        ]
        monthly_commenters = [
            len(users) for users in stats["comment_authors_by_month"].values()
        ]

        processed_stats["active_months"] = len(stats["active_users_by_month"])
        if monthly_users:
            processed_stats["avg_monthly_users"] = statistics.mean(monthly_users)
            processed_stats["peak_monthly_users"] = max(monthly_users)
        else:
            processed_stats["avg_monthly_users"] = None
            processed_stats["peak_monthly_users"] = None

        if monthly_posters:
            processed_stats["avg_monthly_posters"] = statistics.mean(monthly_posters)
            processed_stats["peak_monthly_posters"] = max(monthly_posters)
        else:
            processed_stats["avg_monthly_posters"] = None
            processed_stats["peak_monthly_posters"] = None

        if monthly_commenters:
            processed_stats["avg_monthly_commenters"] = statistics.mean(
                monthly_commenters
            )
            processed_stats["peak_monthly_commenters"] = max(monthly_commenters)
        else:
            processed_stats["avg_monthly_commenters"] = None
            processed_stats["peak_monthly_commenters"] = None

        # Content ratios
        if processed_stats["total_content"] > 0:
            processed_stats["post_to_comment_ratio"] = (
                processed_stats["total_posts"] / processed_stats["total_content"]
            )
            processed_stats["deletion_rate"] = (
                processed_stats["posts_deleted"] + processed_stats["comments_deleted"]
            ) / processed_stats["total_content"]
            processed_stats["removal_rate"] = (
                processed_stats["posts_removed"] + processed_stats["comments_removed"]
            ) / processed_stats["total_content"]
            processed_stats["sticky_rate"] = (
                processed_stats["posts_stickied"] + processed_stats["comments_stickied"]
            ) / processed_stats["total_content"]
        else:
            processed_stats["post_to_comment_ratio"] = None
            processed_stats["deletion_rate"] = None
            processed_stats["removal_rate"] = None
            processed_stats["sticky_rate"] = None

        if processed_stats["total_posts"] > 0:
            processed_stats["nsfw_rate"] = (
                processed_stats["posts_nsfw"] / processed_stats["total_posts"]
            )
            processed_stats["lock_rate"] = (
                processed_stats["posts_locked"] / processed_stats["total_posts"]
            )
        else:
            processed_stats["nsfw_rate"] = None
            processed_stats["lock_rate"] = None

        # User engagement metrics
        if processed_stats["unique_users"] > 0:
            processed_stats["avg_posts_per_user"] = (
                processed_stats["total_posts"] / processed_stats["unique_users"]
            )
            processed_stats["avg_comments_per_user"] = (
                processed_stats["total_comments"] / processed_stats["unique_users"]
            )
            processed_stats["avg_content_per_user"] = (
                processed_stats["total_content"] / processed_stats["unique_users"]
            )
        else:
            processed_stats["avg_posts_per_user"] = None
            processed_stats["avg_comments_per_user"] = None
            processed_stats["avg_content_per_user"] = None

        # Score statistics for posts
        if stats["post_scores"]:
            processed_stats["post_score_mean"] = statistics.mean(stats["post_scores"])
            processed_stats["post_score_median"] = statistics.median(
                stats["post_scores"]
            )
            processed_stats["post_score_min"] = min(stats["post_scores"])
            processed_stats["post_score_max"] = max(stats["post_scores"])
            processed_stats["post_score_std"] = (
                statistics.stdev(stats["post_scores"])
                if len(stats["post_scores"]) > 1
                else 0
            )
            processed_stats["post_upvote_total"] = sum(stats["post_upvotes"])
            processed_stats["post_downvote_total"] = sum(stats["post_downvotes"])
            processed_stats["post_upvote_mean"] = statistics.mean(stats["post_upvotes"])
            processed_stats["post_downvote_mean"] = statistics.mean(
                stats["post_downvotes"]
            )
        else:
            processed_stats.update(
                {
                    "post_score_mean": None,
                    "post_score_median": None,
                    "post_score_min": None,
                    "post_score_max": None,
                    "post_score_std": None,
                    "post_upvote_total": 0,
                    "post_downvote_total": 0,
                    "post_upvote_mean": None,
                    "post_downvote_mean": None,
                }
            )

        # Score statistics for comments
        if stats["comment_scores"]:
            processed_stats["comment_score_mean"] = statistics.mean(
                stats["comment_scores"]
            )
            processed_stats["comment_score_median"] = statistics.median(
                stats["comment_scores"]
            )
            processed_stats["comment_score_min"] = min(stats["comment_scores"])
            processed_stats["comment_score_max"] = max(stats["comment_scores"])
            processed_stats["comment_score_std"] = (
                statistics.stdev(stats["comment_scores"])
                if len(stats["comment_scores"]) > 1
                else 0
            )
            processed_stats["comment_upvote_total"] = sum(stats["comment_upvotes"])
            processed_stats["comment_downvote_total"] = sum(stats["comment_downvotes"])
            processed_stats["comment_upvote_mean"] = statistics.mean(
                stats["comment_upvotes"]
            )
            processed_stats["comment_downvote_mean"] = statistics.mean(
                stats["comment_downvotes"]
            )
        else:
            processed_stats.update(
                {
                    "comment_score_mean": None,
                    "comment_score_median": None,
                    "comment_score_min": None,
                    "comment_score_max": None,
                    "comment_score_std": None,
                    "comment_upvote_total": 0,
                    "comment_downvote_total": 0,
                    "comment_upvote_mean": None,
                    "comment_downvote_mean": None,
                }
            )

        # Overall score statistics
        all_scores = stats["post_scores"] + stats["comment_scores"]
        if all_scores:
            processed_stats["overall_score_mean"] = statistics.mean(all_scores)
            processed_stats["overall_score_median"] = statistics.median(all_scores)
            processed_stats["overall_score_std"] = (
                statistics.stdev(all_scores) if len(all_scores) > 1 else 0
            )
        else:
            processed_stats.update(
                {
                    "overall_score_mean": None,
                    "overall_score_median": None,
                    "overall_score_std": None,
                }
            )

        final_stats[community] = processed_stats

    return final_stats
