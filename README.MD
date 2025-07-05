# Scored Data Handler
**Efficiently handles large datasets from Scored.co**

```python


# Initialize handler
handler = ScoredDatasetHandler("/path/to/dataset")

# Basic iteration
for username, thread in handler.iter_all_data():
    print(f"Processing {username}")
    post = thread['posts'][0]
    comments = thread['comments']

# Filter posts by keywords
text_filter = create_text_filter(["politics", "election"])
for username, post in handler.iter_posts(text_filter=text_filter):
    print(f"Political post by {username}")

# Filter comments by metadata
conditions = {"score__gt": 10, "is_deleted": False}  # not deleted and score greater than 10
metadata_filter = create_metadata_filter(conditions)
for username, comment, post in handler.iter_comments(metadata_filter=metadata_filter):
    # do something with the comment
    pass

# Build interaction network for specific time period
start_time = datetime_to_timestamp(datetime(2022, 1, 1))
end_time = datetime_to_timestamp(datetime(2022, 12, 31))
network = handler.build_interaction_network(time_range=(start_time, end_time))

# Get time slice
slice_2022 = handler.get_time_slice(start_time, end_time)
# this sliced handler automatically filters data
network_2022 = slice_2022.build_interaction_network()

# User analysis
user_stats = handler.get_user_stats("specific_user")
print(f"User has {user_stats['total_posts']} posts")
all_stats = handler.get_all_user_stats()

# Community analysis
community_stats = handler.get_community_stats()
for community, stats in community_stats.items():
    print(f"{community}: {stats['unique_users']} users")

# Discussion tree 
for _, thread in handler.iter_all_data():
    tree = handler.build_discussion_tree(node_attrs=True)
    for edge, attrs in tree.edges(data=True): # nx.DiGraph
        print(f"Seconds from {edge[0]} {edge[1]}: {attrs['time_diff_seconds']}")
```
