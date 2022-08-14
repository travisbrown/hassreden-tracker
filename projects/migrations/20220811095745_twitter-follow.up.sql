CREATE TABLE batches (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    timestamp INTEGER NOT NULL,
    next_id INTEGER UNIQUE,
    follower_additions BIGINT[],
    follower_removals BIGINT[],
    followed_additions BIGINT[],
    followed_removals BIGINT[]
);

CREATE INDEX batch_user_id ON batches (user_id);
CREATE INDEX batch_timestamp ON batches (timestamp);
CREATE INDEX batch_follower_additions ON batches USING GIN (follower_additions);
CREATE INDEX batch_follower_removals ON batches USING GIN (follower_removals);
CREATE INDEX batch_followed_additions ON batches USING GIN (followed_additions);
CREATE INDEX batch_followed_removals ON batches USING GIN (followed_removals);
