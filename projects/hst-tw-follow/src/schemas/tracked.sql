CREATE TABLE user (
    id INTEGER NOT NULL PRIMARY KEY,
    screen_name TEXT NOT NULL,
    target_age INTEGER,
    followers_count INTEGER NOT NULL,
    protected BOOLEAN
);

CREATE TABLE block(
    id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    PRIMARY KEY (id, target_id)
);
