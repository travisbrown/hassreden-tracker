CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    screen_name VARCHAR(20)
);

CREATE TABLE batches (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    timestamp INTEGER NOT NULL,
    next_id INTEGER UNIQUE
    --FOREIGN KEY (user_id) REFERENCES users (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE entries (
    batch_id INTEGER NOT NULL,
    user_id BIGINT NOT NULL,
    is_follower BOOLEAN NOT NULL,
    is_addition BOOLEAN NOT NULL,
    PRIMARY KEY(batch_id, user_id, is_follower),
    FOREIGN KEY(batch_id) REFERENCES batches (id)
    --FOREIGN KEY (user_id) REFERENCES users (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX batch_user_id ON batches (user_id);
CREATE INDEX batch_timestamp ON batches (timestamp);
CREATE INDEX entry_user_id ON entries (user_id);
CREATE INDEX entry_batch_id ON entries (batch_id);

CREATE FUNCTION user_insert()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
    INSERT INTO users (id) VALUES (NEW.user_id) ON CONFLICT DO NOTHING;
	RETURN NEW;
END;
$$;

--CREATE TRIGGER entry_user_insert
--    AFTER INSERT ON entries FOR EACH ROW
--    EXECUTE PROCEDURE user_insert();

--CREATE TRIGGER batch_user_insert
--    AFTER INSERT ON batches FOR EACH ROW
--    EXECUTE PROCEDURE user_insert();
