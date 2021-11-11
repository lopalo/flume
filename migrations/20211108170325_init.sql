
CREATE TABLE IF NOT EXISTS queues (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,

    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS consumers (
    id INTEGER PRIMARY KEY NOT NULL,
    queue_id INTEGER NOT NULL,
    consumer TEXT NOT NULL,
    committed_msg_id INTEGER DEFAULT -1 NOT NULL,

    UNIQUE(queue_id, consumer),
    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY NOT NULL,
    queue_id INTEGER NOT NULL,
    payload TEXT NOT NULL,

    UNIQUE(queue_id, id), -- creates a compound index
    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
);

