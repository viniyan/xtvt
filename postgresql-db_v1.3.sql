CREATE TABLE IF NOT EXISTS bb_commits (
    id VARCHAR(50) PRIMARY KEY,
    author VARCHAR(255),
    author_id VARCHAR(50),
    msg TEXT,
    created_at VARCHAR(255),
    repo VARCHAR(255),
    diff TEXT, 
    branch TEXT
);

CREATE TABLE IF NOT EXISTS bb_pullrequests (
    id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    description TEXT,
    state TEXT,
    author VARCHAR(255),
    author_id VARCHAR(50),
    repo VARCHAR(255),
    created_at VARCHAR(50),
    updated_at VARCHAR(50), 
    branch TEXT
);

CREATE TABLE IF NOT EXISTS bb_sync_history (
    tbl VARCHAR(50),
    repo VARCHAR(255),
    updated_at VARCHAR(50),
    PRIMARY KEY (tbl, repo)
);

CREATE TABLE IF NOT EXISTS bb_mtr (
    id VARCHAR(50),
    repository VARCHAR(255),
    author VARCHAR(255),
    commit_message VARCHAR(255),
    created_at VARCHAR(50),
    commit_id VARCHAR(255) PRIMARY KEY
);