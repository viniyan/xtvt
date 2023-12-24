import logging
import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from generic_utils import date_to_iso_seconds

logger = logging.getLogger()


def connect_db(db_host, db_port, db_user, db_pswd, db_name):
    db_uri = f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}/{db_name}"
    engine = db.create_engine(db_uri, echo=False)
    return engine


def handle_conflict(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["id"])
    conn.execute(do_nothing_stmt)


def append_records(engine, df, chunk_size, table_name):
    df.to_sql(
        con=engine,
        name=table_name,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method=handle_conflict,
    )


def append_commits(engine, df, chunk_size):
    table_name = "bb_commits"
    append_records(engine, df, chunk_size, table_name)


def append_pullrequests(engine, df, chunk_size):
    table_name = "bb_pullrequests"
    append_records(engine, df, chunk_size, table_name)


def query_authors(engine):
    sql = """
        SELECT author_id, author, count(*) as commits
        FROM bb_commits
        GROUP BY author_id, author;
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    # Sort by author name
    df["author.upper"] = df["author"].str.upper()
    df.sort_values(by="author.upper", inplace=True, ascending=True)

    # Drop temporary and unnecessary column
    del df["author.upper"]
    del df["commits"]

    return df


def query_author_repos(engine, author_id):
    sql = """
        SELECT repo, count(*) as commits
        FROM bb_commits
        WHERE author_id = :author_id OR author= :author_id
        GROUP BY repo;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df

def query_repos(engine):
    sql = """
        SELECT repo, count(*) as commits
        FROM bb_commits
        GROUP BY repo;
    """

    stmt = text(sql)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df

def query_author_commits(engine, author_id):
    sql = """
        SELECT 
            author,
            author_id,
            branch,
            created_at,
            id,
            repo 
        FROM bb_commits
        WHERE author_id = :author_id OR author= :author_id;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    # Sort by date (chronologically)
    df.sort_values(by="created_at", inplace=True, ascending=True)

    return df


def query_author_pullrequests(engine, author_id):
    sql = """
        SELECT * 
        FROM bb_pullrequests
        WHERE author_id = :author_id;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def query_unprocessed_commits(engine):
    # Get all commits that have not been processed yet
    sql = """
        SELECT
            id,
            repo
        FROM
            bb_commits
        WHERE
            diff IS NULL;
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def update_commit_diff(engine, commit_id, diff):
    sql = """
        UPDATE bb_commits
        SET diff = :diff
        WHERE id = :commit_id;        
    """

    stmt = text(sql)
    stmt = stmt.bindparams(commit_id=commit_id, diff=diff)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    if result.rowcount > 0:
        print(f"Updated commit: {commit_id}")
        return True

    return False


def query_commit(engine, commit_id):
    # Get commit details
    sql = """
        SELECT
            diff
        FROM
            bb_commits
        WHERE
            id = :commit_id;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(commit_id=commit_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def insert_sync_history(engine, table_name, repo):
    sql = """
        INSERT INTO bb_sync_history(tbl, repo, updated_at)
        VALUES(:table_name, :repo, :updated_at)
        ON CONFLICT (tbl, repo) DO
        UPDATE SET updated_at = :updated_at;    
    """
    print(f"Inserting sync history for {repo}...")

    now = datetime.now()
    dt = date_to_iso_seconds(now)

    stmt = text(sql)
    stmt = stmt.bindparams(table_name=table_name, repo=repo, updated_at=dt)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    if result.rowcount > 0:
        print(f"Success")
        return True

    return False


def query_sync_history(engine, table_name, repo):
    sql = """
        SELECT 
            updated_at
        FROM bb_sync_history
        WHERE tbl = :table_name AND repo = :repo;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(table_name=table_name, repo=repo)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def query_diffs_by_author(engine, author):
    # Get commit details for a specific author
    sql = """
        SELECT
            diff
        FROM
            bb_commits
        WHERE
            author = :author;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author=author)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def query_all_commits(engine):
    # Get all commit details
    sql = """
        SELECT
            id,
            created_at
        FROM
            bb_commits;
    """

    stmt = text(sql)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def query_all_repo_commits(engine, repo_name):
    sql = """
        SELECT
            id,
            created_at
        FROM
            bb_commits
        WHERE
            repo = :repo_name;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(repo_name=repo_name)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append({"commit_id": record.id, "diff": record.diff})

    df = pd.DataFrame(records)

    return df

def query_commits_by_day_and_author(engine, author_id, date):
    # Obtém os commits do banco de dados
    sql = """
        SELECT 
            author,
            author_id,
            branch,
            created_at,
            id,
            repo 
        FROM bb_commits
        WHERE (author_id = :author_id OR author = :author_id)
            AND DATE(created_at) = :date;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id, date=date)

    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df



def query_all_commit_count_by_day_and_author(engine, author_id):
    # Obtém os commits do banco de dados
    sql = """
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as commit_count
        FROM bb_commits
        WHERE (author_id = :author_id OR author = :author_id)
        GROUP BY date;
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id)

    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df

def query_all_pullrequests(engine):
    sql = """
        SELECT * 
        FROM bb_pullrequests;
    """

    stmt = text(sql)
    #stmt = stmt.bindparams(author_id=author_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df


def query_last_author_pullrequest(engine, author_id):
    sql = """
        SELECT * 
        FROM bb_pullrequests
        WHERE (author_id = :author_id OR author = :author_id);
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author_id=author_id)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df

def query_last_pullrequest(engine):
    sql = """
        SELECT * 
        FROM bb_pullrequests;
    """

    stmt = text(sql)
    stmt = stmt.bindparams()
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df

def append_mtr_records(engine, df, chunk_size, table_name):
    df.set_index('commit_id', inplace=True)
    if 'id' not in df.columns:
        # Se não estiver presente, você pode adicionar uma coluna 'id' com valores únicos
        df['id'] = range(1, len(df) + 1)

    df.to_sql(
        con=engine,
        name=table_name,
        if_exists="replace",
        index=True,
        index_label='commit_id',
        chunksize=chunk_size,
    )

def append_mtr(engine, df, chunk_size):
    table_name = "bb_mtr"
    append_mtr_records(engine, df, chunk_size, table_name)


def query_author_mtr(engine, author):
    sql = """
        SELECT * 
        FROM bb_mtr
        WHERE (author = :author);
    """

    stmt = text(sql)
    stmt = stmt.bindparams(author=author)
    with engine.begin() as conn:
        result = conn.execute(stmt)

    records = []
    for record in result:
        records.append(record)

    df = pd.DataFrame(records)

    return df