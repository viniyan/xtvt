from flask import Flask, jsonify, request
from bitbucket import Bitbucket
from datetime import datetime
import pandas as pd
from db_utils import (
    connect_db,
    append_commits,
    append_pullrequests,
    query_authors,
    query_author_repos,
    query_author_commits,
    query_author_pullrequests,
    query_unprocessed_commits,
    update_commit_diff,
    query_commit,
    query_sync_history,
    insert_sync_history,
)

app = Flask(__name__)

# Load the app configuration
app.config.from_pyfile("settings.py")


@app.route("/authors", methods=["GET"])
def get_authors():
    # Connect to database
    engine = init_db_engine()

    # Return a list of authors
    df = query_authors(engine)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/authors/<author_id>/repos", methods=["GET"])
def get_author_repos(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_repos(engine, author_id)

    result = {
        "statusCode": 200,
        "data": df["repo"].tolist(),
    }

    return jsonify(result)


@app.route("/authors/<author_id>/commits", methods=["GET"])
def get_author_commits(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_commits(engine, author_id)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/authors/<author_id>/pullrequests", methods=["GET"])
def get_author_pullrequests(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_pullrequests(engine, author_id)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/commits/<commit_id>/diff", methods=["GET"])
def get_commit_diff(commit_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_commit(engine, commit_id)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records")[0] if len(df) > 0 else {},
    }

    return jsonify(result)


@app.route("/sync/commits", methods=["POST"])
def sync_commits():
    # Retrieve query parameters
    page_size = request.args.get("page_size", default=20, type=int)

    # Retrieve repositories
    repos = app.config["BITBUCKET_REPOS"]

    # Connect to database
    engine = init_db_engine()

    total_count = 0

    for repo in repos:
        workspace, repo_slug = repo.split("/")

        # Get the sync history for the repo
        table_name = "bb_commits"
        df_s = query_sync_history(engine, table_name, repo)
        last_synced_at = None
        if len(df_s) > 0:
            updated_at = df_s.iloc[0]["updated_at"]
            last_synced_at = datetime.fromisoformat(updated_at)
            print(f"Last synced at: {last_synced_at}")

        # Initialize Bitbucket client
        bitbucket = Bitbucket(
            app.config["BITBUCKET_USERNAME"],
            app.config["BITBUCKET_APP_PASSWORD"],
            workspace,
            repo_slug,
        )

        page = 1
        count = 0

        while True:
            # Return a list of commits
            records = bitbucket.list_commits(page=page, page_size=page_size)

            if len(records) == 0:
                break

            df = pd.DataFrame(records)

            append_commits(engine, df, app.config["DB_CHUNK_SIZE"])

            page += 1
            count += len(records)

            look_more = True
            for index, row in df.iterrows():
                created_at = datetime.fromisoformat(row["created_at"])
                if last_synced_at is not None and created_at < last_synced_at:
                    print(
                        f"Reached last synced record: {created_at} | {last_synced_at}"
                    )
                    look_more = False
                    break

            if not look_more:
                break

        # Update the sync history for the repo
        insert_sync_history(engine, table_name, repo)

        total_count += count

    result = {
        "statusCode": 200,
        "count": total_count,
    }
    return jsonify(result)


@app.route("/sync/pullrequests", methods=["POST"])
def sync_pullrequests():
    # Retrieve query parameters
    page_size = request.args.get("page_size", default=10, type=int)

    # Retrieve repositories
    repos = app.config["BITBUCKET_REPOS"]

    # Connect to database
    engine = init_db_engine()

    total_count = 0

    for repo in repos:
        workspace, repo_slug = repo.split("/")

        # Initialize Bitbucket client
        bitbucket = Bitbucket(
            app.config["BITBUCKET_USERNAME"],
            app.config["BITBUCKET_APP_PASSWORD"],
            workspace,
            repo_slug,
        )

        page = 1
        count = 0

        while True:
            # Return a list of pull requests
            records = bitbucket.list_pullrequests(page=page, page_size=page_size)

            if len(records) == 0:
                break

            df = pd.DataFrame(records)

            append_pullrequests(engine, df, app.config["DB_CHUNK_SIZE"])

            page += 1
            count += len(records)

        total_count += count

    result = {
        "statusCode": 200,
        "count": total_count,
    }
    return jsonify(result)


@app.route("/sync/diffs", methods=["POST"])
def sync_diffs():
    # Connect to database
    engine = init_db_engine()

    # Return a list of unprocessed commits
    df = query_unprocessed_commits(engine)

    count = 0

    for index, row in df.iterrows():
        commit_id = row["id"]
        repo = row["repo"]
        workspace, repo_slug = repo.split("/")

        print(f"Processing commit: {commit_id}")

        # Initialize Bitbucket client
        bitbucket = Bitbucket(
            app.config["BITBUCKET_USERNAME"],
            app.config["BITBUCKET_APP_PASSWORD"],
            workspace,
            repo_slug,
        )

        # Return a commit diff
        diff = bitbucket.get_diff_for_commit(commit_id)

        # Update the commit diff
        update_commit_diff(engine, commit_id, diff)

        count += 1

    result = {
        "statusCode": 200,
        "data": {
            "count": count,
        },
    }
    return jsonify(result)


def init_db_engine():
    engine = connect_db(
        app.config["DB_HOST"],
        app.config["DB_PORT"],
        app.config["DB_USER"],
        app.config["DB_PSWD"],
        app.config["DB_NAME"],
    )
    return engine


if __name__ == "__main__":
    app.run(port=8081)
