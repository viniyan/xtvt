from flask import Flask, jsonify, request, render_template
from collections import Counter, defaultdict
from bitbucket import Bitbucket
from datetime import datetime, timedelta
import logging
import pandas as pd
from flask_cors import cross_origin
from db_utils import (
    connect_db,
    append_commits,
    append_pullrequests,
    query_authors,
    query_author_repos,
    query_repos,
    query_author_commits,
    query_author_pullrequests,
    query_unprocessed_commits,
    update_commit_diff,
    query_commit,
    query_sync_history,
    insert_sync_history,
    query_diffs_by_author,
    query_all_commits,
    query_all_repo_commits,
    query_commits_by_day_and_author,
    query_all_commit_count_by_day_and_author,
    query_all_pullrequests,
    query_last_author_pullrequest,
    query_last_pullrequest,
    append_mtr,
    query_author_mtr,
)
import requests
import concurrent.futures
import difflib
import json

app = Flask(__name__)


# Load the app configuration
app.config.from_pyfile("settings.py")


@app.route("/authors", methods=["GET"])
@cross_origin()
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

@app.route("/repos", methods=["GET"])
@cross_origin()
def get_repos():
    # Connect to database
    engine = init_db_engine()

    # Return a list of authors
    df = query_repos(engine)

    data=[]
    bitbucket_api_url = app.config["BITBUCKET_API_BASE_URL"]
    
    # check on bitbucket if this repo exists
    for r in df.to_dict(orient="records"):
        response = requests.get(f'{bitbucket_api_url}/{r["repo"]}')
        response=response.json()
        if response["type"] != 'error':
            data.append(r)
    
    result = {
        "statusCode": 200,
        "data": data,
    }

    return jsonify(result)

@app.route("/authors/<path:author_id>/repos", methods=["GET"])
@cross_origin()
def get_author_repos(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_repos(engine, author_id)

    result=None
    if "repo" in df.columns:
        result = {
            "statusCode": 200,
            "data": df["repo"].tolist(),
        }
    else:
        result = {
            "statusCode": 200,
            "data": "No repo found",  # or handle the absence of "repo" column as needed
        }

    return jsonify(result)


@app.route("/authors/<author_id>/commits", methods=["GET"])
@cross_origin()
def get_author_commits(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_commits(engine, author_id)

    df['created_at'] = pd.to_datetime(df['created_at'])

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/authors/<author>/pullrequests", methods=["GET"])
@cross_origin()
def get_author_pullrequests(author):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_author_pullrequests(engine, author)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/commits/<commit_id>/diff", methods=["GET"])
@cross_origin()
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
@cross_origin()
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
@cross_origin()
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
@cross_origin()
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




@app.route("/<author>/diff", methods=["GET"])
@cross_origin()
def get_author_diff(author):
    # Connect to database
    engine = init_db_engine()

    # Get the diffs from the database
    df = query_diffs_by_author(engine, author)

    # Format the diffs as requested
    formatted_diffs = []
    for index, row in df.iterrows():
        diff_content = row['diff']
        formatted_diff = parse_git_diff(diff_content)
        formatted_diffs.append(formatted_diff)

    result = {
        "statusCode": 200,
        "data": formatted_diffs,
    }

    return jsonify(result)

def parse_git_diff(diff_content):
    diff_lines = diff_content.splitlines()

    # Verifica se o diff tem o formato esperado
    if not diff_lines or not diff_lines[0].startswith('diff --git'):
        raise ValueError("O conteúdo do diff não está no formato esperado.")

    # Extrai o nome do arquivo
    file_name_line = diff_lines[0][13:]  # Remove "diff --git a/" para obter o nome do arquivo
    file_name = file_name_line.split(" ")[0]

    # Inicializa listas para armazenar linhas adicionadas, removidas e informações de seção modificada
    added_lines = []
    removed_lines = []
    section_line = None

    # Inicializa o objeto Differ
    differ = difflib.Differ()

    # Itera sobre as linhas do diff
    for line in diff_lines:
        if line.startswith('@@'):
            section_line = line
        elif line.startswith('+'):
            added_lines.append(line[3:] if line.startswith('+++') else line[1:])
        elif line.startswith('-'):
            removed_lines.append(line[3:] if line.startswith('---') else line[1:])

    # Remove o primeiro valor de cada lista
    added_lines = added_lines[1:]
    removed_lines = removed_lines[1:]

    # Retorna um dicionário com as informações extraídas
    diff_info = {
        'file_name': file_name,
        'section_line': section_line,
        'added_content': '\n'.join(added_lines),
        'removed_content': '\n'.join(removed_lines),
    }

    return diff_info


@app.route("/all_commits", methods=["GET"])
@cross_origin()
def get_all_commits():
    # Connect to database
    engine = init_db_engine()

    # Return a list of all commits
    df = query_all_commits(engine)

    df['created_at'] = pd.to_datetime(df['created_at'])


    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)


@app.route("/<repo_name>/all_commits", methods=["GET"])
@cross_origin()
def get_repo_commits(repo_name):
    # Connect to database
    engine = init_db_engine()

    # Return a list of all commits
    df = query_all_commits(engine)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    return jsonify(result)    


@app.route("/authors/<author_id>/<date>/commits", methods=["GET"])
@cross_origin()
def get_author_commits_by_date(author_id, date):
    # Connect to database
    engine = init_db_engine()

    # Retorna um DataFrame com os commits do autor
    df = query_commits_by_day_and_author(engine, author_id, date)

    # Converta a coluna 'created_at' para datetime, se ainda não for
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Filtre os dados com base na data
    df_filtered = df[df['created_at'].dt.strftime('%Y-%m-%d') == pd.to_datetime(date).strftime('%Y-%m-%d')]

    result = {
        "statusCode": 200,
        "data": {
            "commits": df_filtered.to_dict(orient="records"),
            "commit_count": len(df_filtered)
        },
    }

    return jsonify(result)


@app.route("/authors/<author_id>/commit_count", methods=["GET"])
@cross_origin()
def get_author_all_commit_count_by_date(author_id):
    # Connect to database
    engine = init_db_engine()

    # Retorna um DataFrame com os commits do autor, agrupados por dia
    df = query_all_commit_count_by_day_and_author(engine, author_id)

    result = {
        "statusCode": 200,
        "data": {
            "commit_count": df.to_dict(orient="records"),
        },
    }

    return jsonify(result)

@app.route("/pullrequests", methods=["GET"])
@cross_origin()
def get_all_pullrequests():
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_all_pullrequests(engine)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    data = result["data"]

    return jsonify(result)
    #return data

@app.route("/count_pullrequests", methods=["GET"])
@cross_origin()
def count_pullrequests():
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_all_pullrequests(engine)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }


    pullrequests_data = result
    
    authors_all = [item["author"] for item in pullrequests_data.get("data", [])]
    name_counts = Counter(authors_all)
    return dict(name_counts)
   
    

@app.route("/<author_id>/last_pullrequest", methods=["GET"])
@cross_origin()
def get_author_last_pullrequests(author_id):
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_last_author_pullrequest(engine, author_id)

    result = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    all_dates = [item["created_at"] for item in result.get("data", [])]

    formatted_dates = [datetime.fromisoformat(date.replace('T', ' ').replace('Z', '')) for date in all_dates]

    # Ordenar em ordem decrescente
    ordered_dates = sorted(formatted_dates, reverse=True)

    return ordered_dates

@app.route("/last_pullrequest", methods=["GET"])
@cross_origin()
def get_last_pullrequest():
    # Connect to database
    engine = init_db_engine()

    # Return a list of commits
    df = query_last_pullrequest(engine)

    result1 = {
        "statusCode": 200,
        "data": df.to_dict(orient="records"),
    }

    all_dates = [item["created_at"] for item in result1.get("data", [])]

    all_authors = [item["author"] for item in result1.get("data", [])]

    formatted_dates = [datetime.fromisoformat(date.replace('T', ' ').replace('Z', '')) for date in all_dates]

    dates_by_author = {}

    for author in set(all_authors):
        author_dates = [datetime.fromisoformat(date.replace('T', ' ').replace('Z', '')) for date, a in zip(all_dates, all_authors) if a == author]
        ordered_dates = sorted(author_dates, reverse=True)
        dates_by_author[author] = ordered_dates[0] if ordered_dates else None

    result = {
        "statusCode": 200,
        "data": {
            "dates_by_author": dates_by_author,
        },
    }

    return result
      

@app.route("/sync_mtr", methods=["POST"])
@cross_origin()
def sync_mtr(page=1, page_size=100):

    repos = app.config["BITBUCKET_REPOS"]

    engine = init_db_engine()

    # total_count = 0

    for repo in repos:
        workspace, repo_slug = repo.split("/")
        

    # Initialize Bitbucket client
    bitbucket = Bitbucket(
        app.config["BITBUCKET_USERNAME"],
        app.config["BITBUCKET_APP_PASSWORD"],
        workspace,
        repo_slug,
    )

    result = bitbucket.sync_mtr(page=page, page_size=page_size)

    df = pd.DataFrame(result)

    append_mtr(engine, df, app.config["DB_CHUNK_SIZE"])

    return result

@app.route("/<author>/mtr", methods=["GET"])
@cross_origin()
def get_author_mtr(author):
    # Connect to database
    engine = init_db_engine()

    # Get the diffs from the database
    df = query_author_mtr(engine, author)

    commit_messages_by_author = df.groupby('author')[['commit_message', 'created_at']].agg(list).to_dict()

    result = {
        "author": author,
        "tasks": {}
    }

    commit_messages = commit_messages_by_author['commit_message']
    commit_dates = commit_messages_by_author['created_at']


    result = {
        "author": author,
        "tasks": []
    }


    for author, messages in commit_messages.items():
        mean_time_to_repair = commit_dates.get(author, [])
        task_map = defaultdict(list)

        for message, date in zip(messages, mean_time_to_repair):
            
            task_name_index = message.find('/') 
            task_name = message[:task_name_index] if task_name_index != -1 else message

            task_map[task_name].append({
                "dates": date,
                "task": message
            })
            ############################
            # task_map[message].append({
            #     "dates": date,
            #     "task": message
            # })

        task_entries = []

        # for task_name, entries in task_map.items():
        #     task_entry = {
        #         "mean_time_to_repair": [
        #             {"dates": entry["dates"], "task": entry["task"]} for entry in entries
        #         ]
        #     }
        #     task_entries.append(task_entry)

        error_qty = []
        ##########################################
        for entries in task_map.values():
            #print(task_map.values())
            entries.sort(key=lambda x: x["dates"], reverse=True)


            task_value = entries[0]["task"]
            #print(entries)
            task_name_index = task_value.find('/')  # Obter a parte antes do '/'
            if task_name_index == -1:
                error_qty.append(task_value)
            else:
                task_name = task_value[:task_name_index]
                task_entry = {"mean_time_to_repair": [{"task": task_name, "dates": entry["dates"]} for entry in entries]}
                task_entries.append(task_entry)
                #print(task_name)
        ###############################################       

        task = {
            "author": author,
            "wrong_commit_message_qty": len(error_qty),
            "tasks": task_entries,
        }

        result["tasks"].append(task)

    



    for author_entry in result["tasks"]:
        for task_entry in author_entry["tasks"]:
            mean_time_to_repair = task_entry["mean_time_to_repair"]

            if len(mean_time_to_repair) > 1:
                dates = [datetime.fromisoformat(entry['dates']) for entry in mean_time_to_repair]

                # Calcula a diferença total de tempo em segundos
                total_time_diff = sum((dates[j - 1] - dates[j]).total_seconds() for j in range(1, len(dates)))

                # Converte a diferença total para horas
                total_hours = total_time_diff / 3600

                mean_time_to_repair[0]["total_difference"] = total_hours


    mtr_times = []
    


    def parse_mtr_time(time_str):
        try:
            # Tenta analisar no formato atual
            return datetime.strptime(time_str, "%H:%M:%S.%f") - datetime(1900, 1, 1)
        except ValueError:
            # Se falhar, tenta analisar no formato alternativo
            days, time_part = time_str.split(", ")
            time_obj = datetime.strptime(time_part, "%H:%M:%S")
            return timedelta(days=int(days.split()[0]), hours=time_obj.hour, minutes=time_obj.minute, seconds=time_obj.second)

    def format_timedelta(td):
        seconds = td.total_seconds()
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))

    for author_entry in result["tasks"]:
        for task_entry in author_entry["tasks"]:
            #print(task_entry["mean_time_to_repair"])
            for i in (task_entry["mean_time_to_repair"]):
                if len(i) > 2:
                    mtr_times.append(i['total_difference'])
                    print(i['total_difference'])


    if mtr_times:
        average_time_diff = sum(mtr_times) / len(mtr_times)
        #result['mtr_all'] = format_timedelta(timedelta(seconds=average_time_diff))
        formatted = str(timedelta(hours=average_time_diff))
        if len(formatted) == 22:
            formatted_result = formatted[:-7]
            result['mtr_all'] = formatted_result
        if len(formatted) == 23:
            formatted_result = formatted[:-8]
            result['mtr_all'] = formatted_result
    else:
        result['mtr_all'] = "No MTR Times found."


    return result


if __name__ == "__main__":
    app.run(port=8081)
