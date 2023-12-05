import re
import requests
import json
from db_utils import (
    append_branches_bb_branches_table,
    connect_db,
    query_sync_history,
)
from flask import Flask, jsonify



class Bitbucket:
    def __init__(self, username, app_password, workspace, repo):
        self.api_base_url = "https://api.bitbucket.org/2.0/repositories"
        self.username = username
        self.app_password = app_password
        self.workspace = workspace
        self.repo = repo

    def list_commits(self, page=1, page_size=10):
        """List commits for a given repository."""
        print(
            f"Fetching, repo: {self.workspace}/{self.repo}, page: {page}, size: {page_size}..."
        )

        # Construct the API url
        url = f"{self.api_base_url}/{self.workspace}/{self.repo}/commits/?page={page}&pagelen={page_size}"
        auth = (
            self.username,
            self.app_password,
        )
        response = requests.get(url, auth=auth)

        if response.status_code != 200:
            return None

        response = response.json()
        commits = response["values"]

        print(f"Records: {len(commits)}")

        commits_with_details = []
        for commit in commits:
            commit_hash = commit["hash"]
            author = commit["author"]

            commit_msg = commit["message"]

            if "user" in author:
                author_nickname = author["user"]["nickname"]
                author_id = author["user"]["uuid"].replace("{", "").replace("}", "")
            else:
                author_nickname = self.__extract_email(author["raw"])
                author_id = author_nickname

            repo_full_name = commit["repository"]["full_name"]
            repo_id = commit["repository"]["uuid"].replace("{", "").replace("}", "")

            commit_details = {
                "id": commit_hash,
                "author": author_nickname,
                "author_id": author_id,
                "msg": commit_msg,
                "created_at": commit["date"],
                "repo": repo_full_name,
            }

            commits_with_details.append(commit_details)

        return commits_with_details
        # return self.__transform_commits(commits_with_details)

    def get_diff_for_commit(self, commit_hash):
        url = f"{self.api_base_url}/{self.workspace}/{self.repo}/diff/{commit_hash}"
        auth = (
            self.username,
            self.app_password,
        )

        response = requests.get(url, auth=auth)
        if response.status_code == 200:
            return response.text
        else:
            return None

    def list_pullrequests(self, page=1, page_size=10):
        """List pull requests for a given repository."""
        print(
            f"Fetching, repo: {self.workspace}/{self.repo}, page: {page}, size: {page_size}..."
        )

        # Construct the API url
        url = f"{self.api_base_url}/{self.workspace}/{self.repo}/pullrequests/?page={page}&pagelen={page_size}"
        auth = (
            self.username,
            self.app_password,
        )
        response = requests.get(url, auth=auth)

        if response.status_code != 200:
            return None

        response = response.json()
        records = response["values"]

        print(f"Records: {len(records)}")

        pullrequests = []
        for record in records:
            repo = record["source"]["repository"]["full_name"]
            author_id = record["author"]["uuid"].replace("{", "").replace("}", "")
            pullrequest = {
                "id": record["id"],
                "title": record["title"],
                "description": record["description"],
                "state": record["state"],
                "author": record["author"]["nickname"],
                "author_id": author_id,
                "repo": repo,
                "created_at": record["created_on"],
                "updated_at": record["updated_on"],
            }
            pullrequests.append(pullrequest)

        # return data
        return pullrequests

    def __extract_email(self, txt):
        # Define a regular expression pattern for matching strings between angle brackets
        pattern = r"<([^<>]+)>"

        # Use re.search to find the email address in the text
        matches = re.findall(pattern, txt)

        email_address = None

        if len(matches) > 0:
            email_address = matches[0].strip()

        return email_address

    def list_branches(self, page=1, page_size=10):
        """List branches for a given repository."""
        print(
            f"Fetching, repo: {self.workspace}/{self.repo}, page: {page}, size: {page_size}..."
        )

        # Construct the API url
        url = f"{self.api_base_url}/{self.workspace}/{self.repo}/refs/branches"
        auth = (
            self.username,
            self.app_password,
        )
        response = requests.get(url, auth=auth)

        if response.status_code != 200:
            return None

        response = response.json()
        records = response["values"]

        #print(f"Records: {response}")
        return records

    def list_branches_commits(self, page=1, page_size=100):
        """List commits for a given branch."""
        print(
            f"Fetching, repo: {self.workspace}/{self.repo}, page: {page}, size: {page_size}..."
        )

        # Construct the API url
        branches_url = f"{self.api_base_url}/{self.workspace}/{self.repo}/refs/branches?page={page}&pagelen={page_size}"
        auth = (
            self.username,
            self.app_password,
        )
        response = requests.get(branches_url, auth=auth)

        if response.status_code != 200:
            return None

        response = response.json()
        branch_name = response["values"]

        data = json.dumps(branch_name)

        data2 = json.loads(data)

        branch_names = [{"branch": item["name"]} for item in data2]
        repository_name = [{"repository": item["target"]["repository"]["name"]} for item in data2]

        urls = [f"{self.api_base_url}/{self.workspace}/{self.repo}/commits/{item['branch']}?page={page}&pagelen={page_size}" for item in branch_names]

        # Exibir as URLs resultantes
        for url in urls:
            auth = (
            self.username,
            self.app_password,
            )
            response1 = requests.get(url, auth=auth)

            if response1.status_code != 200:
                return None
            
            response2 = response1.json()
    
        values = response2['values']

        author_names = [{"authors": item["author"]["user"]["display_name"]} for item in values]
        commits = [{"commit_id": item["hash"], "created_at": item["date"]} for item in values]
        branches = [{"branch_name": item["branch"]} for item in branch_names]

        all_data = [branch_names, repository_name, author_names, commits, branches]





        return all_data
        #return branches
        #return data2