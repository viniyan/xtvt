import re
import requests
import json
from db_utils import (
    append_mtr,
    connect_db,
    query_sync_history,
)
from flask import Flask, jsonify
from urllib.parse import urlparse, parse_qs
import os




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

    def sync_mtr(self, page=1, page_size=1000):
        bitbucket_repos_list = os.environ.get("BITBUCKET_REPOS")
        workspaces = []
        repos = []
        if bitbucket_repos_list:
            repos = bitbucket_repos_list.split(',')
            for repo_string in repos:
                # Dividir cada string do repositório em workspace e repo
                parts = repo_string.split('/')
                workspace, repo = parts
                # print(workspace)
                print(f"Fetching, repo: {workspace}/{repo}, page: {page}, size: {page_size}...")

                

                # Construct the API url
                branches_url = f"{self.api_base_url}/{workspace}/{repo}/refs/branches?page={page}&pagelen={page_size}"
                #print(branches_url)
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

                urls = [f"{self.api_base_url}/{workspace}/{repo}/commits/{item['branch']}?page={page}&pagelen={page_size}" for item in branch_names]
                #print(urls)
                #['https://api.bitbucket.org/2.0/repositories/backend-test1/xtvt-teste/commits/feature/teste?page=1&pagelen=100', 
                #'https://api.bitbucket.org/2.0/repositories/backend-test1/xtvt-teste/commits/master?page=1&pagelen=100', 
                #'https://api.bitbucket.org/2.0/repositories/backend-test1/xtvt-teste/commits/feature/teste2?page=1&pagelen=100']
                
                ### O ENDPOINT ABAIXO NÃO RETORNA TODOS OS AUTORES!!!
                #urls = [f"{self.api_base_url}/{self.workspace}/{self.repo}/refs?q=name=%22{item['branch']}%22" for item in branch_names]

                        
                
                result = []
                result_branches = []



                #Exibir as URLs resultantes
                for url in urls:
                    auth = (
                    self.username,
                    self.app_password,
                    )
                    response1 = requests.get(url, auth=auth)
                    # print(urls)

                    if response1.status_code != 200:
                        print("ERRO")
                        return None
                    else:
                        content = response1.json()
                        result.append(content)
                        # parts = url.split("/commits/")
                        # branch_name = parts[1]
                        # special = branch_name.split("?", 1)
                        # branch_final = special[0]
                        # result_branches.append(branch_final)
            

                result_dict = result
                #print(result_dict)
                    
                #print(result_dict_2)

                author_name_list = []
                commit_date_list = []
                commit_id_list = []
                message_list = []
                repository_list = []

                # Conjunto para manter registro dos commit_ids
                seen_commit_ids = set()

                for i in result_dict:    
                    values_list = [i['values'] for i in result_dict]
                    #print(i['values'])
                    #print(values_list)
                    for d in values_list:
                        # print(d)
                        for author_data in d:
                            commit_id = author_data["hash"]
                            if "author" in author_data and "user" in author_data["author"]:
                                #print(author_data["author"]["user"]["display_name"])
                                if commit_id not in seen_commit_ids:
                                    author_name = author_data["author"]["user"]["display_name"]
                                    commit_date = author_data["date"]
                                    commit_id = author_data["hash"]
                                    message = author_data["message"]
                                    repository = author_data["repository"]["name"]
                                    author_name_list.append(author_name)
                                    commit_date_list.append(commit_date)
                                    commit_id_list.append(commit_id)
                                    message_list.append(message)
                                    repository_list.append(repository)

                                    seen_commit_ids.add(commit_id)
                            else:
                                # print(f"Missing 'user' key in author_data['author']: {author_data}")
                                if commit_id not in seen_commit_ids:
                                    author_name = author_data["author"]["raw"]
                                    commit_date = author_data["date"]
                                    commit_id = author_data["hash"]
                                    message = author_data["message"]
                                    repository = author_data["repository"]["name"]
                                    author_name_list.append(author_name)
                                    commit_date_list.append(commit_date)
                                    commit_id_list.append(commit_id)
                                    message_list.append(message)
                                    repository_list.append(repository)

                                    seen_commit_ids.add(commit_id)
                            


                result_all = {
                "commit_message": message_list,
                "author": author_name_list,
                "repository": repository_list,
                "created_at": commit_date_list,
                "commit_id": commit_id_list

                }

        return result_all
        #return branches
        #return data2