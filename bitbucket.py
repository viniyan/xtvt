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
import pandas as pd




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
                    author_file_path = 'author_names.txt'
                    commit_date_file_path = 'commit_dates.txt'
                    commit_id_file_path = 'commit_ids.txt'
                    message_file_path = 'messages.txt'
                    repository_file_path = 'repository.txt'
                    #print(i['values'])
                    #print(values_list)
                    for d in values_list:
                        # print(d)
                        # caminho_arquivo = 'author_data.txt'

                        # Abre o arquivo no modo de escrita
                        # with open(caminho_arquivo, 'w') as arquivo_txt:
                        #     # Itera sobre cada author_data em d
                        for author_data in d:
                            # Escreve a author_data no arquivo, seguido por uma nova linha
                            # arquivo_txt.write(f"{author_data}\n")
                            commit_id = author_data["hash"]
                            if "author" in author_data and "user" in author_data["author"]:
                                if commit_id not in seen_commit_ids:
                                    author_name = author_data["author"]["user"]["display_name"]
                                    commit_date = author_data["date"]
                                    commit_id = author_data["hash"]
                                    message = author_data["message"].strip()
                                    repository = author_data["repository"]["name"]
                                    if message:
                                        message = message.replace('\n', '')
                                        message_list.append(message)
                                        author_name_list.append(author_name)
                                        commit_date_list.append(commit_date)
                                        commit_id_list.append(commit_id)
                                        repository_list.append(repository)
                                        with open(message_file_path, 'a', encoding='utf-8') as file:
                                            file.write(message + '\n')
                                        with open(author_file_path, 'a') as file:
                                            file.write(author_name + '\n')

                                        with open(commit_date_file_path, 'a') as file:
                                            file.write(commit_date + '\n')

                                        with open(commit_id_file_path, 'a') as file:
                                            file.write(commit_id + '\n')

                                        with open(repository_file_path, 'a') as file:
                                            file.write(repository + '\n')
                                    else:
                                        print("Empty message encountered.")       

                                    seen_commit_ids.add(commit_id)
                                    

                            else:
                                if commit_id not in seen_commit_ids:
                                    author_name = author_data["author"]["raw"]
                                    commit_date = author_data["date"]
                                    commit_id = author_data["hash"]
                                    message = author_data["message"].strip()
                                    repository = author_data["repository"]["name"]
                                    if message:
                                        message = message.replace('\n', '')
                                        message_list.append(message)
                                        author_name_list.append(author_name)
                                        commit_date_list.append(commit_date)
                                        commit_id_list.append(commit_id)
                                        repository_list.append(repository)
                                        with open(message_file_path, 'a', encoding='utf-8') as file:
                                            file.write(message + '\n')

                                        with open(author_file_path, 'a') as file:
                                            file.write(author_name + '\n')

                                        with open(commit_date_file_path, 'a') as file:
                                            file.write(commit_date + '\n')

                                        with open(commit_id_file_path, 'a') as file:
                                            file.write(commit_id + '\n')

                                        with open(repository_file_path, 'a') as file:
                                            file.write(repository + '\n')
                    
                                    else:
                                        print("Empty message encountered.")        

                                    seen_commit_ids.add(commit_id)
                                        
                        
                # Carregar dados dos arquivos TXT para listas
                with open(author_file_path, 'r') as file:
                    author_name_list = file.read().splitlines()   
                    # author_name_list = list(enumerate(file.read().splitlines(), start=1))

                with open(commit_date_file_path, 'r') as file:
                    commit_date_list = file.read().splitlines()
                    # commit_date_list = list(enumerate(file.read().splitlines(), start=1))

                with open(commit_id_file_path, 'r') as file:
                    commit_id_list = file.read().splitlines()
                    
            ####PARA ADICIONAR ÍNDICE NOS ARQUIVOS TXT, SE NECESSÁRIO
            # # # # # # processed_lines = []

            # # # # # # with open(message_file_path, 'r') as file:
            # # # # # #     for line in file:
            # # # # # #         processed_lines.append([line.strip()])

            # # # # # # df = pd.DataFrame(processed_lines, columns=['Coluna'])
            # # # # # # df.to_csv(message_file_path, index=True)
            # # # # # #     #print(df)
                with open(message_file_path, 'r') as file:
                    message_list = file.read().splitlines()
                    #message_list = list(enumerate(file.read().splitlines(), start=1))

                with open(repository_file_path, 'r') as file:
                    repository_list = file.read().splitlines()
                    # repository_list = list(enumerate(file.read().splitlines(), start=1))

                                
            print(len(author_name_list))
            print(len(commit_date_list))
            print(len(commit_id_list))
            print(len(message_list))
            print(len(repository_list))


        result_all = {
        "commit_message": message_list,
        "author": author_name_list,
        "repository": repository_list,
        "created_at": commit_date_list,
        "commit_id": commit_id_list

        }
        


        # Limpar o conteúdo de cada arquivo
        with open(author_file_path, 'w') as file:
            file.truncate(0)
        with open(commit_date_file_path, 'w') as file:
            file.truncate(0)
            
        with open(commit_id_file_path, 'w') as file:
            file.truncate(0)
            
        with open(message_file_path, 'w') as file:
            file.truncate(0)

        with open(repository_file_path, 'w') as file:
            file.truncate(0)
                
                

        return result_all