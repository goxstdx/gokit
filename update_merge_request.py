import requests
import argparse

def get_merge_request_comments(base_url, private_token, project_id, merge_request_id):
    url = f"{base_url}/api/v4/projects/{project_id}/merge_requests/{merge_request_id}/notes"
    print(f"Fetching comments for merge request {merge_request_id} in project {project_id}.")

    response = requests.get(url, headers={'Private-Token': private_token})

    if response.status_code == 200:
        comments = response.json()
        # 按 ID 降序排序并返回 ID 最大的 5 条评论
        sorted_comments = sorted(comments, key=lambda x: x['id'], reverse=True)
        print(f"Retrieved {len(comments)} comments.")
        return sorted_comments[:5]  # 返回 ID 最大的 5 条评论
    else:
        print(f"Failed to retrieve comments: {response.status_code} - {response.text}")
        return None

def update_merge_request_title(base_url, private_token, project_id, merge_request_id):
    url = f"{base_url}/api/v4/projects/{project_id}/merge_requests/{merge_request_id}"
    print(f"Updating title for merge request {merge_request_id} in project {project_id}.")

    # 获取当前的 merge request 信息
    response = requests.get(url, headers={'Private-Token': private_token})

    if response.status_code != 200:
        print(f"Failed to retrieve merge request: {response.status_code} - {response.text}")
        return False, "Failed to retrieve merge request."

    merge_request = response.json()
    current_title = merge_request['title']

    # 检查标题是否已经包含 "WIP"
    if not current_title.startswith("WIP:"):
        new_title = f"WIP: {current_title}"
    else:
        new_title = current_title  # 如果已经有 "WIP"，保持原标题

    # 如果标题没有变化，则不调用接口
    if new_title == current_title:
        print("No changes to the title. No update required.")
        return True, "No changes to the title."

    # 更新 merge request 的标题
    update_data = {'title': new_title}
    update_response = requests.put(url, headers={'Private-Token': private_token}, json=update_data)

    if update_response.status_code == 200:
        print("Merge request title updated successfully.")
        return True, "Merge request title updated successfully."
    else:
        print(f"Failed to update merge request title: {update_response.status_code} - {update_response.text}")
        return False, "Failed to update merge request title."

if __name__ == "__main__":
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(description='Update GitLab Merge Request Title if comments contain specific keywords.')
    parser.add_argument('private_token', type=str, help='Your GitLab private token')
    parser.add_argument('project_id', type=str, help='The project ID')
    parser.add_argument('merge_request_id', type=str, help='The merge request ID')

    args = parser.parse_args()

    BASE_URL = "http://gitlab.ops.haochezhu.club"
    PRIVATE_TOKEN = args.private_token
    PROJECT_ID = args.project_id
    MERGE_REQUEST_ID = args.merge_request_id

    comments = get_merge_request_comments(BASE_URL, PRIVATE_TOKEN, PROJECT_ID, MERGE_REQUEST_ID)
    if comments is not None:
        for comment in comments:
            body = comment['body'].lower()  # 转为小写以进行不区分大小写的比较
            if "bug" in body and "pr code suggestions" in body:  # 检查是否同时包含“bug”和“PR Code Suggestions”
                success, message = update_merge_request_title(BASE_URL, PRIVATE_TOKEN, PROJECT_ID, MERGE_REQUEST_ID)
                print(message)
                break  # 找到后可以退出循环
    else:
        print("Failed to retrieve comments.")
