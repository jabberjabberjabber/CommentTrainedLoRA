import csv
import requests
import time
import json
import re

def get_parent_comment_data(comment_id):
    base_url = "https://api.pullpush.io/reddit"
    thread_data = {'comments': [], 'submission': None}

    try:
        api_url = f"{base_url}/comment/search?ids={comment_id}"
        response = requests.get(api_url)
        comments = response.json().get('data', [])
        
        if not comments:
            return thread_data
        
        for comment in comments:
            thread_data['comments'].append(comment)
            parent_id = comment['parent_id']
            link_id = comment['link_id']
            while parent_id.startswith('t1_'):  # 't1_' prefix indicates a comment
                parent_response = requests.get(f"{base_url}/search?ids={parent_id[3:]}")
                if parent_response.status_code == 200 and parent_response.json().get('data'):
                    parent_comment = parent_response.json().get('data')[0]
                    thread_data['comments'].append(parent_comment)
                    parent_id = parent_comment['parent_id']
                else:
                    break

        submission_response = requests.get(f"{base_url}/search/submission/?ids={link_id[3:]}&fields=selftext,title")
        if submission_response.status_code == 200:
            submission_data = submission_response.json().get('data', [])
            if submission_data:
                thread_data['submission'] = submission_data[0]
        return thread_data
    except Exception as e:
        print(f"Error fetching data for comment ID {comment_id}: {e}")
        return None

def get_submission_data(comment_id):
    base_url = "https://api.pullpush.io/reddit"
    submission_data = []
    try:
        api_url = f"{base_url}/comment/search?ids={comment_id}"
        response = requests.get(api_url)
        data = response.json().get('data', [])
        if not data:
            return None
        link_id_str = data['link_id']
        link_id = link_id_str[3:]
        
        submission_response = requests.get(f"{base_url}/search/submission/?ids={link_id}&fields=selftext,title")
        if submission_response.status_code == 200:
            submission_stuff = submission_response.json().get('data', [])
            if submission_stuff:
                submission_data['submission'] = submission_stuff[0]
                return submission_data
        return None
    except Exception as e:
        print(f"Error fetching data for comment ID {comment_id}: {e}")
        return None

def remove_unwanted(text):
    url_pattern = re.compile(r'(http|https)?://\S+', re.IGNORECASE)
    date_pattern = re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b')
    file_path_pattern = re.compile(r'\b[A-Za-z]:\\[^\\]+\\[^\\]+\b')
    code_block_pattern = re.compile(r'```[\s\S]*?```')
    cleaned_text = code_block_pattern.sub('', text)
    cleaned_text = url_pattern.sub('', cleaned_text)
    cleaned_text = date_pattern.sub('', cleaned_text)
    cleaned_text = file_path_pattern.sub('', cleaned_text)
    return cleaned_text

def parse_and_process_entry(row):
    comment_id = row['id']
    my_comment = row['body']
    entry = {
        "date": row['date'],
        "message_id": comment_id,
        "link": row['link'],
        "my_comment": my_comment.replace('\n', ' '),
        "subreddit": row['subreddit']
    }
    parent_id = row['parent']
    if parent_id:
        parent_data = get_parent_comment_data(parent_id)
        if parent_data and parent_data['comments']:
            parent_comment_bodies = [comment.get('body', '').replace('\n', ' ') for comment in parent_data['comments']]
            marker = " ||||||||| "
            all_parent_comments = marker.join(parent_comment_bodies)

            entry.update({
                "parent_id": parent_id,
                "parent_comment": all_parent_comments,
            })

        if parent_data and parent_data['submission']:
            submission_data = parent_data['submission']
            entry.update({
                "submission_text": submission_data.get('selftext', '') + " " + submission_data.get('title', '')
            })
    else:
        print(f"No data found for parent of comment ID {comment_id}.")

    return entry

def estimate_total_comments(csv_file_path):
    pattern = re.compile(r'^[a-z0-9]{7}')
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        total_comments = sum(1 for line in file if pattern.match(line))
    return total_comments

def create_alpaca_dataset(csv_file_path, output_json_path, buffer_size=100):
    author = ""
    processed_comments = 0
    total_comments = estimate_total_comments(csv_file_path)
    print(f"Estimated total comments to process: {total_comments}")

    try:
        with open('last_processed_id.txt', 'r') as f:
            last_processed_id = f.read().strip()
    except FileNotFoundError:
        last_processed_id = None
    starting = True if last_processed_id is None else False

    with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        buffer = []
        for row in reader:
            comment_id = row['id']

            if not starting and comment_id == last_processed_id:
                starting = True
                continue

            if not starting:
                continue

            entry = parse_and_process_entry(row)

            reversed_thread = ''.join(entry.get("parent_comment", "")).split("||||||||| ")
            if entry.get("submission_text", ""):
                context = entry.get("submission_text") + " " + reversed_thread[0]
            else:
                submission_data = get_submission_data(entry["message_id"])
                if submission_data is not None:
                    submission_text = submission_data['submission']
                    context = submission_text.get('title', '') + " " + submission_text.get('selftext', '') + " " + reversed_thread[0]
                else:
                    context = reversed_thread[0]

            cleaned_context = remove_unwanted(context)
            cleaned_response = remove_unwanted(entry["my_comment"])

            restructured_entry = {
                "date": entry["date"],
                "message_id": entry["message_id"],
                "subreddit": entry["subreddit"],
                "author": author,
                "context": cleaned_context,
                "response": cleaned_response
            }

            buffer.append(restructured_entry)
            processed_comments += 1

            if len(buffer) >= buffer_size:
                with open(output_json_path, 'a', encoding='utf-8') as jsonfile:
                    for item in buffer:
                        json.dump(item, jsonfile, ensure_ascii=False)
                        jsonfile.write('\n')
                buffer = []
                with open('last_processed_id.txt', 'w') as f:
                    f.write(comment_id)

            time.sleep(.1)

            print(f"Progress: {processed_comments}/{total_comments}")

    if buffer:
        with open(output_json_path, 'a', encoding='utf-8') as jsonfile:
            for item in buffer:
                json.dump(item, jsonfile, ensure_ascii=False)
                jsonfile.write('\n')

    print(f"Final dataset saved to {output_json_path} after processing {processed_comments} comments.")

# Main script
csv_file_path = 'comments.csv'
output_json_path = 'cleaned_restructured_data.json'

# Create the Alpaca dataset
create_alpaca_dataset(csv_file_path, output_json_path, buffer_size=10)