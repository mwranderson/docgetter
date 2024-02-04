import slack_sdk as slack
from flask import Flask, Response, request
import os
from modules.get_report import getreport
from dotenv import load_dotenv 

load_dotenv('../.env')
 
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SIGNING_SECRET = os.getenv("SIGNING_SECRET")

app = Flask(__name__)
client = slack.WebClient(token=SLACK_TOKEN)

def send_reply(value):
    event_data = value
    print(f'{event_data=}')
    event = event_data.get('event', {})
    channel_id = event.get('channel')
    user_id = event.get('user')
    ts = event.get('ts')
    thread_ts = event.get('thread_ts')
    if thread_ts:
        ts = thread_ts
    text = event.get('text')
    print(20*'-', end='\n')
    print(f'{event=}\n{channel_id=}\n{user_id}\n{ts=}\n{thread_ts=}\n{text=}\n')
    print(20*'-', end='\n')
    if "get report" in text and (len(text.split(' ')) == 4):
        report_id = text.split(' ')[-1]
        if not report_id.isnumeric():
            client.chat_postMessage(channel=channel_id, text=f'Invalid report number. Try again.', thread_ts=ts)
        else:
            report_id = int(report_id)
            client.chat_postMessage(channel=channel_id, text=f'Getting report {report_id}...', thread_ts=ts)
            result = getreport(report_id)
            if result[2]:
                client.chat_postMessage(channel=channel_id, text=f'Report exists in multiple pdf files. \nFile: "{result[2]}" chosen at random.', thread_ts=ts)
            if not result[0]: #something went wrong. Print relevant message.
                client.chat_postMessage(channel=channel_id, text=str(result[1]), thread_ts=ts)
            else:
                client.files_upload_v2(channel=channel_id,
                        initial_comment="Here's the report:",
                        file=f'{os.getcwd()}/tempdir/{result[1]}', 
                        thread_ts = ts)
                os.remove(f'{os.getcwd()}/tempdir/{result[1]}')
    else:
        client.chat_postMessage(channel=channel_id, text='Invalid command. Type "get report" followed by report number.', thread_ts=ts)


def handle_mention(message):
    send_reply(message)
    return Response(status=200)

@app.route('/', methods=['POST']) 
def app_main():
    message = request.get_json()
    if message.get("challenge") is not None:
        print(f'Got challenge:\n {message=}')
        return Response(message.get("challenge"), status=200)
    elif message.get('event').get('type') == 'app_mention':
        print('Handling mention...\n')
        return handle_mention(message)
    else:
        print(f'Not an app mention.')
        return Response(status=200)

if __name__ == "__main__":
    app.run()
    
