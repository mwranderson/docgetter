import slack_sdk as slack
from flask import Flask, Response, request
from slackeventsapi import SlackEventAdapter
import os
from get_report import getreport
from threading import Thread
from waitress import serve
from dotenv import load_dotenv 
#from requests import request

load_dotenv() 
 
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SIGNING_SECRET = os.getenv("SIGNING_SECRET")

app = Flask(__name__)
slack_event_adapter = SlackEventAdapter(SIGNING_SECRET, '/slack/events', app)
client = slack.WebClient(token=SLACK_TOKEN)


@app.route('/', methods=['GET', 'POST']) 
def hey_slack():
    message = request.get_json()
    print('\n\nAHH THIS IS IT, MAN:\n\n')
    print(message)
    """ if request_json.get("challenge") is not None:
        return Response(request_json.get("challenge"), status=200) """
    return Response(status=200)


@slack_event_adapter.on("app_mention")
def handle_message(event_data):
    def send_reply(value):
        event_data = value
        print(event_data)
        event = event_data.get('event', {})
        channel_id = event.get('channel')
        user_id = event.get('user')
        ts = event.get('ts')
        thread_ts = event.get('thread_ts')
        if thread_ts:
            ts = thread_ts
        text = event.get('text')
        if "get report" in text and (len(text.split(' ')) == 4):
            report_id = text.split(' ')[-1]
            if not report_id.isnumeric():
                client.chat_postMessage(channel=channel_id, text=f'Invalid report number. Try again.', thread_ts=ts)
            else:
                report_id = int(report_id)
                client.chat_postMessage(channel=channel_id, text=f'Getting report {report_id}...', thread_ts=ts)
                result = getreport(report_id)
                if result[2]:
                    client.chat_postMessage(channel=channel_id, text=f'Report exists in multiple pdf file. File: {result[2]} chosen at random.', thread_ts=ts)
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
    thread = Thread(target=send_reply, kwargs={"value": event_data})
    thread.start()
    return Response(status=200)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    serve(app, port=port)
    #app.run(debug=True)
    
