from threading import Thread
import slack_sdk as slack
from flask import Flask, request, jsonify
import os
#from waitress import serve
from modules.get_report import getreport
from dotenv import load_dotenv 

load_dotenv() 

# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U01KCEYLA85>'
 
# slack token and signing secret in .env file
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SIGNING_SECRET = os.getenv("SIGNING_SECRET")

# start app
app = Flask(__name__)
# start slack client
client = slack.WebClient(token=SLACK_TOKEN)


# check if a slack json request is of a given type
def check_request_type(request, type):
    # get event
    event = request.get('event')
    if event:
        # get type
        event_type = event.get('type')
        # check if type is given and that it matches given type
        if event_type and event_type == type:
            return True

# basic get handler
@app.route('/', methods=['GET'])
def hey_slack():
    return jsonify({'message': 'Hello world!'})

# handle all incoming post traffic to end point
@app.route('/slack/events', methods=['POST'])  # type: ignore
def verify_slack():
    # convert to json
    message = request.get_json()

    # return challenge if there is one
    challenge = message.get('challenge') #type: ignore
    if challenge:
        # create json response
        res = jsonify({'challenge': challenge})
        # set content type 
        res.headers['Content-Type'] = 'application/json'
        # return response
        return res
    
    # if not challenge, handle request
    thread = Thread(target=handle_message, kwargs={"event_data": message})
    thread.start()

    return {'message': 'succesful request'}, 200

def handle_message(event_data):
    
    print(f'{event_data=}')

    # Only continue if it's an app mention
    if not check_request_type(event_data, 'app_mention'):
        print(f'Not an app_mention.')
        # return 400 with message
        return {'message': 'request is not of valid type. Currently only listening to app mentions.'}, 400

    # get event data
    event = event_data.get('event', {})
    # get channel id to respond properly
    channel_id = event.get('channel')
    # get message information to repl
    ts = event.get('ts')
    # get thread information to reply in thread if needed
    thread_ts = event.get('thread_ts')
    if thread_ts:
        ts = thread_ts
    # get app mention request text
    text = event.get('text')
    
    # confirm request syntax
    if "get report" in text and (len(text.split(' ')) == 4):
        # get document report id
        report_id = text.split(' ')[-1]
        # defend agaisnt non numeric report id
        if not report_id.isnumeric():
            client.chat_postMessage(channel=channel_id, text=f'Invalid report number. Try again.', thread_ts=ts)
        else:
            # convert report id to int
            report_id = int(report_id)
            # send progress message
            client.chat_postMessage(channel=channel_id, text=f'Getting report {report_id}...', thread_ts=ts)
            # try and get report 
            try:
                result = getreport(report_id)
            except Exception as e:
                # return error reason
                client.chat_postMessage(channel=channel_id, thread_ts = ts, text=f'Report not found due to {e}\nRequires manual intervention {RP_ID}.')
                return
            
            # handle if there were multiple names found -- occurs due to oddities in old pdf downloads
            if result[2]:
                client.chat_postMessage(channel=channel_id, text=f'Report exists in multiple pdf files. \nFile: "{result[2]}" chosen at random.', thread_ts=ts)
            # handle some kind of error
            if not result[0]: 
                client.chat_postMessage(channel=channel_id, text=str(result[1]), thread_ts=ts)
            # all went well -- upload files from tempdir and delete it
            else:
                client.files_upload_v2(channel=channel_id,
                        initial_comment="Here's the report:",
                        file=f'{os.getcwd()}/tmp/{result[1]}', 
                        thread_ts = ts)
    else:
        client.chat_postMessage(channel=channel_id, text='Invalid command. Type "get report" followed by report number.', thread_ts=ts)

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    #app.run(debug=True)
    app.run(port=port)