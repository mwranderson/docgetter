from threading import Thread
import slack_sdk as slack
from flask import Flask, request, jsonify
import os
from modules.handler import handle_message
from dotenv import load_dotenv 

# load dotenv files
load_dotenv() 
 
# slack token and signing secret in .env file
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SIGNING_SECRET = os.getenv("SIGNING_SECRET")

# start app
app = Flask(__name__)
# start slack client
client = slack.WebClient(token=SLACK_TOKEN)

# start event ID tracker
EVENT_ID_QUEUE = []

# basic get handler
@app.route('/', methods=['GET'])
def hey_slack():
    return jsonify({'message': 'Hello world!'})

# handle all incoming post traffic to end point
@app.route('/slack/events', methods=['POST'])  # type: ignore
def verify_slack():

    # trying header retry check
    retry_check_reason = request.headers.get('x-slack-retry-reason')

    print(f'{retry_check_reason=}')
    if retry_check_reason and retry_check_reason == 'http-timeout':
        print(f'Fucking retries... getting it out of the way.')
        # it's being processed
        return {'message': 'succesful request'}, 200


    # convert to json
    message = request.get_json()

    print(f'{message=}')

    # return challenge if there is one
    challenge = message.get('challenge') #type: ignore
    if challenge:
        # create json response
        res = jsonify({'challenge': challenge})
        # set content type 
        res.headers['Content-Type'] = 'application/json'
        # return response
        return res
    
    # get unique event id
    event_id = message.get('event_id')
    
    # see if event is being processed
    # note: this hack had to be done to avoid slack's multiple
    # follow-up pings when ok response is not instant.
    if event_id in EVENT_ID_QUEUE:
        print(f'Found {event_id} in queue. Skipping.')
        # it's being processed
        return {'message': 'succesful request'}, 200
    else:
        print(f'{event_id} is new. Running it.')
        # add id to queue
        EVENT_ID_QUEUE.append(event_id)
        # run process
        handle_message(client, message)
        print(f'Process done. Removing {event_id} from queue.')
        # remove event_id from process
        EVENT_ID_QUEUE.remove(event_id)
        # it's being processed
        return {'message': 'succesful request'}, 200
    
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    #app.run(debug=True)
    #serve(app, port=port)
    pass