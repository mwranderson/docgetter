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
event_id_queue = []

# basic get handler
@app.route('/', methods=['GET'])
def hey_slack():
    return jsonify({'message': 'Hello world!'})

# handle all incoming post traffic to end point
@app.route('/slack/events', methods=['POST'])  # type: ignore
def verify_slack():
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
    if event_id in event_id_queue:
        # it's being processed
        return {'message': 'succesful request'}, 200
    else:
        # add id to queue
        event_id_queue.append(event_id)
        # run process
        handle_message(client, message)
        # remove event_id from process
        event_id_queue.remove(event_id)
    

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    #app.run(debug=True)
    #serve(app, port=port)
    pass