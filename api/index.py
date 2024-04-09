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
    
    """ # if not challenge, handle request
    thread = Thread(target=handle_message, kwargs={"event_data": message})
    thread.start() """
    handle_message(client, message)    

    return {'message': 'succesful request'}, 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    #app.run(debug=True)
    #serve(app, port=port)
    pass