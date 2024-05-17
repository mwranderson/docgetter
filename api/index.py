import slack_sdk as slack
from flask import Flask, request, jsonify
import os
from .modules.slack_handler import handle_request
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

# handle all incoming post traffic to events end point
@app.route('/slack/events', methods=['POST'])
def verify_slack():


    ## this method of avoiding duplicates must be done
    ## since free hosting services do not allow threading
    ## if we do not do this, slack will send us the bot request
    ## up to 4 times in a 5 minute window
    ## slack retries if we don't respond 200 within 3 seconds of initial
    ## request
    
    # trying header retry check to see if we are getting time out retries
    retry_check_reason = request.headers.get('x-slack-retry-reason')

    # if we have a retry and it's due to http timeout, block it.
    if retry_check_reason and retry_check_reason == 'http_timeout':
        print(f'Retry request')
        # reject retry -- try and ask for no more retries
        res = jsonify({'message': 'task in progress'})
        # set content type 
        res.headers['x-slack-no-retry'] = '1'
        return res, 418


    # convert to json
    message = request.get_json()

    # return challenge if there is one
    challenge = message.get('challenge')
    if challenge:
        # create json response
        res = jsonify({'challenge': challenge})
        # set content type 
        res.headers['Content-Type'] = 'application/json'
        # return response
        return res
    
    # run process
    handle_request(client, message)
        
    # return 200
    return {'message': 'succesful request'}, 200


# handle all incoming post traffic to interactions end point
@app.route('/slack/interactions', methods=['POST'])
def slack_interact():

    print(f'{request=}', end='\n\n\n')
    # convert to json
    message = request.get_json()
    print(f'{message=}')

    ## this method of avoiding duplicates must be done
    ## since free hosting services do not allow threading
    ## if we do not do this, slack will send us the bot request
    ## up to 4 times in a 5 minute window
    ## slack retries if we don't respond 200 within 3 seconds of initial
    ## request
    
    # trying header retry check to see if we are getting time out retries
    retry_check_reason = request.headers.get('x-slack-retry-reason')

    # if we have a retry and it's due to http timeout, block it.
    if retry_check_reason and retry_check_reason == 'http_timeout':
        print(f'Retry request')
        # reject retry -- try and ask for no more retries
        res = jsonify({'message': 'task in progress'})
        # set content type 
        res.headers['x-slack-no-retry'] = '1'
        return res, 418


    

    # return challenge if there is one
    challenge = message.get('challenge')
    if challenge:
        # create json response
        res = jsonify({'challenge': challenge})
        # set content type 
        res.headers['Content-Type'] = 'application/json'
        # return response
        return res
    
   
        
    # return 200
    return {'message': 'succesful request'}, 200
