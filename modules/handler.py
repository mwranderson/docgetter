from modules.get_report import getreport
import os

# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U01KCEYLA85>'

def check_request_type(request, type):
    '''
    Given json request and a slack event type, 
    return true if request is of event type: type
    '''
    # get event
    event = request.get('event')
    if event:
        # get type
        event_type = event.get('type')
        # check if type is given and that it matches given type
        if event_type and event_type == type:
            return True

def handle_message(client, event_data):
    '''
    Given slack client and request json, handles request,
    including communicating with backend as well as slack.
    '''
    
    # Only continue if it's an app mention
    if not check_request_type(event_data, 'app_mention'):
        print(f'Not an app_mention.')
        # exit
        return

    # get event data
    event = event_data.get('event')
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
                        file=f'/tmp/{result[1]}', 
                        thread_ts = ts)
    else:
        client.chat_postMessage(channel=channel_id, text='Invalid command. Type "get report" followed by report number.', thread_ts=ts)