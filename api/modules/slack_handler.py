import os
print(f'In slack_handler: cwd: {os.getcwd()}')
from .get_report import getreport
print('made it past handler module import')


# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U01KCEYLA85>'

# accepted transcript source inputs and their mappings
ts_dict = {'0': 0, 'ref-pdf': 0, 'ref_pdf': 0, 'refpdf': 0, 
           '1': 1, 'ref-xml': 1, 'ref_xml': 1, 'refxml': 1, 
           '2': 2, 'fs': 2, 'factset': 2,
           '3': 3, 'ciq': 3, 'capitaliq': 3}


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

def handle_request(client, event_data):
    '''
    Given slack client and request json, handles request, \\
    passing it to corresponding handler.
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
    
    # figure out request type
    if "get report" in text:
        # separate out unneeded parts of text
        text = text.split('get report')[-1].strip()
        # handle get report
        handle_get_report(client, text, channel_id, ts)
    # process other future requests such as dropbox monitoring etc.
    # for now, only get report is supported
    else:
        client.chat_postMessage(channel=channel_id, text='Invalid command. Type "get report" followed by report number.', thread_ts=ts)   


def handle_get_report(client, text, channel_id, ts):
    '''
    Given slack client and request subparts, processes get report request, \\
    including communicating with backend as well as slack.
    '''
    # check if request is complete
    if not text:
        client.chat_postMessage(channel=channel_id, 
                                text='Invalid request. Provide at least the report number.', 
                                thread_ts=ts)
        return
    # get request parts
    request_parts = text.split(' ')
    
    # check if request asks for too much
    if len(request_parts) > 2:
        client.chat_postMessage(channel=channel_id, 
                                text='Invalid request. Provide at most the report number followed by the transcript source.', 
                                thread_ts=ts)
        return
    
    # get report id
    report_id = request_parts[0]
    # defend against non numeric report id
    if not report_id.isnumeric():
        client.chat_postMessage(channel=channel_id, text=f'Invalid report number. It must be an integer.', thread_ts=ts)
        return
    else:
        # convert to integer
        report_id = int(report_id)

    # init missing transcript source
    transcript_source = -1

    # get transcript source id if it has been provided
    if len(request_parts) > 1:
        transcript_source_raw = request_parts[1]
        # process transcript source
        transcript_source = transcript_source_raw.lower()
        # defend against bad input
        if transcript_source not in ts_dict.keys():
            client.chat_postMessage(channel=channel_id, text=f'Invalid transcript source. It must be one of: {list(ts_dict.keys())}', thread_ts=ts)
        # convert transcript source
        transcript_source = ts_dict[transcript_source]

    # send processing message
    client.chat_postMessage(channel=channel_id, text=f'Getting report {report_id}...', thread_ts=ts)
    # try and get report 
    try:
        success, filename, mutilpdf_filename = getreport(report_id, transcript_source)
    except Exception as e:
        # return error reason
        client.chat_postMessage(channel=channel_id, thread_ts = ts, text=f'Report not found due to {e}\nRequires manual intervention {RP_ID}.')
        return
    
    # handle if there were multiple names found -- occurs due to oddities in old pdf downloads
    if mutilpdf_filename:
        client.chat_postMessage(channel=channel_id, text=f'Report exists in multiple pdf files. \nFile: "{mutilpdf_filename}" chosen at random.', thread_ts=ts)
    # handle some kind of error
    if not success: 
        client.chat_postMessage(channel=channel_id, text=str(filename), thread_ts=ts)
    # all went well -- upload files from tempdir and delete it
    else:
        client.files_upload_v2(channel=channel_id,
                initial_comment="Here's the report:",
                file=f'/tmp/{filename}', 
                thread_ts = ts)