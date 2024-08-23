from typing import Union
from .get_report import getreport, get_file_info

# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U079GUP88LW>'

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
    # check if it's an interaction
    if event_data['type'] == 'block_actions':

        container = event_data.get('container')
        # get channel_id
        channel_id = container['channel_id']
        # get thread information to reply in thread if needed
        thread_ts = container.get('thread_ts')
        if thread_ts:
            ts = thread_ts

        # get selected option
        choice_raw = event_data.get('actions')[0]['selected_option']

        print(f'handling interaction {choice_raw=}')

        # clean choice text
        choice = choice_raw.get('text').get('text').replace('```', '') 

        # get choice parts
        parts = choice.split('|')

        # check if slide or report
        if '.pdf' in choice:
            # slide mode
            # construct request text
            text = parts[0].strip()
            # get directory
            directory = f"/project/FactSet/Doc_Retrieval_API/factset_slides_API/output/{parts[-2]}"
            # get slide
            handle_get_slide(client, text, channel_id, ts, directory=directory)
            pass
        else:
            # transcript mode
            # construct request text
            text = parts[0].strip() + ' ' + parts[1].strip()
            # standardize report/transcript float conversions
            text = text.replace('.0', '')
            # get requested report
            handle_get_report(client, text, channel_id, ts)
   
        return

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
    elif "get slide" in text:# separate out unneeded parts of text
        text = text.split('get slide')[-1].strip()
        # handle get report
        handle_get_slide(client, text, channel_id, ts)

    # process other future requests such as dropbox monitoring etc.
    # for now, only get report is supported
    else:
        client.chat_postMessage(channel=channel_id, text='Invalid command. Type "get report" followed by report number.', thread_ts=ts)  
        
def create_measure(value: Union[str, int], max_len: int):
    '''Helper for block builder. \\
    Given value and max len of block, returns a well-spaced string of value.'''

    # create formatting pattern
    pat = f':^{max_len}'
    formatting = '{'+pat+'}'
    
    # return text with formatting applied
    return formatting.format(value)

def multi_choice_block_builder(report, options, slide_mode = False):
    '''
    Given 2-D array of transcript choices from dataframe, returns slack choice block \\
    if dealing with slides, set slide_mode to True
    '''
    print(f'{options=}')
    # get option max lengths
    report_len = options.report.astype(str).str.len().max()
    ts_len = 8
    file_name_len = options.file_name.astype(str).str.len().max() + 2
    date_len = options.date.astype(str).str.len().max()
    
    # extra space constant
    header_padding = 2*' '

    # construct divider
    divider = { "type": "divider" }

    # construct top message
    if slide_mode:
        top_message = f"Multiple slides in database.\n\n *Please select the one you are looking for:*"
    else:
        top_message = f"Multiple transcrips in database with report = {report}.\n\n *Please select the one you are looking for:*"
    # construct top part
    top_part = {
        'type': 'section', 
        'text': { 'type': 'mrkdwn',
                 'text': top_message}}

    # construct table header
    if slide_mode:
        table_text = f"{header_padding}|{create_measure('file_name', file_name_len)}|{create_measure('slide_id', report_len)}|{create_measure('address', file_name_len)}|\n"
    else:
        table_text = f"{header_padding}|{create_measure('report', report_len)}|{create_measure('source', ts_len)}|{create_measure('file_name', file_name_len)}|{create_measure('date', date_len)}|\n"
    table_header = {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"```{table_text}```"
			}
		}
    # init choices list
    choices_list = []
    
    # create choices list
    for i, option in enumerate(options.values.tolist()):

        # create row 
        if slide_mode:
            row = f'{create_measure(option[2], file_name_len)}|{create_measure(option[4], report_len)}|{create_measure(option[5], file_name_len)}|'
        else:
            row = f'{create_measure(option[0], report_len)}|{create_measure(option[1], ts_len)}|{create_measure(option[2], file_name_len)}|{create_measure(option[3], date_len)}|'
       
        choice = {
            'text': {
                'type': 'mrkdwn',
                'text': f"```{row}```"
            },
            'value': f'value-{i}'
        }

        choices_list.append(choice)

    # construct choice part
    choices = {'type': 'actions', 
                      'elements': [
                          {
                            'type': 'radio_buttons',
                            'options': choices_list,
					        "action_id": "actionId-0"
                          }]}

    blocks = [top_part, divider, table_header, choices, divider]
		
    return blocks


def handle_get_slide(client, text, channel_id, ts, directory = None):
    '''
    Given slack client and request subparts, processes get report request, \\
    including communicating with backend as well as slack.\\
    Allow full directory provided in case of duplicates to circumvent duplicate issues.
    '''
    # check if request is complete
    if not text:
        client.chat_postMessage(channel=channel_id, 
                                text='Invalid request. Provide more identifying information on the slide.', 
                                thread_ts=ts)
        return
    
    # init slide_id and slide_file_name and slide_identifier variables
    slide_identifer = 0
    slide_id = 0
    slide_file_name = ''
    # figure out if id is given or file name
    if text.isnumeric(): 
        slide_id = int(text)
        slide_identifer = slide_id
    else:
        slide_file_name = text
        slide_identifer = slide_file_name

    # send processing message
    client.chat_postMessage(channel=channel_id, text=f'Looking for {slide_identifer}...', thread_ts=ts)
    
    # get slide info
    response, rest = get_file_info(slide_id=slide_id, slide_file_name=slide_file_name, slide_mode=True)

    # REVIEW THESE
    # possible outcomes: 
    # 1. report not found
    # 2. too many options -- fixable with transcript_source
    # 3. too many options -- not fixable with transcript_source
    # 4. invalid transcript source -- shouldn't be happening though
    # 5. Success. Downloading report.

    # 1 and 4 can be combined.
    # 3 needs RP call
    # 2 has IO
    # 5 is nothing
    
    # react to different responses
    if not response and not directory:
        if len(rest) > 10:
            # send error message if error case
            client.chat_postMessage(channel=channel_id, text=rest, thread_ts=ts)
        else:
            # give choices if not
            client.chat_postMessage(channel=channel_id, thread_ts=ts, text='More information needed.', blocks=multi_choice_block_builder(slide_id, rest, slide_mode = True))
        return
    elif directory:
        non_directory, filename, transcript_source, multipdf_filename = rest
    else:
        # get necessary information
        directory, filename, transcript_source, multipdf_filename = rest
    
    
    # send processing message
    client.chat_postMessage(channel=channel_id, text=f'Found slide {slide_identifer}. Downloading...', thread_ts=ts)

    # try and download report 
    try:
        success, rest = getreport(report=-1, directory=directory, filename=filename, multipdf_filename=multipdf_filename, transcript_source=-1, slide_mode=True) #type: ignore
    except Exception as e:
        # return error reason
        client.chat_postMessage(channel=channel_id, thread_ts = ts, text=f'Report not found due to {e}\nRequires manual intervention {RP_ID}.')
        return

    # got report successfully
    if success:
        filename, multipdf_filename = rest
        client.files_upload_v2(channel=channel_id,
                initial_comment="Here's the slide:",
                file=f'/tmp/{filename}', 
                thread_ts = ts)
        return
    else:
        # general download problem
        client.chat_postMessage(channel=channel_id, text=str(rest), thread_ts=ts)
        return



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
        print(f'Original message: {text}')
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
    client.chat_postMessage(channel=channel_id, text=f'Looking for report {report_id}...', thread_ts=ts)
    
    # get report info
    response, rest = get_file_info(report_id, transcript_source)

    # possible outcomes: 
    # 1. report not found
    # 2. too many options -- fixable with transcript_source
    # 3. too many options -- not fixable with transcript_source
    # 4. invalid transcript source -- shouldn't be happening though
    # 5. Success. Downloading report.

    # 1 and 4 can be combined.
    # 3 needs RP call
    # 2 has IO
    # 5 is nothing
    
    # react to different responses
    if not response:
        if len(rest) > 10:
            # send error message if error case
            client.chat_postMessage(channel=channel_id, text=rest, thread_ts=ts)
        else:
            # give transcript source choices if not
            client.chat_postMessage(channel=channel_id, thread_ts=ts, text='More information needed.', blocks=multi_choice_block_builder(report_id, rest))
        return
    else:
        # get necessary information
        directory, filename, transcript_source, multipdf_filename = rest
    
    # send processing message
    client.chat_postMessage(channel=channel_id, text=f'Found report {report_id}. Downloading...', thread_ts=ts)
    # send capital IQ warning
    if transcript_source == 3:
        client.chat_postMessage(channel=channel_id, text=f'Warning: You have requested a Capital IQ transcript. Due to the strucutre of our dataset, this request can take up to a minute. If you have not received the file after 1 minute, the request has failed and requires manual retrieval.', thread_ts=ts)
    # send random pdf choice warning
    if multipdf_filename:
        client.chat_postMessage(channel=channel_id, text=f'Warning: Report exists in multiple pdf files. \nFile: "{multipdf_filename}" chosen at random.', thread_ts=ts)


    # try and download report 
    try:
        success, rest = getreport(report=report_id, directory=directory, filename=filename, multipdf_filename=multipdf_filename, transcript_source=transcript_source) #type: ignore
    except Exception as e:
        # return error reason
        client.chat_postMessage(channel=channel_id, thread_ts = ts, text=f'Report not found due to {e}\nRequires manual intervention {RP_ID}.')
        return

    # got report successfully
    if success:
        filename, multipdf_filename = rest
        client.files_upload_v2(channel=channel_id,
                initial_comment="Here's the report:",
                file=f'/tmp/{filename}', 
                thread_ts = ts)
    else:
        # handle error cases
        # if pdf not found in multifile pdf
        if multipdf_filename:
            client.chat_postMessage(channel=channel_id, text=str(rest)+multipdf_filename, thread_ts=ts)
        else:
        # general download problem
            client.chat_postMessage(channel=channel_id, text=str(rest), thread_ts=ts)