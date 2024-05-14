import pandas as pd
import paramiko as pk
import modules.pdf_mods as pdfm
import datetime
import os
import warnings
from dotenv import load_dotenv 
from io import StringIO

load_dotenv() 
warnings.simplefilter(action='ignore', category=UnicodeWarning)

# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U01KCEYLA85>'

#ssh into mercury using paramiko
host = 'mercury.chicagobooth.edu'
username = 'erouhani'
## add .env file to main folder that contains your mercury sshkey
key = pk.RSAKey.from_private_key(StringIO(str(os.environ.get("MERCURY_KEY"))))

# dataset of all reports with column set up to download from mercury directory structure
DF = pd.read_csv('./trans_ref.csv', compression='gzip')

def getreport(report, transcript_source, local_dir = ''):
    """
    Given report number, finds it in mercury and saves it to temporary directory.\\
    This function can be used outside the context of the app, in which case \\
    local_dir determines the output folder for saving report files. 
    """
    # initialize batch pdf file variable
    multipdf_filename = False
    
    # check if transcript source was provided and subset dataset accordingly
    if transcript_source > -1:
        sub = DF[(DF.report == report) & (DF.transcript_source == transcript_source)].drop_duplicates()
    else:
        sub = DF[DF.report == report].drop_duplicates()
    
    # if not found in dataset
    if not sub.shape[0]:
        print('Report does not exist in dataset. Verify report number and try again.')
        return [False, 'Report does not exist in dataset. Verify report number and try again.', multipdf_filename]
    
    # if multiple different matches found with report number, use manual intervention
    # NEEDS IMPROVEMENT
    if (len(set(sub.transcript_source)) > 1) or (len(set(sub.date)) > 1):
        print(f'Too many options. Requires manual intervention. {RP_ID}.')
        return [False, f'Too many options. Requires manual intervention. {RP_ID}.', multipdf_filename]
    
    # get filenames of report
    filenames = sub.file_name.to_list()

    # if multiple pdfs, due to oddities with old pdf downloads, pick randomly.
    # manual inspection showed that these are effectively identical. 
    if len(filenames) > 1:
        print('Multipdf problem. One PDF picked at random.')
        multipdf_filename = filenames[0]
    
    print(f'{report=}')

    # get filename
    filename = filenames[0]
    # get transcript source
    # 0: refinitiv pdf
    # 1: refinitiv xml
    # 2: factset xml
    # 3: capital IQ
    transcript_source = sub.transcript_source.values[0]
    print(f'{transcript_source=}')
    # get event_date, date, and year
    event_date = pd.to_datetime(sub.date)
    date = pd.DatetimeIndex(event_date).date[0]
    print(f'{date=}')
    year = pd.DatetimeIndex(event_date).year[0]
    print(f'{year=}')
    
    ## get file path -- logic follows organization on Mercury
    # refinitiv pdf case
    if transcript_source == 0:
        directory = "/project/kh_mercury_1/conference_call/pdf_files/output/01_download_cc/01.1_pdf/consolidated_20010101-20220617"
    # refinitiv xml case
    elif transcript_source == 1:
        directory = f"/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/{year}"
    # factset xml case
    elif transcript_source == 2:
        directory = f"/project/FactSet/fdsloader/unzipped_data/tr_history_{year}"
    # capital IQ case
    elif transcript_source == 3:
        directory = f"/project/kh_mercury_1/conference_call/ciq/output/transcript_data/{year}_ciq_trans_cleaned.csv"
    else:
        print(f'Invalid transcript source. Requires manual intervention. {RP_ID}.')
        return [False, f'Invalid transcript source. Requires manual intervention. {RP_ID}.', multipdf_filename]
    

    # download file
    filename = handle_download(directory=directory, filename=filename, local_dir=local_dir, transcript_source=transcript_source)

    if not filename:
        print('Problem with Capital IQ search. Requires manual intervention. {RP_ID}.')
        return [False, 'Problem with Capital IQ search. Requires manual intervention. {RP_ID}']

    # process pdf batch file if necessary
    if transcript_source == 0:
        # process and get filename for modified pdf file
        filename = pdfm.pdf_splitter(report, filenames[0], local_dir)
        # handle file not found.
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.', multipdf_filename]
   
    return [True, filename, multipdf_filename]

def handle_download(
        directory: str, 
        filename: str, 
        transcript_source: int,
        local_dir: str):
    '''
    Given directory path, filename, and transcript_source, downloads given file \\
    from Mercury.

    If local_dir is given, file is downlaod to local_dir instead of bot's /tmp folder.
    '''
    ## download file from mercury
    # log into mercury
    print(f'Logging into mercury to get {directory}/{filename}\n')
    ssh = pk.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, pkey=key)
    
    # open mercury connection
    sftp = ssh.open_sftp()

    # capital iq get case
    if transcript_source == 3:
        # get report from file name
        report = int(filename.split('_')[-1])

        # read file iteratively for memory performance purposes
        chunksize = 100

        # initialize row
        row = None
        # open file and iterate through it to find needed row
        with sftp.open(directory) as f:
            # prefetching speeds up reading while not maximally increasing
            # memory use
            f.prefetch()
            # read in csv file with iterator
            ciq_options = pd.read_csv(f, chunksize=chunksize) #type: ignore
            
            # look for relevant report
            for chunk in ciq_options:
                if report in chunk.transcriptid.to_list():
                    # get report text body
                    row = chunk[chunk.transcriptid == report]
                    break
        
        # return false if row not found
        if not isinstance(row, pd.DataFrame):
            return False

        # get needed vars from row
        body = row.text.values[0]
        event_title = row.event_title.values[0]
        event_date = row.event_date.values[0]
        
        # turn it into pdf or txt file and save in local dir
        return pdfm.pdf_creator(body=body, filename=filename+'.pdf', event_title=event_title, event_date=event_date, local_dir=local_dir)
        
    else:
        # get file
        if local_dir:
            # if download was local
            sftp.get(f'{directory}/{filename}', f'{local_dir}/{filename}')
        else:
            # if download was via slack
            sftp.get(f'{directory}/{filename}', f'/tmp/{filename}')
    # close mercury connection
    sftp.close()

    return filename
