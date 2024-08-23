import pandas as pd
import paramiko as pk
import os
from .pdf_mods import pdf_creator, pdf_splitter
import time
import warnings
from dotenv import load_dotenv 
from io import StringIO
from typing import Union

load_dotenv() 
warnings.simplefilter(action='ignore', category=UnicodeWarning)

# slack ID of RP in charge -- currently Esfandiar
RP_ID = '<@U079GUP88LW>'

# ssh into mercury using paramiko
host = 'mercury.chicagobooth.edu'
username = 'mrumsey0'
# increasing paramiko speed
class FastTransport(pk.Transport):
    def __init__(self, sock):
        super(FastTransport, self).__init__(sock)
        self.window_size = 2147483647
        self.packetizer.REKEY_BYTES = pow(2, 40)
        self.packetizer.REKEY_PACKETS = pow(2, 40)

## add .env file to main folder that contains your mercury sshkey
key = pk.RSAKey.from_private_key(StringIO(str(os.environ.get("MERCURY_KEY"))))

# dataset of all reports with column set up to download from mercury directory structure
DF = pd.read_csv('./trans_ref.csv', compression='gzip', low_memory=False)

def get_file_info(report: int = 0, 
                  transcript_source: int = -1,
                  slide_id: int = 0,
                  slide_file_name : str = '',
                  slide_mode = False) -> tuple[bool, Union[list, str]]:
    """
    Given report number, finds it in mercury \\
    and returns directory, filename, and transcript source if found.\\
    Returns informative error messages if not.\\
    If looking for slides, set slide_mode to True (Default False).
    """
    # initialize batch pdf file variable
    multipdf_filename = False


    
    # subset dataset accordingly
    if slide_mode:
        if slide_file_name:
            sub = DF[(DF.file_name == slide_file_name)]
        else:
            sub = DF[(DF.slide_id == slide_id)]
    else:
        if transcript_source > -1:
            sub = DF[(DF.report == report) & (DF.transcript_source == transcript_source)].drop_duplicates()
        else:
            sub = DF[DF.report == report].drop_duplicates()
       
    
    # if not found in dataset
    if not sub.shape[0]:
        print('File does not exist in dataset. Verify provided information and try again.')
        return False, 'File does not exist in dataset. Verify provided information and try again.'
    
    # if multiple different matches found
    if (len(set(sub.transcript_source.dropna())) > 1) or (len(set(sub.date)) > 1) or (len(set(sub.address)) > 1):
        if transcript_source > -1:
            print(f'Too many options. Requires manual intervention. {RP_ID}.')
            return False, f'Too many options. Requires manual intervention. {RP_ID}.'
        else:
            print(f'Multiple files have provided information.\n\
                    Please choose your desired file.')
            return False, sub.reset_index(drop=True)

    # get filenames of report
    filenames: list[str] = sub.file_name.to_list()

    # if multiple pdfs, due to oddities with old pdf downloads, pick randomly.
    # manual inspection showed that these are effectively identical. 
    if len(filenames) > 1:
        print('Multiple duplicate files problem. One instance picked at random.')
        multipdf_filename = filenames[0]
    
    print(f'id={report|slide_id}')

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
    # slide case
    elif slide_mode:
        # get mercury address of file 
        merc_add = sub.address.values[0]
        print(f'{merc_add=}')
        directory = f"/project/FactSet/Doc_Retrieval_API/factset_slides_API/output/{merc_add}"

    else:
        print(f'Invalid transcript source. Requires manual intervention. {RP_ID}.')
        return False, f'Invalid transcript source. Try again.'

    return True, [directory, filename, transcript_source, multipdf_filename]

def getreport(report: int, 
              directory: str, 
              filename: str, 
              transcript_source: int, 
              multipdf_filename: str, 
              local_dir: str = '',
              slide_mode = False):
    """
    Given report, directory and file name, finds it in mercury and saves it to temporary directory. \\
    If looking for slide, set slide_mode to True (default False)
    """

    st_time = time.time()
    
    # download file
    filename_new = handle_download(directory=directory, filename=filename, local_dir=local_dir, transcript_source=transcript_source, st_time=st_time)

    if not filename_new:
        print('Problem with download process. Requires manual intervention. {RP_ID}.')
        return False, 'Problem with download process. Requires manual intervention. {RP_ID}'

    # process pdf batch file if necessary
    if transcript_source == 0:
        # process and get filename for modified pdf file
        filename_new = pdf_splitter(report, filename_new, local_dir)
        # handle file not found.
        if not filename_new:
            print('Problem with pdf search. Report not found in large pdf.')
            return False, 'Problem with pdf search. Report not found in large pdf: '
   
    return True, [filename_new, multipdf_filename]


def getreport_local(report: int, transcript_source: int, local_dir: str):
    """
    Given report, transcript source, and local directory, \\
    finds and saves file into local_dir
    """

    # get report info
    success, rest = get_file_info(report, transcript_source)

    if success:
        directory, filename, transcript_source, multipdf_filename = rest # type: ignore
    else:
        print(rest)
        return
    
    st_time = time.time()
    
    # download file
    filename = handle_download(directory=directory, filename=filename, local_dir=local_dir, transcript_source=transcript_source, st_time=st_time)

    if not filename:
        print('Problem with download process. Requires manual intervention. {RP_ID}.')
        return [False, 'Problem with download process. Requires manual intervention. {RP_ID}']

    # process pdf batch file if necessary
    if transcript_source == 0:
        # process and get filename for modified pdf file
        filename = pdf_splitter(report, filename, local_dir)
        # handle file not found.
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.']
   
    return [True, filename]


def handle_download(
        directory: str, 
        filename: str, 
        transcript_source: int,
        local_dir: str,
        st_time: float = 0):
    '''
    Given directory path, filename, and transcript_source, downloads given file \\
    from Mercury.

    If local_dir is given, file is downlaod to local_dir instead of bot's /tmp folder.
    '''
    ## download file from mercury
    # log into mercury
    print(f'Logging into mercury to get {directory}/{filename}\n')

    # set up connection via fast transport
    ssh_conn = FastTransport((host, 22))
    ssh_conn.connect(username=username, pkey=key)
    
    # open mercury connection
    sftp = pk.SFTPClient.from_transport(ssh_conn)

    # exit if sftp not established
    if not sftp:
        print('Sftp connection to Mercury not established. Exiting.')
        return
    
    # initialize row finding bool
    found = False

    # capital iq get case
    if transcript_source == 3:
        # get report from file name
        report = int(filename.split('_')[-1])

        # read file iteratively for memory performance purposes
        chunksize = 500

        # initialize row
        row = None
        # open file and iterate through it to find needed row
        with sftp.open(directory) as f:
            # read in csv file with iterator
            ciq_options = pd.read_csv(f, usecols=['transcriptid', 'event_title', 'event_date', 'text'], chunksize=chunksize) #type: ignore
            # look for relevant report
            for chunk in ciq_options:
                print(f'Finding ciq row is now at {time.time()-st_time} seconds.')
                # get report text body
                row = chunk[chunk.transcriptid == report]
                # exit when report is found
                if row.shape[0]:
                    found = True
                    break
        
        print(f'Finding ciq row done at {time.time()-st_time} seconds.')
        
        # return if row not found
        if not found:
            return

        # get needed vars from row
        body = row.text.values[0] #type:ignore
        event_title = row.event_title.values[0] #type:ignore
        event_date = row.event_date.values[0] #type:ignore
        
        # turn it into pdf or txt file and save in local dir
        return pdf_creator(body=body, filename=filename+'.pdf', event_title=event_title, event_date=event_date, local_dir=local_dir)
        
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
