import pandas as pd
import paramiko as pk
import datetime
from pypdf import PdfReader, PdfWriter
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
DF = pd.read_csv('./trans_ref.csv', low_memory=False)
# for now, filter out capital IQ since it is not implemented
DF = DF[DF.transcript_source < 3]



def pdf_helper(report, file, local_dir = None):
    ''' Old pdf files are saved as bundles. \\ 
    Given report number and address to master pdf bundle file, \\ 
    cuts out that report and returns it as its own PDF.\\
    If given local_dir, operates using local directory'''
    # open master pdf bundle
    if local_dir:
        reader = PdfReader(f'{local_dir}/{file}')
    else:
        reader = PdfReader("/tmp/"+file)
    # get number of pages
    number_of_pages = len(reader.pages)
    # initialize variables needed cut up pages
    break_point = False
    match = False
    page_range = ''
    # go through pages
    for i in range(number_of_pages):
        # exit condition
        if break_point:
            break
        # read page
        page = reader.pages[i]
        # get page text
        text = page.extract_text()
        # get page lines
        lines = text.split('\n')
        # go through lines
        for j, line in enumerate(lines):
            # if report number is found, get page range of report
            # this is typically in the batch pdf's table of contents
            if str(report) in line:
                # line where report number is found
                match = line
                # get that line and the one after 
                # -- page range takes up two lines
                page_range = lines[j+1]
                # set exit condition
                break_point = True
                break
    # if no match found, exit
    if not match:
        return False
    
    # get first page of report
    first_page = int(page_range.split(' ')[0])
    # get last page of report
    last_page = int(page_range.split(' ')[-1])

    # set up pdf object
    writer = PdfWriter()
    # add pages to pdf object
    for i in range(first_page-1, last_page):
        writer.add_page(reader.pages[i])
    # save pdf file to local directory
    if local_dir:
        with open(f"{local_dir}/{report}.pdf", "wb") as fp:
            writer.write(fp)
    else:
        with open(f"/tmp/{report}.pdf", "wb") as fp:
            writer.write(fp)
    
    # return pdf
    return f"{report}.pdf"

def getreport(report, local_dir = None):
    """
    Given report number, finds it in mercury and saves it to temporary directory.\\
    This function can be used outside the context of the app, in which case \\
    local_dir determines the output folder for saving report files. 
    """
    # initialize batch pdf file variable
    multipdf_filename = False
    # decide which folder it's in using dataset
    sub = DF[DF.report == report]
    
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
        # IMPLEMENT CAPITALIQ LOGIC
        print(f'CapitalIQ retrieval not yet implemented. Requires manual intervention. {RP_ID}.')
        return [False, f'CapitalIQ retrieval not yet implemented. Requires manual intervention. {RP_ID}.', multipdf_filename]
    else:
        print(f'Invalid transcript source. Requires manual intervention. {RP_ID}.')
        return [False, f'Invalid transcript source. Requires manual intervention. {RP_ID}.', multipdf_filename]
    
    ## download file from mercury
    # log into mercury
    print(f'Logging into mercury to get {directory}/{filename}\n')
    ssh = pk.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, pkey=key)
    
    # open mercury connection
    sftp = ssh.open_sftp()
    # get file
    if local_dir:
        # if download was local
        sftp.get(f'{directory}/{filename}', f'{local_dir}/{filename}')
    else:
        # if download was via slack
        sftp.get(f'{directory}/{filename}', f'/tmp/{filename}')
    # close mercury connection
    sftp.close()

    # process pdf batch file if necessary
    if transcript_source == 0:
        # process and get filename for modified pdf file
        filename = pdf_helper(report, filenames[0])
        # handle file not found.
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.', multipdf_filename]
   
    return [True, filename, multipdf_filename]




def handle_download(directory, filename, transcript_source, report, local_dir = None, suppress_print = False, multipdf_filename = 'IMPLEMENT THIS'):
    '''
    Given directory path, filename, and report number, downloads given file \\
    from Mercury.

    If local_dir is given, file is downlaod to local_dir instead of bot's /tmp folder.
    '''
    # log into mercury
    if not suppress_print:
        print(f'Logging into mercury to get {directory}/{filename}\n')
    ssh = pk.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, pkey=key)
    
    # open mercury connection
    sftp = ssh.open_sftp()
    # get file
    if local_dir:
        # if download was local
        sftp.get(f'{directory}/{filename}', f'{local_dir}/{filename}')
    else:
        # if download was via slack
        sftp.get(f'{directory}/{filename}', f'/tmp/{filename}')
    
    # close mercury connection
    sftp.close()

    # process pdf batch file if necessary
    if transcript_source == 0:
        # process and get filename for modified pdf file
        filename = pdf_helper(report, filename)
        # handle file not found.
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.', multipdf_filename]
   
    return [True, filename, multipdf_filename]
