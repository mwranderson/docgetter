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

#ssh into mercury using paramiko
host = 'mercury.chicagobooth.edu'
username = 'erouhani'
key = pk.RSAKey.from_private_key(StringIO(str(os.environ.get("MERCURY_KEY"))))
print(f'{key=}')

DF = pd.read_csv('./trans_ref.csv', index_col=0)

def pdf_helper(report, file):
    reader = PdfReader("./tempdir/"+file)
    number_of_pages = len(reader.pages)
    break_point = False
    match = False
    page_range = ''
    for i in range(number_of_pages):
        if break_point:
            break
        page = reader.pages[i]
        text = page.extract_text()
        lines = text.split('\n')
        for j, line in enumerate(lines):
            if str(report) in line:
                match = line
                page_range = lines[j+1]
                break_point = True
                break
    if not match:
        return False
    first_page = int(page_range.split(' ')[0])
    last_page = int(page_range.split(' ')[-1])

    writer = PdfWriter()
    for i in range(first_page-1, last_page):
        writer.add_page(reader.pages[i])
    with open(f"./tempdir/{report}.pdf", "wb") as fp:
        writer.write(fp)
    
    os.remove("./tempdir/"+file)
    return f"{report}.pdf"

def getreport(report):
    multipdf_filename = False
    # decide which folder it's in
    sub = DF[DF.report == report]
    if not sub.shape[0]:
        print('Report does not exist in dataset. Verify report number and try again.')
        return [False, 'Report does not exist in dataset. Verify report number and try again.', multipdf_filename]
    if (len(set(sub.transcript_source)) > 1) or (len(set(sub.date)) > 1):
        print('Too many options. Requires manual intervention. <@U01KCEYLA85>.')
        return [False, 'Too many options. Requires manual intervention. <@U01KCEYLA85>.', multipdf_filename]
    filenames = sub.file_name.to_list()
    if len(filenames) > 1:
        print('Multipdf problem. One PDF picked at random.')
        multipdf_filename = filenames[0]
    print(f'{report=}')
    filename = filenames[0]
    transcript_source = sub.transcript_source.values[0]
    print(f'{transcript_source=}')
    event_date = pd.to_datetime(sub.date)
    date = pd.DatetimeIndex(event_date).date[0]
    print(f'{date=}')
    year = pd.DatetimeIndex(event_date).year[0]
    print(f'{year=}')
    
    if transcript_source == 0:
        if date <= datetime.date(2021, 4, 5):
            directory = "/project/kh_mercury_1/conference_call/pdf_files/output/01_download_cc/01.1_pdf/20010101-20210405"
        elif date <= datetime.date(2021, 9, 9):
            directory = "/project/kh_mercury_1/conference_call/pdf_files/output/01_download_cc/01.1_pdf/20201001-20210909"
        else:
            directory = "/project/kh_mercury_1/conference_call/pdf_files/output/01_download_cc/01.1_pdf/20210101-20220617"
    elif transcript_source == 1:
        directory = "/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/"+str(year)
        if date >= datetime.date(2023, 1, 1):
            directory = directory + '/2023_firsthalf'
    elif transcript_source == 2:
        directory = "/project/FactSet/fdsloader/unzipped_data/"+str(year)
    else:
        print('Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.')
        return [False, 'Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.', multipdf_filename]
    
    # get file from mercury
    # log into mercury
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, pkey=key)
    sftp = ssh.open_sftp()
    sftp.get(directory+f'/{filename}',f'./tempdir/{filename}')
    sftp.close()
    if transcript_source == 0:
        filename = pdf_helper(report, filenames[0])
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.', multipdf_filename]
    else:
        filename = filenames[0]
        print(f'{filename=}')
   
    return [True, filename, multipdf_filename]
