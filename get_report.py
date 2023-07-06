import pandas as pd
import numpy as np
import paramiko as pk
import datetime
from pypdf import PdfReader, PdfWriter
import os
import warnings
warnings.simplefilter(action='ignore', category=UnicodeWarning)

#ssh into mercury using paramiko
host = 'mercury.chicagobooth.edu'
port = 22
username = 'erouhani'
password = 'Zereshkpolo78!'

MAINDF = pd.read_stata('./paragraphs_rounds123456789A_gvkey.dta')
ALL_PDF = pd.read_csv('../Conference Calls/random/xls_combined_20010101-20220617.csv', low_memory=False)
ALL_PDF.rename({'Report #': 'report', 'filestem': 'file_name'}, axis=1, inplace=True)
ALL_REF = pd.read_csv('./all_refinitiv_trans_2001-2023-firsthalf.csv', index_col=0)
ALL_FS = pd.read_csv('./all_fs_files_2023.csv', index_col=0, low_memory=False)
ALL_FS.rename({'names': 'file_name'}, axis=1, inplace=True)


# not sure how to do this one
# for now, use hardcode
def df_user(report, transcript_source, key=None, master=False):
    df_dir = "/project/kh_mercury_1/conference_call/docgetter_datasets/"
    if master:
        df =  pd.read_csv(df_dir+'paragraphs_rounds123456789A_gvkey.dta')
        sub = df[df.report == report]
        if not sub.shape[0]:
            print('Report does not exist in dataset. Verify report number and try again.')
            return [False, 'Report does not exist in dataset. Verify report number and try again.']
        if len(set(sub.transcript_source)) > 1:
            print('Too many options. Requires manual intervention. <@U01KCEYLA85>.')
            return [False, 'Too many options. Requires manual intervention. <@U01KCEYLA85>.']
        transcript_source = sub.transcript_source.values[0]
        event_date = pd.to_datetime(sub.date)
        year = pd.DatetimeIndex(event_date).year[0]
        print(f'{year=}')
        date = pd.DatetimeIndex(event_date).date[0]
        print(f'{date=}')
        print(f'{transcript_source=}')
        return [True, transcript_source, year, date]
    elif transcript_source == 0:
        df = pd.read_csv(df_dir+'xls_combined_20010101-20220617.csv', low_memory=False)
        df.rename({'Report #': 'report', 'filestem': 'file_name'}, axis=1, inplace=True)
        sub_check = df[df.report == report]
        if date <= datetime.date(2021, 4, 5):
            directory = "/project/kh_mercury_1/conference_call/output/01_download_cc/01.1_pdf/20010101-20210405"
        else:
            directory = "/project/kh_mercury_1/conference_call/output/01_download_cc/01.1_pdf/20210101-20220617"
    elif transcript_source == 1:
        sub_check = ALL_REF[ALL_REF.file_name.str.contains(str(report))]
        directory = "/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/"+str(year)
    elif transcript_source == 2:
        directory = "/project/FactSet/fdsloader/unzipped_data/"+str(year)
        sub_check = ALL_FS[ALL_FS.id == report]
    else:
        print('Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.')
        return [False, 'Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.']
    if not sub_check.shape[0]:
        print('Report does not exist in dataset. Verify report number and try again.')
        return [False, 'Report does not exist in dataset. Verify report number and try again.']
        
    filename = sub_check.file_name.values[0]
    if transcript_source == 0:
        filename = filename + '.pdf'


    
    # log into mercury
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)
    sftp = ssh.open_sftp()
    #get file
    sftp.get(directory+f'/{filename}',f'./tempdir/{filename}')
    if transcript_source == 0:
        filename = pdf_helper(report, filename)
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.']
    #log out of mercury
    sftp.close()



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
            if break_point:
                break
            if str(report) in line:
                match = line
                page_range = lines[j+1]
                break_point = True
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
    # decide which folder it's in
    sub = MAINDF[MAINDF.report == report]
    if not sub.shape[0]:
        print('Report does not exist in dataset. Verify report number and try again.')
        return [False, 'Report does not exist in dataset. Verify report number and try again.']
    if len(set(sub.transcript_source)) > 1:
        print('Too many options. Requires manual intervention. <@U01KCEYLA85>.')
        return [False, 'Too many options. Requires manual intervention. <@U01KCEYLA85>.']
    transcript_source = sub.transcript_source.values[0]
    event_date = pd.to_datetime(sub.date)
    year = pd.DatetimeIndex(event_date).year[0]
    print(f'{year=}')
    date = pd.DatetimeIndex(event_date).date[0]
    print(f'{date=}')
    print(f'{transcript_source=}')
    if transcript_source == 0:
        sub_check = ALL_PDF[ALL_PDF.report == report]
        if date <= datetime.date(2021, 4, 5):
            directory = "/project/kh_mercury_1/conference_call/output/01_download_cc/01.1_pdf/20010101-20210405"
        else:
            directory = "/project/kh_mercury_1/conference_call/output/01_download_cc/01.1_pdf/20210101-20220617"
    elif transcript_source == 1:
        sub_check = ALL_REF[ALL_REF.file_name.str.contains(str(report))]
        directory = "/project/kh_mercury_1/refinitiv_univ/TRANSCRIPT/XML_Add_IDs/Archive/"+str(year)
    elif transcript_source == 2:
        directory = "/project/FactSet/fdsloader/unzipped_data/"+str(year)
        sub_check = ALL_FS[ALL_FS.id == report]
    else:
        print('Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.')
        return [False, 'Invalid transcript source. Requires manual intervention. <@U01KCEYLA85>.']
    if not sub_check.shape[0]:
        print('Report does not exist in dataset. Verify report number and try again.')
        return [False, 'Report does not exist in dataset. Verify report number and try again.']
        
    filename = sub_check.file_name.values[0]
    if transcript_source == 0:
        filename = filename + '.pdf'
        
    print(f'Found {filename=}')
    # log into mercury
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    ssh.connect(host, username=username, password=password)
    sftp = ssh.open_sftp()
    #get file
    sftp.get(directory+f'/{filename}',f'./tempdir/{filename}')
    if transcript_source == 0:
        filename = pdf_helper(report, filename)
        if not filename:
            print('Problem with pdf search. Report not found in large pdf.')
            return [False, 'Problem with pdf search. Report not found in large pdf.']
    #log out of mercury
    sftp.close()
    return [True, filename]